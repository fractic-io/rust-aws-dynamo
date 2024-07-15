use std::collections::HashMap;

use aws_sdk_dynamodb::{
    operation::{
        batch_write_item::BatchWriteItemError, delete_item::DeleteItemError,
        update_item::UpdateItemError,
    },
    types::AttributeValue,
};
use backend::DynamoBackendImpl;
use calculate_sort::calculate_sort_values;
use fractic_core::collection;
use fractic_generic_server_error::GenericServerError;

use crate::{
    errors::{DynamoConnectionError, DynamoInvalidOperationError, DynamoNotFoundError},
    schema::{
        id_calculations::{generate_uuid, get_object_type, get_pk_sk_from_map},
        parsing::{build_dynamo_map, parse_dynamo_map, IdKeys},
        DynamoObject, PkSk, Timestamp,
    },
};

pub mod backend;
mod calculate_sort;

pub type DynamoMap = HashMap<String, AttributeValue>;
pub const AUTO_FIELDS_CREATED_AT: &str = "created_at";
pub const AUTO_FIELDS_UPDATED_AT: &str = "updated_at";
pub const AUTO_FIELDS_SORT: &str = "sort";

pub enum DynamoQueryMatchType {
    BeginsWith,
    Equals,
}

pub enum DynamoInsertPosition {
    First,
    Last,
    After(PkSk),
}

pub struct DynamoUtil<B: DynamoBackendImpl> {
    pub backend: B,
    pub table: String,
}
impl<C: DynamoBackendImpl> DynamoUtil<C> {
    pub async fn query<T: DynamoObject>(
        &self,
        index: Option<String>,
        id: PkSk,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<T>, GenericServerError> {
        self.query_generic(index, id, match_type)
            .await?
            .into_iter()
            .filter_map(|item| {
                let (pk, sk) =
                    get_pk_sk_from_map(&item).expect("Query result item did not have pk/sk.");
                if get_object_type(pk, sk) == T::id_label() {
                    // Item is of type T.
                    Some(parse_dynamo_map::<T>(&item))
                } else {
                    // Item is not of type T, but instead an inline child (of a
                    // different type), which will be skipped. Use query_dynamic
                    // to access objects of type T and their inline children.
                    None
                }
            })
            .collect::<Result<Vec<T>, GenericServerError>>()
    }

    pub async fn query_generic(
        &self,
        index: Option<String>,
        id: PkSk,
        match_type: DynamoQueryMatchType,
    ) -> Result<Vec<DynamoMap>, GenericServerError> {
        let dbg_cxt: &'static str = "query_generic";
        let condition = match match_type {
            DynamoQueryMatchType::BeginsWith => "pk = :pk_val AND begins_with(sk, :sk_val)",
            DynamoQueryMatchType::Equals => "pk = :pk_val AND sk = :sk_val",
        }
        .to_string();
        let attribute_values = collection! {
            ":pk_val".to_string() => AttributeValue::S(id.pk),
            ":sk_val".to_string() => AttributeValue::S(id.sk),
        };
        let response = self
            .backend
            .query(self.table.clone(), index, condition, attribute_values)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        let mut items = response.items().to_vec();
        items.sort_by(|a, b| {
            let a_sort = a
                .get(AUTO_FIELDS_SORT)
                .and_then(|v| v.as_n().ok().map(|n| n.parse::<f64>().ok()))
                .flatten();
            let b_sort = b
                .get(AUTO_FIELDS_SORT)
                .and_then(|v| v.as_n().ok().map(|n| n.parse::<f64>().ok()))
                .flatten();
            match (a_sort, b_sort) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap(),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            }
        });
        Ok(items.to_vec())
    }

    pub async fn get_item<T: DynamoObject>(
        &self,
        id: PkSk,
    ) -> Result<Option<T>, GenericServerError> {
        let dbg_cxt: &'static str = "get_item";
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        let response = self
            .backend
            .get_item(self.table.clone(), key)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        response
            .item
            .map(|item| parse_dynamo_map::<T>(&item))
            .transpose()
    }

    pub async fn create_item<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        object: &T,
        custom_sort: Option<f64>,
    ) -> Result<PkSk, GenericServerError> {
        let dbg_cxt: &'static str = "create_item";
        let uuid = generate_uuid();
        let new_pk = object.generate_pk(&parent_id.pk, &parent_id.sk, &uuid);
        let new_sk = object.generate_sk(&parent_id.pk, &parent_id.sk, &uuid);
        let map = build_dynamo_map::<T>(
            &object,
            IdKeys::Override(new_pk.clone(), new_sk.clone()),
            Some(vec![
                (AUTO_FIELDS_CREATED_AT, Box::new(Timestamp::now())),
                (AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now())),
                (AUTO_FIELDS_SORT, Box::new(custom_sort)),
            ]),
        )?;
        self.backend
            .put_item(self.table.clone(), map)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        Ok(PkSk {
            pk: new_pk,
            sk: new_sk,
        })
    }

    pub async fn batch_create_item<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        objects_and_custom_sorts: Vec<(&T, Option<f64>)>,
    ) -> Result<Vec<PkSk>, GenericServerError> {
        let dbg_cxt: &'static str = "batch_create_item";
        if objects_and_custom_sorts.is_empty() {
            return Ok(Vec::new());
        }
        let (items, ids) = objects_and_custom_sorts
            .into_iter()
            .map(|(object, custom_sort)| {
                let uuid = generate_uuid();
                let new_pk = object.generate_pk(&parent_id.pk, &parent_id.sk, &uuid);
                let new_sk = object.generate_sk(&parent_id.pk, &parent_id.sk, &uuid);
                Ok((
                    build_dynamo_map::<T>(
                        &object,
                        IdKeys::Override(new_pk.clone(), new_sk.clone()),
                        Some(vec![
                            (AUTO_FIELDS_CREATED_AT, Box::new(Timestamp::now())),
                            (AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now())),
                            (AUTO_FIELDS_SORT, Box::new(custom_sort)),
                        ]),
                    )?,
                    PkSk {
                        pk: new_pk,
                        sk: new_sk,
                    },
                ))
            })
            .collect::<Result<Vec<(DynamoMap, PkSk)>, GenericServerError>>()?
            .into_iter()
            .unzip();
        self.backend
            .batch_put_item(self.table.clone(), items)
            .await
            .map_err(|e| DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", e)))?;
        Ok(ids)
    }

    pub async fn create_item_ordered<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        object: &T,
        insert_position: DynamoInsertPosition,
    ) -> Result<PkSk, GenericServerError> {
        let dbg_cxt: &'static str = "create_item_ordered";
        let sort_val = calculate_sort_values(self, parent_id.clone(), object, insert_position, 1)
            .await?
            .pop()
            .ok_or(DynamoInvalidOperationError::new(
                dbg_cxt,
                "failed to generate new ordered ID",
            ))?;
        self.create_item(parent_id, object, Some(sort_val)).await
    }

    pub async fn batch_create_item_ordered<T: DynamoObject>(
        &self,
        parent_id: PkSk,
        objects: Vec<&T>,
        insert_position: DynamoInsertPosition,
    ) -> Result<Vec<PkSk>, GenericServerError> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let new_ids = calculate_sort_values(
            self,
            parent_id.clone(),
            *objects.first().unwrap(),
            insert_position,
            objects.len(),
        )
        .await?;
        self.batch_create_item(
            parent_id,
            objects
                .into_iter()
                .zip(new_ids.into_iter())
                .map(|(object, sort_val)| (object, Some(sort_val)))
                .collect(),
        )
        .await
    }

    pub async fn update_item<T: DynamoObject>(&self, object: &T) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "update_item";
        let key = collection! {
            "pk".to_string() => AttributeValue::S(object.pk().ok_or(
                DynamoInvalidOperationError::new(
                    dbg_cxt, "pk required"))?.to_string()),
            "sk".to_string() => AttributeValue::S(object.sk().ok_or(
                DynamoInvalidOperationError::new(
                    dbg_cxt, "sk required"))?.to_string()),
        };
        let map = build_dynamo_map::<T>(
            &object,
            IdKeys::None,
            Some(vec![(AUTO_FIELDS_UPDATED_AT, Box::new(Timestamp::now()))]),
        )?;
        let mut expression_attribute_names = HashMap::new();
        let mut expression_attribute_values = HashMap::new();
        let update_expression = "SET ".to_string()
            + &map
                .iter()
                .enumerate()
                .map(|(idx, (key, _))| {
                    let key_placeholder = format!("#k{}", idx + 1);
                    let value_placeholder = format!(":v{}", idx + 1);
                    expression_attribute_names.insert(key_placeholder.clone(), key.to_string());
                    expression_attribute_values.insert(value_placeholder.clone(), map[key].clone());
                    format!("{} = {}", key_placeholder, value_placeholder)
                })
                .collect::<Vec<String>>()
                .join(", ");
        self.backend
            .update_item(
                self.table.clone(),
                key,
                update_expression,
                expression_attribute_values,
                expression_attribute_names,
            )
            .await
            .map_err(|e| match e.into_service_error() {
                UpdateItemError::ResourceNotFoundException(_) => DynamoNotFoundError::default(),
                other => DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other)),
            })?;
        Ok(())
    }

    pub async fn delete_item(&self, id: PkSk) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "delete_item";
        let key = collection! {
            "pk".to_string() => AttributeValue::S(id.pk),
            "sk".to_string() => AttributeValue::S(id.sk),
        };
        self.backend
            .delete_item(self.table.clone(), key)
            .await
            .map_err(|e| match e.into_service_error() {
                DeleteItemError::ResourceNotFoundException(_) => DynamoNotFoundError::default(),
                other => DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other)),
            })?;
        Ok(())
    }

    pub async fn batch_delete_item(&self, keys: Vec<PkSk>) -> Result<(), GenericServerError> {
        let dbg_cxt: &'static str = "batch_delete_item";
        if keys.is_empty() {
            return Ok(());
        }
        let items = keys
            .into_iter()
            .map(|id| {
                collection! {
                    "pk".to_string() => AttributeValue::S(id.pk),
                    "sk".to_string() => AttributeValue::S(id.sk),
                }
            })
            .collect::<Vec<_>>();
        self.backend
            .batch_delete_item(self.table.clone(), items)
            .await
            .map_err(|e| match e.into_service_error() {
                BatchWriteItemError::ResourceNotFoundException(_) => DynamoNotFoundError::default(),
                other => DynamoConnectionError::with_debug(dbg_cxt, "", format!("{:#?}", other)),
            })?;
        Ok(())
    }
}

// Tests.
// --------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::{
        impl_dynamo_object,
        schema::{AutoFields, NestingType},
    };

    use super::*;
    use aws_sdk_dynamodb::{
        operation::{
            batch_write_item::BatchWriteItemOutput, delete_item::DeleteItemOutput,
            get_item::GetItemOutput, put_item::PutItemOutput, query::QueryOutput,
            update_item::UpdateItemOutput,
        },
        types::AttributeValue,
    };
    use backend::MockDynamoBackendImpl;
    use mockall::predicate::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
    struct TestDynamoObject {
        id: Option<PkSk>,
        #[serde(flatten)]
        auto_fields: AutoFields,
        data: Option<String>,
    }
    impl_dynamo_object!(TestDynamoObject, "TEST", NestingType::TopLevelChild);

    fn build_item_no_data() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: Some(PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#1".to_string(),
                }),
                auto_fields: Default::default(),
                data: None,
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#1".to_string()),
            },
        )
    }

    fn build_item_high_sort() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: Some(PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#2".to_string(),
                }),
                auto_fields: AutoFields {
                    sort: Some(0.75),
                    ..Default::default()
                },
                data: Some("high_sort".to_string()),
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#2".to_string()),
                "sort".to_string() => AttributeValue::N("0.75".to_string()),
                "data".to_string() => AttributeValue::S("high_sort".to_string()),
            },
        )
    }

    fn build_item_low_sort() -> (TestDynamoObject, HashMap<String, AttributeValue>) {
        (
            TestDynamoObject {
                id: Some(PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST#3".to_string(),
                }),
                auto_fields: AutoFields {
                    sort: Some(0.10001),
                    ..Default::default()
                },
                data: Some("low_sort".to_string()),
            },
            collection! {
                "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                "sk".to_string() => AttributeValue::S("GROUP#123#TEST#3".to_string()),
                "sort".to_string() => AttributeValue::N("0.10001".to_string()),
                "data".to_string() => AttributeValue::S("low_sort".to_string()),
            },
        )
    }

    #[tokio::test]
    async fn test_query() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .with(
                eq("my_table".to_string()),
                eq(None),
                eq("pk = :pk_val AND begins_with(sk, :sk_val)".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    ":pk_val".to_string() => AttributeValue::S("ROOT".to_string()),
                    ":sk_val".to_string() => AttributeValue::S("GROUP#123".to_string())
                }),
            )
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_item_high_sort().1,
                        build_item_no_data().1,
                        build_item_low_sort().1,
                        collection!(
                            "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                            "sk".to_string() => AttributeValue::S("GROUP#123#OTHEROBJECT#1".to_string())
                        )
                    ]))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };
        let result = util
            .query::<TestDynamoObject>(
                None,
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123".to_string(),
                },
                DynamoQueryMatchType::BeginsWith,
            )
            .await
            .unwrap();

        // Should have ignored object of label "OTHEROBJECT", since we are
        // are querying only TestDynamoObject, which has label "TEST".
        assert_eq!(result.len(), 3);

        // Lower sort value item first.
        assert_eq!(result[0], build_item_low_sort().0);
        assert_eq!(result[1], build_item_high_sort().0);
        assert_eq!(result[2], build_item_no_data().0);
    }

    #[tokio::test]
    async fn test_query_generic() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_query()
            .with(
                eq("my_table".to_string()),
                eq(None),
                eq("pk = :pk_val AND begins_with(sk, :sk_val)".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    ":pk_val".to_string() => AttributeValue::S("ROOT".to_string()),
                    ":sk_val".to_string() => AttributeValue::S("GROUP#123#TEST".to_string())
                }),
            )
            .returning(|_, _, _, _| {
                Ok(QueryOutput::builder()
                    .set_items(Some(vec![
                        build_item_high_sort().1,
                        build_item_low_sort().1,
                    ]))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let result = util
            .query_generic(
                None,
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123#TEST".to_string(),
                },
                DynamoQueryMatchType::BeginsWith,
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 2);

        // Lower sort value item first.
        assert_eq!(result[0], build_item_low_sort().1);
        assert_eq!(result[1], build_item_high_sort().1);
    }

    #[tokio::test]
    async fn test_get_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_get_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("ROOT".to_string()),
                    "sk".to_string() => AttributeValue::S("GROUP#123#TEST#2".to_string())
                }),
            )
            .returning(|_, _| {
                Ok(GetItemOutput::builder()
                    .set_item(Some(build_item_high_sort().1))
                    .build())
            });

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let result = util
            .get_item::<TestDynamoObject>(PkSk {
                pk: "ROOT".to_string(),
                sk: "GROUP#123#TEST#2".to_string(),
            })
            .await
            .unwrap();

        assert!(result.is_some());
        let item = result.unwrap();
        assert_eq!(item.pk(), Some("ROOT"));
        assert_eq!(item.sk(), Some("GROUP#123#TEST#2"));
        assert_eq!(item.data, Some("high_sort".to_string()));
    }

    #[tokio::test]
    async fn test_create_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_put_item()
            .withf(|_, item| {
                item.get(AUTO_FIELDS_CREATED_AT).is_some()
                    && item.get(AUTO_FIELDS_UPDATED_AT).is_some()
                    && item.get(AUTO_FIELDS_SORT).is_some()
                    && item.get("data").is_some()
            })
            .returning(|_, _| Ok(PutItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let new_item = build_item_high_sort().0;

        let result = util
            .create_item(
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123".to_string(),
                },
                &new_item,
                Some(0.75),
            )
            .await
            .unwrap();

        assert_eq!(result.pk, "GROUP#123".to_string());
    }

    #[tokio::test]
    async fn test_batch_create_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_batch_put_item()
            .withf(|_, items| {
                items.len() == 2
                    && items.iter().all(|item| {
                        item.get(AUTO_FIELDS_CREATED_AT).is_some()
                            && item.get(AUTO_FIELDS_UPDATED_AT).is_some()
                            && item.get(AUTO_FIELDS_SORT).is_some()
                            && item.get("data").is_some()
                    })
            })
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let item1 = build_item_no_data().0;
        let item2 = build_item_no_data().0;
        let items = vec![(&item1, Some(0.1200000)), (&item2, Some(12.0))];

        let result = util
            .batch_create_item(
                PkSk {
                    pk: "ROOT".to_string(),
                    sk: "GROUP#123".to_string(),
                },
                items,
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_update_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_update_item()
            .withf(|_, id, update_expr, values, keys| {
                id.get("pk").unwrap().as_s().unwrap() == "pk_to_update"
                    && id.get("sk").unwrap().as_s().unwrap() == "sk_to_update"
                    && update_expr == "SET #k1 = :v1, #k2 = :v2"
                    && values.get(":v1").is_some()
                    && values.get(":v2").is_some()
                    && {
                        let mut v = vec![keys.get("#k1").unwrap(), keys.get("#k2").unwrap()];
                        v.sort();
                        v
                    } == vec![&"data".to_string(), &"updated_at".to_string()]
            })
            .returning(|_, _, _, _, _| Ok(UpdateItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let update_item = TestDynamoObject {
            id: Some(PkSk {
                pk: "pk_to_update".to_string(),
                sk: "sk_to_update".to_string(),
            }),
            auto_fields: Default::default(),
            data: Some("new_data".to_string()),
        };

        let result = util.update_item(&update_item).await.unwrap();
        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_delete_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_delete_item()
            .with(
                eq("my_table".to_string()),
                eq::<HashMap<String, AttributeValue>>(collection! {
                    "pk".to_string() => AttributeValue::S("GROUP#123".to_string()),
                    "sk".to_string() => AttributeValue::S("LIST#123#ITEM#456".to_string())
                }),
            )
            .returning(|_, _| Ok(DeleteItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let result = util
            .delete_item(PkSk {
                pk: "GROUP#123".to_string(),
                sk: "LIST#123#ITEM#456".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_batch_delete_item() {
        let mut backend = MockDynamoBackendImpl::new();
        backend
            .expect_batch_delete_item()
            .with(
                eq("my_table".to_string()),
                eq(vec![
                    collection! {
                        "pk".to_string() => AttributeValue::S("pk_to_delete_1".to_string()),
                        "sk".to_string() => AttributeValue::S("sk_to_delete_1".to_string()),
                    },
                    collection! {
                        "pk".to_string() => AttributeValue::S("pk_to_delete_2".to_string()),
                        "sk".to_string() => AttributeValue::S("sk_to_delete_2".to_string()),
                    },
                ]),
            )
            .returning(|_, _| Ok(BatchWriteItemOutput::builder().build()));

        let util = DynamoUtil {
            backend,
            table: "my_table".to_string(),
        };

        let keys = vec![
            PkSk {
                pk: "pk_to_delete_1".to_string(),
                sk: "sk_to_delete_1".to_string(),
            },
            PkSk {
                pk: "pk_to_delete_2".to_string(),
                sk: "sk_to_delete_2".to_string(),
            },
        ];

        let result = util.batch_delete_item(keys).await.unwrap();
        assert_eq!(result, ());
    }
}
