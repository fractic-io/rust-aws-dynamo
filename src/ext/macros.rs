/// Query Dynamo for mixed child types using `DynamoUtil::query_generic`, then
/// filter+parse items into typed `Vec<T>` outputs in a single pass.
///
/// Syntax:
/// ```rust,ignore
/// let (a, b, c) = query_generic!(
///     dynamo_util => DynamoGenericQuery::pk(id.pk).sk_begins_with(id.sk);
///     TypeA, TypeB, TypeC
/// );
/// ```
///
/// Notes:
/// - Runs exactly one Dynamo query.
/// - Ignores any returned items whose `object_type()` does not match any of the
///   expected types.
/// - Preserves per-type ordering as returned by Dynamo.
#[macro_export]
macro_rules! query_generic {
    (
        $dynamo_util:expr => $query:expr;
        $($ty:ty),+ $(,)?
    ) => {{
        $crate::query_generic!(
            @zip
            $dynamo_util, $query;
            (0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63);
            ();
            $($ty),+
        )
    }};

    // Zip type list with tuple indices (tt-muncher).
    // --
    (@zip
        $dynamo_util:expr, $query:expr;
        ($idx0:tt $($idx_rest:tt)*);
        ($($pairs:tt)*);
        $ty0:ty, $($ty_rest:ty),+
    ) => {{
        $crate::query_generic!(
            @zip
            $dynamo_util, $query;
            ($($idx_rest)*);
            ($($pairs)* ($idx0, $ty0));
            $($ty_rest),+
        )
    }};

    (@zip
        $dynamo_util:expr, $query:expr;
        ($idx0:tt $($idx_rest:tt)*);
        ($($pairs:tt)*);
        $ty0:ty
    ) => {{
        $crate::query_generic!(
            @run
            $dynamo_util, $query;
            ( $($pairs)* ($idx0, $ty0) )
        )
    }};

    // Core implementation.
    // --
    (@run
        $dynamo_util:expr, $query:expr;
        ( $(($idx:tt, $ty:ty))* )
    ) => {{
        use $crate::schema::{parsing::parse_dynamo_map, PkSk};

        // Initialize output tuple.
        let mut __out = ( $( Vec::<$ty>::new() ),* );

        // Query once.
        let __items = $dynamo_util
            .query_generic($query)
            .await?;

        // Partition+parse in a single pass.
        for __item in __items.into_iter() {
            let __id = PkSk::from_map(&__item)?;
            let __t = __id.object_type()?;
            match __t {
                $(
                    t if t == <$ty as $crate::schema::DynamoObject>::id_label() => {
                        __out.$idx.push(parse_dynamo_map::<$ty>(&__item)?);
                    }
                )*
                _ => {}
            }
        }

        __out
    }};
}
