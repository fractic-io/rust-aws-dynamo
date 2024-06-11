pub fn placeholder() {
    ()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result() {
        let result = placeholder();
        assert_eq!(result, ());
    }
}
