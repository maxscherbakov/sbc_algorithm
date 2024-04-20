use super::*;

#[test]
fn test_hash_function() {
    let string = String::from("Blue");
    let mut data = Vec::new();
    for byte in string.bytes() {
        data.push(byte);
    }
    assert_eq!(hash_function::hash(data.as_slice()), 1);
}

