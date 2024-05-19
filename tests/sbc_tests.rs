
#[test]
fn test_hash_function() {
    let string = String::from("hello_world!");
    let mut data = Vec::new();
    for byte in string.bytes() {
        data.push(byte);
    }
    // let hash = hash(data.as_slice());
    // println!("{:b}", hash);
}
