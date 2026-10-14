use super::*;

#[test]
fn meta_rejects_non_ascii_and_bad_names() {
    assert!(validate_meta(&[json!({"nm":"Utc","ut":"ms"})]).is_ok());
    assert!(validate_meta(&[json!({"nm":"Oil Temp"})]).is_err());
    assert!(validate_meta(&[json!({"nm":"OilTemp","ut":"°C"})]).is_err());
    assert!(validate_meta(&[]).is_err());
}

#[test]
fn samples_must_be_numeric() {
    assert!(validate_samples(&[vec![json!(1), json!(34.843112), json!(3)]]).is_ok());
    assert!(validate_samples(&[vec![json!("1")]]).is_err());
    assert!(validate_samples(&[]).is_err());
}
