#![cfg(not(target_arch = "wasm32"))]

use std::error::Error as _;

#[tokio::test]
async fn a_session_join_error_exposes_its_original_panic_cause() {
    let end: Result<(), russh::Error> = russh_util::runtime::spawn(async {
        panic!("SSH session panic cause");
    })
    .await
    .map_err(russh::Error::from);
    let error = end.unwrap_err();
    assert!(matches!(error, russh::Error::Join(_)));
    assert!(error.to_string().contains("SSH session panic cause"));
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_panic()
    );
}
