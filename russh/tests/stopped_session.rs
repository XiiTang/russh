#![cfg(not(target_arch = "wasm32"))]

use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use russh::{
    client,
    keys::{PrivateKey, PublicKeyOrCertificate, agent::AgentIdentity, ssh_key},
    server,
};

struct TrustKey;

impl client::Handler for TrustKey {
    type Error = russh::Error;

    async fn check_server_key(&mut self, _: &PublicKeyOrCertificate) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

struct Peer {
    answers: Arc<AtomicUsize>,
}

impl server::Handler for Peer {
    type Error = russh::Error;

    async fn auth_keyboard_interactive<'a>(
        &'a mut self,
        _: &str,
        _: &str,
        response: Option<server::Response<'a>>,
    ) -> Result<server::Auth, Self::Error> {
        if response.is_some() {
            self.answers.fetch_add(1, Ordering::SeqCst);
            return Ok(server::Auth::reject());
        }
        Ok(server::Auth::Partial {
            name: Cow::Borrowed("stop regression"),
            instructions: Cow::Borrowed("leave this challenge unanswered"),
            prompts: Cow::Owned(vec![(Cow::Borrowed("answer: "), false)]),
        })
    }

    async fn auth_publickey(
        &mut self,
        _: &str,
        _: russh::server::VerifiedPublicKey<'_>,
    ) -> Result<server::Auth, Self::Error> {
        self.answers.fetch_add(1, Ordering::SeqCst);
        Ok(server::Auth::reject())
    }

    async fn auth_openssh_certificate(
        &mut self,
        _: &str,
        _: &ssh_key::Certificate,
    ) -> Result<server::Auth, Self::Error> {
        self.answers.fetch_add(1, Ordering::SeqCst);
        Ok(server::Auth::reject())
    }
}

fn key(seed: u8) -> PrivateKey {
    use ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
    PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(
        &[seed; 32],
    )))
}

struct Fixture {
    handle: client::Handle<TrustKey>,
    peer: tokio::task::JoinHandle<Result<(), russh::Error>>,
    answers: Arc<AtomicUsize>,
}

impl Fixture {
    async fn connect() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let answers = Arc::new(AtomicUsize::new(0));
        let handler = Peer {
            answers: answers.clone(),
        };
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let config = server::Config {
                keys: vec![key(72)],
                inactivity_timeout: None,
                ..Default::default()
            };
            server::run_stream(Arc::new(config), stream, handler)
                .await?
                .await
        });
        let handle = client::connect(Arc::new(client::Config::default()), address, TrustKey)
            .await
            .unwrap();
        Self {
            handle,
            peer,
            answers,
        }
    }

    async fn stop_and_read(mut self) {
        assert!(!self.handle.is_closed());
        self.handle.stop();
        self.handle.stop(); // Repeated requests must leave the same end readable.
        (&mut self.handle).await.unwrap();
        assert!(self.handle.is_closed());
        self.handle.stop(); // Stopping an ended session is harmless too.

        // Read the real peer end before fixture teardown; no answer is fabricated.
        match self.peer.await.unwrap() {
            Ok(()) => {}
            Err(russh::Error::IO(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {}
            result => panic!("unexpected peer end: {result:?}"),
        }
        assert_eq!(self.answers.load(Ordering::SeqCst), 0);
    }
}

struct HoldSigning {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
}

impl russh::Signer for HoldSigning {
    type Error = russh::Error;

    async fn auth_sign(
        &mut self,
        _: &AgentIdentity,
        _: Option<ssh_key::HashAlg>,
        _: Vec<u8>,
    ) -> Result<Vec<u8>, Self::Error> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        std::future::pending().await
    }
}

#[tokio::test]
async fn stop_ends_a_session_waiting_for_keyboard_interactive_answers() {
    // Only a hang budget; receiving the prompt establishes the parked receive.
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut fixture = Fixture::connect().await;
        let reply = fixture
            .handle
            .authenticate_keyboard_interactive_start("test-user", None)
            .await
            .unwrap();
        assert!(matches!(
            reply,
            client::KeyboardInteractiveAuthResponse::InfoRequest { prompts, .. }
                if prompts.len() == 1 && prompts[0].prompt == "answer: "
        ));
        fixture.stop_and_read().await;
    })
    .await
    .expect("stop left the session or peer running at a keyboard-interactive wait");
}

#[tokio::test]
async fn stop_ends_a_session_waiting_for_a_publickey_signature() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut fixture = Fixture::connect().await;
        let (entered, signing) = tokio::sync::oneshot::channel();
        let mut signer = HoldSigning {
            entered: Some(entered),
        };
        let mut authentication = Box::pin(fixture.handle.authenticate_publickey_with(
            "test-user",
            key(73).public_key().clone(),
            None,
            &mut signer,
        ));
        // 0.65 abandons a refused signature. Hold the signer instead, so no
        // Signed or SignFailed is sent before the owner stops the session.
        tokio::select! {
            result = &mut authentication => panic!("signing unexpectedly ended: {result:?}"),
            result = signing => result.unwrap(),
        }
        drop(authentication);
        fixture.stop_and_read().await;
    })
    .await
    .expect("stop left the session or peer running at a public-key signing wait");
}

#[tokio::test]
async fn stop_ends_a_session_waiting_for_a_certificate_signature() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut fixture = Fixture::connect().await;
        let (entered, signing) = tokio::sync::oneshot::channel();
        let mut signer = HoldSigning {
            entered: Some(entered),
        };
        let user_key = key(74);
        let mut builder = ssh_key::certificate::Builder::new_with_random_nonce(
            &mut rand::rng(),
            user_key.public_key(),
            0,
            u64::MAX,
        )
        .unwrap();
        builder
            .cert_type(ssh_key::certificate::CertType::User)
            .unwrap();
        builder.key_id("stop-regression").unwrap();
        builder.valid_principal("test-user").unwrap();
        let certificate = builder.sign(&key(75)).unwrap();
        let mut authentication = Box::pin(fixture.handle.authenticate_certificate_with(
            "test-user",
            certificate,
            None,
            &mut signer,
        ));
        tokio::select! {
            result = &mut authentication => panic!("signing unexpectedly ended: {result:?}"),
            result = signing => result.unwrap(),
        }
        drop(authentication);
        fixture.stop_and_read().await;
    })
    .await
    .expect("stop left the session or peer running at a certificate signing wait");
}
