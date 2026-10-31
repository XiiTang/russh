use rand::rng;
// Copyright 2016 Pierre-Étienne Meunier
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use ssh_encoding::{Decode, Reader};
use ssh_key::public::KeyData;
use ssh_key::{Algorithm, EcdsaCurve, PublicKey};

use crate::keys::Error;

pub trait PublicKeyExt {
    fn decode(bytes: &[u8]) -> Result<PublicKey, Error>;
}

impl PublicKeyExt for PublicKey {
    fn decode(mut bytes: &[u8]) -> Result<PublicKey, Error> {
        check_cert_nesting(bytes)?;
        let key = KeyData::decode(&mut bytes)?;
        Ok(PublicKey::new(key, ""))
    }
}

#[doc(hidden)]
pub trait Verify {
    fn verify_client_auth(&self, buffer: &[u8], sig: &[u8]) -> bool;
    fn verify_server_auth(&self, buffer: &[u8], sig: &[u8]) -> bool;
}

/// Upper bound on certificate nesting in a key blob.
const MAX_CERT_NESTING: usize = 8;

/// Reject key/certificate blobs nested deeply enough to overflow the stack.
///
/// `ssh-key` recursively decodes a certificate's `signature_key` without a depth
/// limit. Follow only that field iteratively, leaving opaque data such as key_id
/// out of the nesting count. Drop once ssh-key caps the depth itself.
pub(crate) fn check_cert_nesting(mut blob: &[u8]) -> Result<(), ssh_key::Error> {
    let mut depth = 0;
    loop {
        // Use Certificate::decode's algorithm recognition, including custom names.
        let Ok(algorithm) = Algorithm::new_certificate(&String::decode(&mut blob)?) else {
            return Ok(());
        };
        depth += 1;
        if depth > MAX_CERT_NESTING {
            return Err(ssh_key::Error::CertificateFieldInvalid(
                ssh_key::certificate::Field::SignatureKey,
            ));
        }

        // Follow Certificate::decode_as without recursively decoding signature_key.
        blob.read_prefixed(|_| Ok::<_, ssh_key::Error>(()))?; // nonce
        KeyData::decode_as(&mut blob, algorithm)?; // nonrecursive subject key
        u64::decode(&mut blob)?; // serial
        u32::decode(&mut blob)?; // certificate type
        for _ in 0..2 {
            // key id and principals
            blob.read_prefixed(|_| Ok::<_, ssh_key::Error>(()))?;
        }
        u64::decode(&mut blob)?; // valid after
        u64::decode(&mut blob)?; // valid before
        for _ in 0..3 {
            // critical options, extensions and reserved
            blob.read_prefixed(|_| Ok::<_, ssh_key::Error>(()))?;
        }
        blob = blob.read_prefixed(|body| Ok::<_, ssh_key::Error>(*body))?;
    }
}

/// Parse a public key from a byte slice.
pub fn parse_public_key(mut p: &[u8]) -> Result<PublicKey, Error> {
    check_cert_nesting(p)?;
    Ok(ssh_key::public::KeyData::decode(&mut p)?.into())
}

/// Obtain a cryptographic-safe random number generator.
pub fn safe_rng() -> impl rand::CryptoRng {
    rng()
}

mod private_key_with_hash_alg {
    use std::ops::Deref;
    use std::sync::Arc;

    use ssh_key::Algorithm;

    use crate::helpers::AlgorithmExt;

    /// Helper structure to correlate a key and (in case of RSA) a hash algorithm.
    /// Only used for authentication, not key storage as RSA keys do not inherently
    /// have a hash algorithm associated with them.
    #[derive(Clone, Debug)]
    pub struct PrivateKeyWithHashAlg {
        key: Arc<crate::keys::PrivateKey>,
        hash_alg: Option<crate::keys::HashAlg>,
    }

    impl PrivateKeyWithHashAlg {
        /// Direct constructor.
        ///
        /// For RSA, passing `None` is mapped to the legacy `sha-rsa` (SHA-1).
        /// For other keys, `hash_alg` is ignored.
        pub fn new(
            key: Arc<crate::keys::PrivateKey>,
            mut hash_alg: Option<crate::keys::HashAlg>,
        ) -> Self {
            if !key.algorithm().is_rsa() {
                hash_alg = None;
            }
            Self { key, hash_alg }
        }

        pub fn algorithm(&self) -> Algorithm {
            self.key.algorithm().with_hash_alg(self.hash_alg)
        }

        pub fn hash_alg(&self) -> Option<crate::keys::HashAlg> {
            self.hash_alg
        }
    }

    impl Deref for PrivateKeyWithHashAlg {
        type Target = crate::keys::PrivateKey;

        fn deref(&self) -> &Self::Target {
            &self.key
        }
    }
}

pub use private_key_with_hash_alg::PrivateKeyWithHashAlg;

pub const ALL_KEY_TYPES: &[Algorithm] = &[
    Algorithm::Dsa,
    Algorithm::Ecdsa {
        curve: EcdsaCurve::NistP256,
    },
    Algorithm::Ecdsa {
        curve: EcdsaCurve::NistP384,
    },
    Algorithm::Ecdsa {
        curve: EcdsaCurve::NistP521,
    },
    Algorithm::Ed25519,
    #[cfg(feature = "rsa")]
    Algorithm::Rsa { hash: None },
    #[cfg(feature = "rsa")]
    Algorithm::Rsa {
        hash: Some(ssh_key::HashAlg::Sha256),
    },
    #[cfg(feature = "rsa")]
    Algorithm::Rsa {
        hash: Some(ssh_key::HashAlg::Sha512),
    },
    Algorithm::SkEcdsaSha2NistP256,
    Algorithm::SkEd25519,
];

#[cfg(test)]
mod test {
    fn s(v: &mut Vec<u8>, b: &[u8]) {
        v.extend((b.len() as u32).to_be_bytes());
        v.extend(b);
    }

    /// Smallest certificate chain where each level's CA key is the next certificate.
    fn nested_cert(depth: usize) -> Vec<u8> {
        let mut inner = Vec::new();
        s(&mut inner, b"a@b");
        s(&mut inner, b"");
        for _ in 0..depth {
            let mut v = Vec::new();
            s(&mut v, b"a-cert-v01@x");
            s(&mut v, b""); // nonce
            s(&mut v, b""); // opaque public key
            v.extend(0u64.to_be_bytes()); // serial
            v.extend(1u32.to_be_bytes()); // user cert
            s(&mut v, b""); // key id
            s(&mut v, b""); // principals
            v.extend(0u64.to_be_bytes());
            v.extend(u64::MAX.to_be_bytes());
            s(&mut v, b""); // critical options
            s(&mut v, b""); // extensions
            s(&mut v, b""); // reserved
            s(&mut v, &inner); // signature key
            let mut sig = Vec::new();
            s(&mut sig, b"a@b");
            s(&mut sig, b"x");
            s(&mut v, &sig);
            inner = v;
        }
        inner
    }

    #[test]
    fn certificate_key_id_markers_do_not_count_as_nesting() -> Result<(), ssh_key::Error> {
        use ssh_key::certificate::Builder;

        let ca = ssh_key::PrivateKey::from(ssh_key::private::Ed25519Keypair::from_seed(&[1; 32]));
        let subject =
            ssh_key::PrivateKey::from(ssh_key::private::Ed25519Keypair::from_seed(&[2; 32]));
        let mut builder = Builder::new(vec![0; 32], subject.public_key().clone(), 0, u64::MAX)?;
        builder.key_id("-cert-v01@".repeat(8))?;
        builder.valid_principal("fixture")?;
        let certificate = builder.sign(&ca)?;
        let blob = certificate.to_bytes()?;
        let decoded = ssh_key::Certificate::from_bytes(&blob)?;
        decoded.verify_signature()?;

        assert!(super::check_cert_nesting(&blob).is_ok());
        assert!(super::parse_public_key(&blob).is_ok());
        assert!(crate::cert::PublicKeyOrCertificate::decode(
            "ssh-ed25519-cert-v01@openssh.com",
            &blob,
        )
        .is_ok());
        Ok(())
    }

    #[test]
    fn deeply_nested_certificate_is_rejected() {
        // Shallow chain is well-formed and still decodes.
        assert!(super::parse_public_key(&nested_cert(2)).is_ok());
        assert!(super::parse_public_key(&nested_cert(8)).is_ok());
        assert!(super::parse_public_key(&nested_cert(9)).is_err());
        // One packet's worth of nesting; without the guard this aborts on stack overflow.
        let deep = nested_cert(2848);
        assert!(super::parse_public_key(&deep).is_err());
        assert!(crate::cert::PublicKeyOrCertificate::decode("ssh-ed25519", &deep).is_err());
        assert!(crate::cert::PublicKeyOrCertificate::decode(
            "ssh-ed25519-cert-v01@openssh.com",
            &deep
        )
        .is_err());
    }
}
