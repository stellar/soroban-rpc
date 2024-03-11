use ed25519_dalek::ed25519::signature::Keypair;
use sha2::{Digest, Sha256};

use crate::{txn::requires_auth, Error};
use soroban_env_host::xdr::{
    self, AccountId, Hash, HashIdPreimage, HashIdPreimageSorobanAuthorization, Limits,
    OperationBody, PublicKey, ScAddress, ScMap, ScSymbol, ScVal, SorobanAddressCredentials,
    SorobanAuthorizationEntry, SorobanCredentials, Transaction, Uint256, WriteXdr,
};
pub trait Signer {
    fn new<T>(network_passphrase: &str, options: Option<T>) -> Self;

    fn network_hash(&self) -> xdr::Hash;
    fn sign_txn(&self) -> Result<String, Error>;

    fn sign_soroban_authorizations(
        &self,
        raw: &Transaction,
        source_account: stellar_strkey::ed25519::PublicKey,
        signature_expiration_ledger: u32,
    ) -> Result<Option<Transaction>, Error> {
        let mut tx = raw.clone();
        let Some(mut op) = requires_auth(&tx) else {
            return Ok(None);
        };

        let xdr::Operation {
            body: OperationBody::InvokeHostFunction(ref mut body),
            ..
        } = op
        else {
            return Ok(None);
        };

        let signed_auths = body
            .auth
            .as_slice()
            .iter()
            .map(|raw_auth| {
                Ok(self.maybe_sign_soroban_authorization_entry(
                    raw_auth,
                    signature_expiration_ledger,
                )?)
            })
            .collect::<Result<Vec<_>, Error>>()?;

        body.auth = signed_auths.try_into()?;
        tx.operations = vec![op].try_into()?;
        Ok(Some(tx))
    }

    fn maybe_sign_soroban_authorization_entry(
        &self,
        unsigned_entry: &SorobanAuthorizationEntry,
        signature_expiration_ledger: u32,
    ) -> Result<SorobanAuthorizationEntry, Error> {
        if let SorobanAuthorizationEntry {
            credentials: SorobanCredentials::Address(SorobanAddressCredentials { ref address, .. }),
            ..
        } = unsigned_entry
        {
            // See if we have a signer for this authorizationEntry
            // If not, then we Error
            let needle = match address {
                ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(ref a)))) => {
                    a
                }
                ScAddress::Contract(Hash(c)) => {
                    // This address is for a contract. This means we're using a custom
                    // smart-contract account. Currently the CLI doesn't support that yet.
                    return Err(Error::MissingSignerForAddress {
                        address: stellar_strkey::Strkey::Contract(stellar_strkey::Contract(*c))
                            .to_string(),
                    });
                }
            };
            self.sign_soroban_authorization_entry(
                unsigned_entry,
                signature_expiration_ledger,
                needle,
            )
        } else {
            Ok(unsigned_entry.clone())
        }
    }
    fn sign_soroban_authorization_entry(
        &self,
        unsigned_entry: &SorobanAuthorizationEntry,
        signature_expiration_ledger: u32,
        address: &[u8; 32],
    ) -> Result<SorobanAuthorizationEntry, Error>;
}

struct DefaultSigner {
    network_passphrase: String,
    keypairs: Vec<ed25519_dalek::SigningKey>,
}

impl Signer for DefaultSigner {
    fn new(network_passphrase: &str, options: Option<&[ed25519_dalek::SigningKey]>) -> Self {
        DefaultSigner {
            network_passphrase: network_passphrase.to_string(),
            keypairs: options.map(|keys| keys.to_vec()).unwrap_or_default(),
        }
    }

    fn sign_txn(&self) -> Result<String, Error> {
        Ok("".to_string())
    }

    fn sign_soroban_authorization_entry(
        &self,
        unsigned_entry: &SorobanAuthorizationEntry,
        signature_expiration_ledger: u32,
        signer: &[u8; 32],
    ) -> Result<SorobanAuthorizationEntry, Error> {
        let mut auth = unsigned_entry.clone();
        let SorobanAuthorizationEntry {
            credentials: SorobanCredentials::Address(ref mut credentials),
            ..
        } = auth
        else {
            // Doesn't need special signing
            return Ok(auth);
        };
        let SorobanAddressCredentials { nonce, .. } = credentials;

        let preimage = HashIdPreimage::SorobanAuthorization(HashIdPreimageSorobanAuthorization {
            network_id: self.network_hash(),
            invocation: auth.root_invocation.clone(),
            nonce: *nonce,
            signature_expiration_ledger,
        })
        .to_xdr(Limits::none())?;

        let payload = Sha256::digest(preimage);
        let signer = self.keypairs
            .iter()
            .find(|keypair| {
                keypair.verifying_key().to_bytes() == signer
            })
            .ok_or(Error::MissingSignerForAddress {
                address: signer.to_string(),
            })?;
        let signature = signer.sign(&payload);

        let map = ScMap::sorted_from(vec![
            (
                ScVal::Symbol(ScSymbol("public_key".try_into()?)),
                ScVal::Bytes(
                    signer
                        .verifying_key()
                        .to_bytes()
                        .to_vec()
                        .try_into()
                        .map_err(Error::Xdr)?,
                ),
            ),
            (
                ScVal::Symbol(ScSymbol("signature".try_into()?)),
                ScVal::Bytes(
                    signature
                        .to_bytes()
                        .to_vec()
                        .try_into()
                        .map_err(Error::Xdr)?,
                ),
            ),
        ])
        .map_err(Error::Xdr)?;
        credentials.signature = ScVal::Vec(Some(
            vec![ScVal::Map(Some(map))].try_into().map_err(Error::Xdr)?,
        ));
        credentials.signature_expiration_ledger = signature_expiration_ledger;
        auth.credentials = SorobanCredentials::Address(credentials.clone());

        Ok(auth)
    }

    fn network_hash(&self) -> xdr::Hash {
        xdr::Hash(Sha256::digest(self.network_passphrase.as_bytes()))
    }
}
