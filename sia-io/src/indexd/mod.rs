use crate::tagged::{TaggedValue, TryFromInner, WithFromStr};
use bon::bon;
use reqwest::Url;
use sia_storage::{AppMetadata, Hash256, PublicKey};
use std::borrow::Cow;
use std::convert::Infallible;

pub mod client;
pub mod download;
pub mod object;

pub struct HostKeyKind;
pub type HostKey = TaggedValue<HostKeyKind, PublicKey>;

pub struct AppIdKind;
pub type AppId = TaggedValue<AppIdKind, Hash256>;

impl TryFromInner<Hash256> for AppId {
    type Err = Infallible;

    fn try_from_inner(inner: Hash256) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        Ok(Self::new_from_inner(inner))
    }
}
impl WithFromStr for AppId {}

#[derive(Debug, Clone)]
pub struct AppDetails {
    id: AppId,
    name: Cow<'static, str>,
    description: Cow<'static, str>,
    service_url: Url,
    logo_url: Option<Url>,
    callback_url: Option<Url>,
}

#[bon]
impl AppDetails {
    #[builder]
    pub fn new(
        id: AppId,
        #[builder(into)] name: Cow<'static, str>,
        #[builder(into)] description: Cow<'static, str>,
        service_url: Url,
        logo_url: Option<Url>,
        callback_url: Option<Url>,
    ) -> Self {
        Self {
            id,
            name,
            description,
            service_url,
            logo_url,
            callback_url,
        }
    }
}

impl From<AppDetails> for AppMetadata {
    fn from(value: AppDetails) -> Self {
        Self {
            id: value.id.into_inner(),
            name: into_static_str(value.name),
            description: into_static_str(value.description),
            service_url: into_static_str(value.service_url.to_string().into()),
            logo_url: value
                .logo_url
                .map(|v| into_static_str(v.to_string().into())),
            callback_url: value
                .callback_url
                .map(|v| into_static_str(v.to_string().into())),
        }
    }
}

fn into_static_str(s: Cow<'static, str>) -> &'static str {
    match s {
        Cow::Borrowed(s) => s,
        Cow::Owned(s) => {
            // Warning: this leaks memory, the string will live for the program's duration
            Box::leak(s.into_boxed_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::confidential::{NewSecretExt, RevealExt, RevealMutExt};
    use crate::indexd::client::Client;
    use crate::indexd::{AppDetails, AppId};
    use anyhow::anyhow;
    use ct_codecs::{Decoder, Encoder, Hex};
    use futures_util::AsyncReadExt;
    use futures_util::io::Cursor;
    use reqwest::Url;
    use sia_storage::AppKey;
    use std::io::BufRead;
    use std::iter;
    use std::str::FromStr;

    static ONE_MB: &[u8] = include_bytes!("../../testdata/1mb.bin");

    fn app_details() -> Result<AppDetails, anyhow::Error> {
        let app_id = AppId::from_str(std::env::var("INDEXD_APP_ID").unwrap().as_str())?;

        Ok(AppDetails::builder()
            .id(app_id)
            .name(std::env::var("INDEXD_APP_NAME")?)
            .description(std::env::var("INDEXD_APP_DESCRIPTION")?)
            .service_url(Url::parse(
                std::env::var("INDEXD_APP_SERVICE_URL")?.as_str(),
            )?)
            .build())
    }

    fn app_key() -> Result<AppKey, anyhow::Error> {
        let hex = std::env::var("INDEXD_APP_KEY")?;
        Ok(AppKey::import(
            Hex::decode_to_vec(hex, None)?
                .try_into()
                .map_err(|_| anyhow!("app key invalid format"))?,
        ))
    }

    #[ignore]
    #[tokio::test]
    async fn acquire_authorization() -> Result<(), anyhow::Error> {
        dotenv::dotenv().ok();
        let app_details = app_details()?;
        let handle =
            Client::acquire_authorization(std::env::var("INDEXD_ENDPOINT")?, app_details).await?;

        eprintln!();
        eprintln!("AUTHORIZATION URL: {}", handle.url().as_str());
        eprintln!("waiting for authorization");
        let handle = handle.await_authorization().await?;

        eprintln!();
        eprintln!("enter mnemonic and press enter");
        let mut mnemonic = String::new().confidential();
        std::io::stdin().lock().read_line(mnemonic.reveal_mut())?;
        let trim_end = mnemonic.reveal().trim_end().len();
        mnemonic.reveal_mut().truncate(trim_end);
        let app_key = handle.finalize(&mnemonic).await?;

        eprintln!();
        eprintln!(
            "APP KEY: {}",
            Hex::encode_to_string(app_key.reveal().export())?
        );
        eprintln!();
        Ok(())
    }

    async fn connect() -> Result<Client, anyhow::Error> {
        dotenv::dotenv().ok();
        let app_details = app_details()?;
        let app_key = app_key()?;

        Ok(Client::builder()
            .indexd_endpoint(std::env::var("INDEXD_ENDPOINT")?)
            .app_details(app_details)
            .app_key(&app_key)
            .build()
            .await?)
    }

    #[ignore]
    #[tokio::test]
    async fn integration_test1() -> Result<(), anyhow::Error> {
        let client = connect().await?;

        let (objects, _) = client.list_objects().await?;
        assert!(objects.len() < 10);
        if !objects.is_empty() {
            eprintln!("deleting objects from previous run");
            for object in objects {
                client.delete_objects(iter::once(object.id())).await?;
            }
            let (objects, _) = client.list_objects().await?;
            assert_eq!(objects.len(), 0);
        }

        let object = client.upload(Cursor::new(ONE_MB), None).await?;
        eprintln!("object: {:?}", object);
        let (objects, _) = client.list_objects().await?;
        assert_eq!(objects.len(), 1);

        let dl = client.download(object.id()).await?;
        assert_eq!(dl.object().id(), object.id());
        assert_eq!(dl.object().size(), ONE_MB.len() as u64);

        let mut buf = Vec::with_capacity(ONE_MB.len());
        let mut reader = dl.open(None).await?;
        let read = reader.read_to_end(&mut buf).await?;
        assert_eq!(read, ONE_MB.len());
        assert_eq!(&buf, ONE_MB);

        let _ = client
            .update_object_metadata(object.id(), "this is a test".as_bytes().to_vec())
            .await?;
        let object = client.object(object.id()).await?;
        assert_eq!(object.metadata(), "this is a test".as_bytes());

        client.delete_objects(iter::once(object.id())).await?;
        let (objects, _) = client.list_objects().await?;
        assert!(objects.is_empty());

        Ok(())
    }
}
