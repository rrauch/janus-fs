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
