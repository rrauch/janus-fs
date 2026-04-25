use crate::confidential::{Confidential, NewSecretExt, RevealExt, Sensitive};
use crate::indexd::AppDetails;
use bon::bon;
use derive_where::derive_where;
use sia_storage::{
    AppKey, ApprovedState, Builder, BuilderError, Error as SiaError, IntoUrl,
    RequestingApprovalState, SDK, SealedObjectError, Url,
};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    SiaError(#[from] SiaError),
    #[error(transparent)]
    BuilderError(#[from] BuilderError),
    #[error(transparent)]
    SealedObjectError(#[from] SealedObjectError),
    #[error("user authorization required")]
    AuthorizationRequired,
    #[error("client error: {0}")]
    Other(String),
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Client(pub(crate) Arc<Inner>);

#[bon]
impl Client {
    #[builder]
    pub async fn new(
        indexd_endpoint: impl IntoUrl,
        app_details: AppDetails,
        app_key: &AppKey,
        #[builder(default = 65536)] download_buffer_size: usize,
        download_max_inflight: Option<usize>,
        upload_max_inflight: Option<usize>,
    ) -> Result<Self, ClientError> {
        let builder = Builder::new(indexd_endpoint, app_details.into())?;
        let sdk = builder
            .connected(app_key)
            .await?
            .ok_or(ClientError::AuthorizationRequired)?;

        Ok(Self(Arc::new(Inner {
            sdk,
            download_buffer_size,
            download_max_inflight,
            upload_max_inflight,
        })))
    }
}

pub struct AwaitingUserAuthorization {
    url: Url,
    builder: Builder<RequestingApprovalState>,
}

pub struct Finalize {
    builder: Builder<ApprovedState>,
}

pub struct AuthorizationHandle<S> {
    state: S,
}

impl AuthorizationHandle<AwaitingUserAuthorization> {
    pub fn url(&self) -> &Url {
        &self.state.url
    }

    pub async fn await_authorization(self) -> Result<AuthorizationHandle<Finalize>, ClientError> {
        let builder = self.state.builder.wait_for_approval().await?;
        Ok(AuthorizationHandle {
            state: Finalize { builder },
        })
    }
}

impl AuthorizationHandle<Finalize> {
    pub async fn finalize(
        self,
        mnemonic: &Confidential<String>,
    ) -> Result<Sensitive<AppKey>, ClientError> {
        let sdk = self
            .state
            .builder
            .register(mnemonic.reveal().as_str())
            .await?;
        Ok(sdk.app_key().clone().sensitive())
    }
}

impl Client {
    pub async fn acquire_authorization(
        indexd_endpoint: impl IntoUrl,
        app_details: AppDetails,
    ) -> Result<AuthorizationHandle<AwaitingUserAuthorization>, ClientError> {
        let builder = Builder::new(indexd_endpoint, app_details.into())?
            .request_connection()
            .await?;
        let url = Url::from_str(builder.response_url())
            .map_err(|e| ClientError::Other(format!("authorization url invalid: {}", e)))?;
        Ok(AuthorizationHandle {
            state: AwaitingUserAuthorization { url, builder },
        })
    }

    #[inline]
    pub(crate) fn sdk(&self) -> &SDK {
        &self.0.sdk
    }

    #[inline]
    pub(crate) fn download_buffer_size(&self) -> usize {
        self.0.download_buffer_size
    }

    #[inline]
    pub(crate) fn download_max_inflight(&self) -> Option<usize> {
        self.0.download_max_inflight
    }

    #[inline]
    pub(crate) fn upload_max_inflight(&self) -> Option<usize> {
        self.0.upload_max_inflight
    }
}

#[derive_where(Debug)]
pub(crate) struct Inner {
    #[derive_where(skip)]
    sdk: SDK,
    download_buffer_size: usize,
    download_max_inflight: Option<usize>,
    upload_max_inflight: Option<usize>,
}
