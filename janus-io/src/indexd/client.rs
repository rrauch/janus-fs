use crate::confidential::{Confidential, NewSecretExt, RevealExt, Sensitive};
use crate::indexd::AppDetails;
use bon::bon;
use derive_where::derive_where;
use sia_storage::{
    ApprovedState, Builder, BuilderError, Error as SiaError, RequestingApprovalState, Sdk,
    SealedObjectError, Url,
};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

pub use sia_storage::Account;
pub use sia_storage::AppKey;

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
        indexd_endpoint: Url,
        app_details: AppDetails,
        app_key: &AppKey,
        download_max_buffered_chunks: Option<usize>,
        upload_max_buffered_slabs: Option<usize>,
    ) -> Result<Self, ClientError> {
        let builder = Builder::new(indexd_endpoint.clone(), app_details.into())?;
        let sdk = builder
            .connected(app_key)
            .await?
            .ok_or(ClientError::AuthorizationRequired)?;

        Ok(Self(Arc::new(Inner {
            sdk,
            endpoint: indexd_endpoint,
            download_max_buffered_chunks,
            upload_max_buffered_slabs,
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
    pub fn endpoint(&self) -> &Url {
        &self.0.endpoint
    }

    pub async fn acquire_authorization(
        indexd_endpoint: Url,
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

    pub async fn account(&self) -> Result<Account, ClientError> {
        Ok(self.0.sdk.account().await?)
    }

    #[inline]
    pub(crate) fn sdk(&self) -> &Sdk {
        &self.0.sdk
    }

    #[inline]
    pub(crate) fn download_max_buffered_chunks(&self) -> Option<usize> {
        self.0.download_max_buffered_chunks
    }

    #[inline]
    pub(crate) fn upload_max_buffered_slabs(&self) -> Option<usize> {
        self.0.upload_max_buffered_slabs
    }
}

#[derive_where(Debug)]
pub(crate) struct Inner {
    #[derive_where(skip)]
    sdk: Sdk,
    endpoint: Url,
    download_max_buffered_chunks: Option<usize>,
    upload_max_buffered_slabs: Option<usize>,
}
