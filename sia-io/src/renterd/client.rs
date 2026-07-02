use crate::Password;
use crate::confidential::RevealExt;
use crate::renterd::BucketName;
use crate::renterd::object::{FolderId, ObjectId, ObjectKeyError, SupportedObjectKind};
use bon::bon;
use futures_io::AsyncRead;
use futures_util::{AsyncReadExt, stream};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Body, Client as ReqwestClient, Response, Url};
use serde_json::Value;
use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use typed_path::Utf8UnixPathBuf;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error("incorrect api password")]
    AuthenticationError,
    #[error("http response error, status code:`{0}`, text: `{1}`")]
    HttpResponseError(u16, String),
    #[error("server sent 404 not found")]
    NotFoundError,
    #[error("wrong bucket, expected '{expected}' but got '{actual}'")]
    WrongBucket {
        expected: BucketName,
        actual: BucketName,
    },
    #[error("invalid path: {0}")]
    InvalidPath(Utf8UnixPathBuf),
    #[error(transparent)]
    ObjectKeyError(#[from] ObjectKeyError),
}

pub struct ApiPasswordKind;
pub type ApiPassword = Password<ApiPasswordKind>;

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Client(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    api_endpoint: Url,
    api_password: Option<ApiPassword>,
    root: FolderId,
    reqwest_client: ReqwestClient,
}

#[bon]
impl Client {
    #[builder]
    pub fn new(
        api_endpoint: Url,
        api_password: Option<ApiPassword>,
        #[builder(default)] bucket: BucketName,
        #[builder(default = "/", into)] root: String,
        reqwest_client_builder: Option<reqwest::ClientBuilder>,
    ) -> Result<Self, ClientError> {
        let root = ObjectId::new_root(bucket, root)?;

        Ok(Self(Arc::new(Inner {
            api_endpoint,
            api_password,
            root,
            reqwest_client: reqwest_client_builder.unwrap_or_default().build()?,
        })))
    }

    async fn api_request_builder(
        &self,
        request: ApiRequest<'_>,
    ) -> Result<reqwest::RequestBuilder, ClientError> {
        let url = self
            .0
            .api_endpoint
            .join(request.path.as_ref())
            .expect("endpoint url join error");

        let mut request_builder = match request.request_type {
            RequestType::Get => self.0.reqwest_client.get(url),
            RequestType::Post => self.0.reqwest_client.post(url),
            RequestType::Put => self.0.reqwest_client.put(url),
            RequestType::Delete => self.0.reqwest_client.delete(url),
        };

        if let Some(params) = &request.params {
            request_builder = request_builder.query(params);
        }

        if let Some(headers) = &request.headers {
            for (k, v) in headers {
                request_builder = request_builder.header(k.as_ref(), v.as_ref());
            }
        }

        if let Some(content) = request.content {
            match content {
                RequestContent::Json(json) => request_builder = request_builder.json(&json),
                RequestContent::Stream(stream, content_type, metadata) => {
                    if let Some(content_type) = content_type {
                        request_builder =
                            request_builder.header(CONTENT_TYPE, content_type.as_ref());
                    }
                    if let Some(metadata) = metadata {
                        for (name, value) in metadata.into_iter() {
                            request_builder = request_builder
                                .header(format!("X-Sia-Meta-{}", name.as_ref()), value.as_ref());
                        }
                    }
                    request_builder = request_builder.body(Body::wrap_stream(stream::try_unfold(
                        (stream, vec![0u8; 64 * 1024]),
                        |(mut stream, mut buf)| async move {
                            let n = match Pin::new(&mut stream).read(&mut buf).await {
                                Ok(0) => return Ok(None), // end of stream
                                Ok(n) => n,
                                Err(e) => return Err(e),
                            };
                            Ok(Some((buf[..n].to_vec(), (stream, buf))))
                        },
                    )));
                }
            }
        }

        if let Some(api_password) = &self.0.api_password {
            request_builder = request_builder.basic_auth("api", Some(api_password.reveal()));
        }

        Ok(request_builder)
    }

    pub(crate) async fn send_api_request(
        &self,
        request: ApiRequest<'_>,
    ) -> Result<Response, ClientError> {
        match self.send_api_request_optional(request).await {
            Ok(Some(resp)) => Ok(resp),
            Ok(None) => Err(ClientError::NotFoundError),
            Err(e) => Err(e),
        }
    }

    pub(crate) async fn send_api_request_optional(
        &self,
        request: ApiRequest<'_>,
    ) -> Result<Option<Response>, ClientError> {
        let req = self.api_request_builder(request).await?.build()?;
        let resp = self.0.reqwest_client.execute(req).await?;
        let status = resp.status();
        if status.as_u16() == 401 {
            return Err(ClientError::AuthenticationError);
        }

        if status.as_u16() == 404 {
            return Ok(None);
        }

        if status.is_client_error() || status.is_server_error() {
            let text = resp
                .text_with_charset("utf-8")
                .await
                .ok()
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| "".to_string());
            return Err(ClientError::HttpResponseError(status.as_u16(), text));
        }

        Ok(Some(resp))
    }

    #[inline]
    pub fn root(&self) -> &FolderId {
        &self.0.root
    }

    #[inline]
    pub fn bucket(&self) -> &BucketName {
        self.0.root.bucket()
    }

    pub(crate) fn check_object_id<T: SupportedObjectKind>(
        &self,
        object_id: &ObjectId<T>,
    ) -> Result<(), ClientError> {
        if object_id.bucket() != self.root().bucket() {
            Err(ClientError::WrongBucket {
                expected: self.root().bucket().clone(),
                actual: object_id.bucket().clone(),
            })?
        }
        object_id.key().check_root(self.root().key())?;
        Ok(())
    }
}

pub(crate) struct ApiRequest<'a> {
    path: Cow<'a, str>,
    params: Option<Vec<(Cow<'a, str>, Cow<'a, str>)>>,
    headers: Option<Vec<(Cow<'a, str>, Cow<'a, str>)>>,
    content: Option<RequestContent<'a>>,
    request_type: RequestType,
}

pub(crate) struct ApiRequestBuilder<'a> {
    request: ApiRequest<'a>,
}

impl<'a> ApiRequestBuilder<'a> {
    fn new<T: Into<Cow<'a, str>>>(path: T, request_type: RequestType) -> Self {
        Self {
            request: ApiRequest {
                request_type,
                path: path.into(),
                params: None,
                headers: None,
                content: None,
            },
        }
    }

    pub(crate) fn get<T: Into<Cow<'a, str>>>(path: T) -> Self {
        Self::new(path, RequestType::Get)
    }

    pub(crate) fn post<T: Into<Cow<'a, str>>>(path: T) -> Self {
        Self::new(path, RequestType::Post)
    }

    pub(crate) fn put<T: Into<Cow<'a, str>>>(path: T) -> Self {
        Self::new(path, RequestType::Put)
    }

    pub(crate) fn delete<T: Into<Cow<'a, str>>>(path: T) -> Self {
        Self::new(path, RequestType::Delete)
    }

    pub(crate) fn params<K: Into<Cow<'a, str>>, V: Into<Cow<'a, str>>>(
        mut self,
        params: Option<Vec<(K, V)>>,
    ) -> Self {
        self.request.params =
            params.map(|v| v.into_iter().map(|(k, v)| (k.into(), v.into())).collect());
        self
    }

    pub(crate) fn headers<K: Into<Cow<'a, str>>, V: Into<Cow<'a, str>>>(
        mut self,
        headers: Option<Vec<(K, V)>>,
    ) -> Self {
        self.request.headers =
            headers.map(|v| v.into_iter().map(|(k, v)| (k.into(), v.into())).collect());
        self
    }

    pub(crate) fn content(mut self, content: Option<RequestContent<'a>>) -> Self {
        self.request.content = content;
        self
    }

    pub(crate) fn build(self) -> ApiRequest<'a> {
        self.request
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum RequestType {
    Get,
    Post,
    Put,
    Delete,
}

pub(crate) enum RequestContent<'a> {
    Json(Value),
    Stream(
        Box<dyn AsyncRead + Send + Unpin + 'static>,
        Option<Cow<'a, str>>,
        Option<Vec<(Cow<'a, str>, Cow<'a, str>)>>,
    ),
}
