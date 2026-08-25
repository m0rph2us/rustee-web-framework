use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;

use crate::{
    AiBatchArtifactKind, AiBatchArtifactLoader, AiBatchArtifactProcessor, AiBatchArtifactReference,
    AiBatchCatalog, AiBatchProvider, AiBatchReceipt, AiBatchReference,
};

#[derive(Clone)]
pub(super) struct Catalog {
    pub(super) calls: Arc<Mutex<usize>>,
}

impl AiBatchCatalog for Catalog {
    type Work = String;
    type Error = Infallible;

    fn load(
        &self,
        _reference: AiBatchReference,
    ) -> BoxFuture<'static, Result<Self::Work, Self::Error>> {
        let calls = self.calls.clone();
        Box::pin(async move {
            *calls.lock().unwrap() += 1;
            Ok("private batch prompt and expected answer".to_owned())
        })
    }
}

#[derive(Clone)]
pub(super) struct Provider {
    pub(super) calls: Arc<Mutex<usize>>,
    pub(super) fail: bool,
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("test batch provider unavailable")]
pub(super) enum ProviderError {
    Unavailable,
}

impl AiBatchProvider<String> for Provider {
    type Error = ProviderError;

    fn submit(
        &self,
        _reference: AiBatchReference,
        _work: String,
    ) -> BoxFuture<'static, Result<AiBatchReceipt, Self::Error>> {
        let calls = self.calls.clone();
        let fail = self.fail;
        Box::pin(async move {
            *calls.lock().unwrap() += 1;
            if fail {
                Err(ProviderError::Unavailable)
            } else {
                Ok(AiBatchReceipt::new("provider-batch-42").unwrap())
            }
        })
    }
}

pub(super) fn reference() -> AiBatchReference {
    AiBatchReference::new("tenant-a.policy-v2", "catalog-17", "run-20260803-1").unwrap()
}

pub(super) fn artifact_reference() -> AiBatchArtifactReference {
    AiBatchArtifactReference::new(
        reference(),
        AiBatchArtifactKind::Output,
        "file-output-17",
        "reconcile-output-17",
    )
    .unwrap()
}

#[derive(Clone)]
pub(super) struct ArtifactLoader {
    pub(super) calls: Arc<Mutex<usize>>,
}

impl AiBatchArtifactLoader for ArtifactLoader {
    type Artifact = String;
    type Error = Infallible;

    fn load_artifact(
        &self,
        _reference: AiBatchArtifactReference,
    ) -> BoxFuture<'static, Result<Self::Artifact, Self::Error>> {
        let calls = self.calls.clone();
        Box::pin(async move {
            *calls.lock().unwrap() += 1;
            Ok("private provider batch output body".to_owned())
        })
    }
}

#[derive(Clone)]
pub(super) struct ArtifactProcessor {
    pub(super) calls: Arc<Mutex<usize>>,
    pub(super) fail: bool,
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("test artifact processor unavailable")]
pub(super) enum ArtifactProcessorError {
    Unavailable,
}

impl AiBatchArtifactProcessor<String> for ArtifactProcessor {
    type Error = ArtifactProcessorError;

    fn process_artifact(
        &self,
        _reference: AiBatchArtifactReference,
        _artifact: String,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        let calls = self.calls.clone();
        let fail = self.fail;
        Box::pin(async move {
            *calls.lock().unwrap() += 1;
            if fail {
                Err(ArtifactProcessorError::Unavailable)
            } else {
                Ok(())
            }
        })
    }
}
