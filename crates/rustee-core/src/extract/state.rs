//! Copy-on-write typed application state and its request extractor.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
    sync::Arc,
};

use futures_util::future::BoxFuture;

use crate::{Error, Request, Result, RouteParams};

use super::FromRequest;

/// Shared application state request extractor.
///
/// [`Debug`] identifies only the state type so application configuration and credentials do not
/// enter framework diagnostics accidentally.
#[derive(Clone)]
pub struct State<T>(pub Arc<T>);

impl<T> fmt::Debug for State<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("State")
            .field("type_name", &std::any::type_name::<T>())
            .finish()
    }
}

/// Cloneable type-indexed application state used by [`State`].
#[derive(Clone, Default)]
pub struct StateStore {
    values: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl fmt::Debug for StateStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateStore")
            .field("registered_types", &self.values.len())
            .finish()
    }
}

impl StateStore {
    /// Adds or replaces a state value by concrete type.
    pub fn insert<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.values).insert(TypeId::of::<T>(), Arc::new(value));
    }

    /// Returns a clone of a typed state handle.
    #[must_use]
    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| value.downcast::<T>().ok())
    }
}

impl<T> FromRequest for State<T>
where
    T: Send + Sync + 'static,
{
    fn from_request<'a>(
        _request: &'a mut Request,
        _params: &'a RouteParams,
        state: &'a StateStore,
    ) -> BoxFuture<'a, Result<Self>> {
        Box::pin(async move { state.get::<T>().map(Self).ok_or_else(Error::internal) })
    }
}
