use std::os::raw::c_int;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub(crate) struct RegistryTracker {
    pub(crate) recent_key_validity: Vec<Arc<(c_int, AtomicBool)>>,
    pub(crate) unref_list: Vec<c_int>,
}

impl RegistryTracker {
    pub(crate) fn new() -> Self {
        Self {
            recent_key_validity: Vec::new(),
            unref_list: Vec::new(),
        }
    }
}
