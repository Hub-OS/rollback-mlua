use std::fmt;
use std::hash::{Hash, Hasher};
use std::os::raw::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// An auto generated key into the Lua registry.
///
/// This is a handle to a value stored inside the Lua registry. It is not automatically
/// garbage collected on Drop, but it can be removed with [`Lua::remove_registry_value`],
/// and instances not manually removed can be garbage collected with [`Lua::expire_registry_values`].
///
/// Be warned, If you place this into Lua via a [`UserData`] type or a rust callback, it is *very
/// easy* to accidentally cause reference cycles that the Lua garbage collector cannot resolve.
/// Instead of placing a [`RegistryKey`] into a [`UserData`] type, prefer instead to use
/// [`AnyUserData::set_user_value`] / [`AnyUserData::get_user_value`].
///
/// [`UserData`]: crate::UserData
/// [`RegistryKey`]: crate::RegistryKey
/// [`Lua::remove_registry_value`]: crate::Lua::remove_registry_value
/// [`Lua::expire_registry_values`]: crate::Lua::expire_registry_values
/// [`AnyUserData::set_user_value`]: crate::AnyUserData::set_user_value
/// [`AnyUserData::get_user_value`]: crate::AnyUserData::get_user_value
pub struct RegistryKey {
    pub(crate) registry_id: c_int,
    pub(crate) validity: Arc<(c_int, AtomicBool)>,
    pub(crate) registry_tracker: Arc<Mutex<RegistryTracker>>,
}

impl fmt::Debug for RegistryKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RegistryKey({})", self.validity.0)
    }
}

impl Hash for RegistryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.registry_id.hash(state)
    }
}

impl PartialEq for RegistryKey {
    fn eq(&self, other: &RegistryKey) -> bool {
        Arc::ptr_eq(&self.validity, &other.validity)
    }
}

impl Eq for RegistryKey {}

impl Drop for RegistryKey {
    fn drop(&mut self) {
        if self.is_valid() {
            let mut registry_tracker = self.registry_tracker.lock().expect("unref list poisoned");
            registry_tracker.unref_list.push(self.validity.0);
        }
    }
}

impl RegistryKey {
    // Destroys the RegistryKey without adding to the drop list
    pub(crate) fn take(self) -> c_int {
        let registry_id = self.registry_id;
        unsafe {
            std::ptr::read(&self.validity);
            std::ptr::read(&self.registry_tracker);
            std::mem::forget(self);
        }
        registry_id
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.validity.1.load(Ordering::Relaxed)
    }
}

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
