use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Admin,
    ReadWrite,
    ReadOnly,
}

impl Role {
    pub fn can_read(&self) -> bool {
        true
    }

    pub fn can_write(&self) -> bool {
        matches!(self, Role::Admin | Role::ReadWrite)
    }

    pub fn can_admin(&self) -> bool {
        matches!(self, Role::Admin)
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(Role::Admin),
            "readwrite" | "read_write" => Some(Role::ReadWrite),
            "readonly" | "read_only" => Some(Role::ReadOnly),
            _ => None,
        }
    }
}
