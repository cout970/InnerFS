use std::cell::RefCell;
use std::rc::Rc;
use crate::sql::{FileRow, FILE_KIND_DIRECTORY, FILE_KIND_REGULAR};
use serde::{Deserialize, Serialize};

pub type FsTreeRef = Rc<RefCell<FsTree>>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct FsTree {
    pub id: i64,
    pub kind: FsTreeKind,
    pub name: String,
    pub uid: i64,
    pub gid: i64,
    pub perms: i64,
    pub size: i64,
    pub sha512: String,
    pub encryption_key: String,
    pub accessed_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub children: Vec<FsTreeRef>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum FsTreeKind {
    File,
    Directory,
}

impl <'a> From<FileRow> for FsTree {
    fn from(value: FileRow) -> FsTree {
        FsTree {
            id: value.id,
            kind: match value.kind {
                FILE_KIND_REGULAR => FsTreeKind::File,
                FILE_KIND_DIRECTORY => FsTreeKind::Directory,
                _ => panic!("Invalid kind"),
            },
            name: value.name,
            uid: value.uid,
            gid: value.gid,
            perms: value.perms,
            size: value.size,
            sha512: value.sha512,
            encryption_key: value.encryption_key,
            accessed_at: value.accessed_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
            children: vec![],
        }
    }
}