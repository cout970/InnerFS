use crate::metadata_db::{FileRow, FILE_KIND_DIRECTORY, FILE_KIND_REGULAR};
use crate::AnyError;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::Arc;

pub type FsTreeRef = Arc<RefCell<FsTree>>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct FsTree {
    pub id: i64,
    pub version: i64,
    pub kind: FsTreeKind,
    pub name: String,
    pub external_id: String,
    pub uid: i64,
    pub gid: i64,
    pub perms: i64,
    pub size: i64,
    pub sha512: String,
    pub encryption_key: String,
    pub compression: String,
    pub accessed_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub children: Vec<FsTreeChild>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct FsTreeChild {
    pub name: String,
    pub external_id: String,
    pub kind: FsTreeKind,
    pub file: FsTreeRef,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum FsTreeKind {
    File,
    Directory,
}

impl FsTreeKind {
    pub fn to_file_kind(&self) -> i64 {
        match self {
            FsTreeKind::File => FILE_KIND_REGULAR,
            FsTreeKind::Directory => FILE_KIND_DIRECTORY,
        }
    }
    pub fn from_i64(value: i64) -> FsTreeKind {
        match value {
            FILE_KIND_REGULAR => FsTreeKind::File,
            FILE_KIND_DIRECTORY => FsTreeKind::Directory,
            _ => panic!("Invalid kind"),
        }
    }
}

impl<'a> From<FileRow> for FsTree {
    fn from(value: FileRow) -> FsTree {
        FsTree {
            id: value.id,
            version: value.version,
            kind: FsTreeKind::from_i64(value.kind),
            name: value.name,
            external_id: value.external_id,
            uid: value.uid,
            gid: value.gid,
            perms: value.perms,
            size: value.size,
            sha512: value.sha512,
            encryption_key: value.encryption_key,
            compression: value.compression,
            accessed_at: value.accessed_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
            children: vec![],
        }
    }
}

impl FsTree {
    pub fn for_each<F>(root: FsTreeRef, mut func: F) -> Result<(), AnyError>
    where
        F: FnMut(&FsTree, PathBuf) -> Result<(), AnyError>,
    {
        let mut queue = vec![(root, PathBuf::new())];

        while !queue.is_empty() {
            let (node_ref, sub_path) = queue.pop().unwrap();
            let dir_node = node_ref.borrow();

            for child_tree in &dir_node.children {
                let child = child_tree.file.borrow();
                let child_path = sub_path.join(&child.name);

                if child.kind == FsTreeKind::Directory {
                    queue.push((child_tree.file.clone(), child_path.clone()));
                }

                func(&child, child_path)?;
            }
        }

        Ok(())
    }
}
