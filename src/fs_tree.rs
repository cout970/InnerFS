use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use crate::metadata_db::{FileRow, FILE_KIND_DIRECTORY, FILE_KIND_REGULAR};
use serde::{Deserialize, Serialize};
use crate::AnyError;

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

impl<'a> From<FileRow> for FsTree {
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

impl FsTree {
    pub fn for_each<F>(root: FsTreeRef, mut func: F) -> Result<(), AnyError>
    where
        F: FnMut(&FsTree, PathBuf) -> Result<(), AnyError>,
    {
        let mut queue = vec![(root, PathBuf::new())];

        while !queue.is_empty() {
            let (node_ref, sub_path) = queue.pop().unwrap();
            let dir_node = node_ref.borrow();

            for child_ref in &dir_node.children {
                let child = child_ref.borrow();
                let child_path = sub_path.join(&child.name);

                if child.kind == FsTreeKind::Directory {
                    queue.push((child_ref.clone(), child_path.clone()));
                }

                func(&child, child_path)?;
            }
        }

        Ok(())
    }
}