use std::collections::HashMap;
use std::ffi::OsStr;
use std::rc::Rc;

use fuse::{FileAttr, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request};
use libc::{c_int, ENOENT, ENOSYS, EOPNOTSUPP};
use time::{get_time, Timespec};

use crate::config::Config;
use crate::sql::{FileRow, SQL};
use crate::sql_fs::SqlFileSystem;
use crate::storage::Storage;

pub struct ProxyFileSystem {
    pub fs: SqlFileSystem,
    pub open_files: HashMap<u64, u64>,
    pub fh_counter: u64,
}

impl ProxyFileSystem {
    pub fn new(sql: Rc<SQL>, config: Rc<Config>, storage: Box<dyn Storage>) -> Self {
        ProxyFileSystem {
            fs: SqlFileSystem::new(sql, config, storage),
            open_files: HashMap::new(),
            fh_counter: 0,
        }
    }
}

impl Filesystem for ProxyFileSystem {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        println!("FS init");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        println!("FS destroy");
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        println!("FS statfs(ino: {})", _ino);
        reply.statfs(
            65536,
            65536,
            65536,
            9999999,
            9999999,
            65536,
            255,
            65536,
        );
    }

    fn lookup(&mut self, _req: &Request, parent: u64, os_name: &OsStr, reply: ReplyEntry) {
        println!("FS lookup(parent: {}, name: {:?})", parent, os_name);
        let name = os_name.to_string_lossy();

        match self.fs.lookup(parent, &name) {
            Ok(file) => {
                if let Some(file) = file {
                    let attr = FileAttr::from(&file);
                    reply.entry(&get_time(), &attr, 0);
                } else {
                    reply.error(ENOENT);
                }
            }
            Err(e) => {
                eprintln!("Error looking up file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("FS getattr(ino: {})", ino);

        match self.fs.getattr(ino as i64) {
            Ok(file) => {
                let attr = FileAttr::from(&file);
                reply.attr(&get_time(), &attr);
            }
            Err(e) => {
                eprintln!("Error looking up file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn setattr(&mut self, _req: &Request, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>, fh: Option<u64>, crtime: Option<Timespec>, chgtime: Option<Timespec>, bkuptime: Option<Timespec>, flags: Option<u32>, reply: ReplyAttr) {
        println!("FS setattr(ino: {}, mode: {:?}, uid: {:?}, gid: {:?}, size: {:?}, atime: {:?}, mtime: {:?}, fh: {:?}, crtime: {:?}, chgtime: {:?}, bkuptime: {:?}, flags: {:?})", ino, mode, uid, gid, size, atime, mtime, fh, crtime, chgtime, bkuptime, flags);

        match self.fs.setattr(
            ino as i64, mode, uid, gid, size,
            atime.map(|i| i.sec),
            mtime.map(|i| i.sec),
            crtime.map(|i| i.sec),
        ) {
            Ok(file) => {
                let attr = FileAttr::from(&file);
                reply.attr(&get_time(), &attr);
            }
            Err(e) => {
                eprintln!("Error looking up file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn mknod(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, _rdev: u32, reply: ReplyEntry) {
        println!("FS mknod(parent: {}, name: {:?}, mode: {}, rdev: {})", parent, name, mode, _rdev);
        let name = name.to_string_lossy();
        match self.fs.mknod(parent as i64, &name, mode) {
            Ok(file) => {
                let attr = FileAttr::from(&file);
                reply.entry(&get_time(), &attr, 0);
            }
            Err(e) => {
                eprintln!("Error creating file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        println!("FS mkdir(parent: {}, name: {:?}, mode: {})", parent, name, mode);
        let name = name.to_string_lossy();
        match self.fs.mkdir(parent, &name, mode) {
            Ok(file) => {
                let attr = FileAttr::from(&file);
                reply.entry(&get_time(), &attr, 0);
            }
            Err(e) => {
                eprintln!("Error creating directory: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!("FS unlink(parent: {}, name: {:?})", parent, name);
        let name = name.to_string_lossy();
        match self.fs.unlink(parent as i64, &name) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                eprintln!("Error unlinking file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!("FS rmdir(parent: {}, name: {:?})", parent, name);
        let name = name.to_string_lossy();
        match self.fs.rmdir(parent as i64, &name) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                eprintln!("Error removing directory: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn rename(&mut self, _req: &Request, parent: u64, os_name: &OsStr, new_parent_id: u64, new_os_name: &OsStr, reply: ReplyEmpty) {
        println!("FS rename(parent: {}, name: {:?}, new_parent: {}, new_name: {:?})", parent, os_name, new_parent_id, new_os_name);

        if parent == new_parent_id && os_name == new_os_name {
            reply.ok();
            return;
        }

        // Not allowed to move across directories
        if parent != new_parent_id {
            eprintln!("Unable to move file to new folder: Functionality not supported");
            reply.error(EOPNOTSUPP);
            return;
        }

        let old_name = os_name.to_string_lossy();
        let new_name = new_os_name.to_string_lossy();

        match self.fs.rename(parent as i64, &old_name, &new_name) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                eprintln!("Error renaming file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        println!("FS open(ino: {}, flags: {})", ino, flags);

        match self.fs.open(ino as i64, flags) {
            Ok(_) => {
                self.fh_counter += 1;
                let fh = self.fh_counter;
                self.open_files.insert(fh, ino);
                reply.opened(fh, flags);
            }
            Err(e) => {
                eprintln!("Error opening file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        println!("FS read(ino: {}, file_handle: {}, offset: {}, size: {})", ino, fh, offset, size);
        match self.fs.read(ino as i64, offset, size as usize) {
            Ok(data) => {
                reply.data(&data);
            }
            Err(e) => {
                eprintln!("Error reading file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, data: &[u8], flags: u32, reply: ReplyWrite) {
        println!("FS write(ino: {}, file_handle: {}, offset: {}, data: {} B, flags: {})", ino, fh, offset, data.len(), flags);
        match self.fs.write(ino as i64, offset, data) {
            Ok(size) => {
                reply.written(size as u32);
            }
            Err(e) => {
                eprintln!("Error writing file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        println!("FS flush(ino: {}, file_handle: {})", _ino, _fh);
        reply.ok();
    }

    fn release(&mut self, _req: &Request, ino: u64, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool, reply: ReplyEmpty) {
        println!("FS release(ino: {}, file_handle: {}, flags: {})", ino, fh, _flags);
        match self.fs.release(ino as i64) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                eprintln!("Error releasing file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        println!("FS fsync(ino: {}, file_handle: {}, datasync: {})", _ino, _fh, _datasync);
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        println!("FS opendir(ino: {}, flags: {})", _ino, _flags);
        reply.opened(0, 0);
    }

    fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        println!("FS readdir(ino: {}, file_handle: {}, offset: {})", ino, fh, offset);

        match self.fs.readdir(ino as i64, offset) {
            Ok(entries) => {
                let mut index = offset + 1;
                for e in entries {
                    let fuse_kind = if e.kind == 1 { fuse::FileType::Directory } else { fuse::FileType::RegularFile };
                    if reply.add(e.entry_file_id as u64, index, fuse_kind, e.name) {
                        break;
                    }
                    index += 1;
                }
                reply.ok();
            }
            Err(e) => {
                eprintln!("Error reading directory: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: u32, reply: ReplyEmpty) {
        println!("FS releasedir(ino: {}, file_handle: {}, flags: {})", _ino, _fh, _flags);
        reply.ok();
    }

    fn fsyncdir(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        println!("FS fsyncdir(ino: {}, file_handle: {}, datasync: {})", _ino, _fh, _datasync);
        reply.ok();
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        println!("FS access(ino: {}, mask: {})", _ino, _mask);
        eprintln!("Access not implemented");
        reply.error(ENOSYS);
    }

    fn create(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, flags: u32, reply: ReplyCreate) {
        println!("FS create(parent: {}, name: {:?}, mode: {}, flags: {})", parent, name, mode, flags);

        let name = name.to_string_lossy();
        let res = self.fs.mknod(parent as i64, &name, flags);

        if let Err(e) = res {
            eprintln!("Error creating file: {:?}", e.error);
            reply.error(e.code);
            return;
        }

        let file = res.unwrap();

        match self.fs.open(file.id, flags) {
            Ok(_) => {
                self.fh_counter += 1;
                let fh = self.fh_counter;
                self.open_files.insert(fh, file.id as u64);

                let attr = FileAttr::from(&file);
                reply.created(&get_time(), &attr, 0, fh, flags);
            }
            Err(e) => {
                eprintln!("Error opening file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }
}

impl From<&FileRow> for FileAttr {
    fn from(value: &FileRow) -> Self {
        FileAttr {
            ino: value.id as u64,
            size: value.size as u64,
            blocks: value.size as u64 / 512,
            atime: Timespec::new(value.accessed_at, 0i32),
            mtime: Timespec::new(value.updated_at, 0i32),
            ctime: Timespec::new(value.updated_at, 0i32),
            crtime: Timespec::new(value.created_at, 0i32),
            kind: if value.kind == 1 { fuse::FileType::Directory } else { fuse::FileType::RegularFile },
            perm: value.perms as u16,
            nlink: 1,
            uid: value.uid as u32,
            gid: value.gid as u32,
            rdev: 0,
            flags: 0,
        }
    }
}
