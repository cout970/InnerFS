use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::time::{Duration, SystemTime};
use cntr_fuse::{FileAttr, FileType, Filesystem, ReplyAttr, ReplyBmap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyRead, ReplyStatfs, ReplyWrite, Request, UtimeSpec};
use libc::{c_int, ENOENT, ENOSYS, O_APPEND, O_CREAT, O_DSYNC, O_EXCL, O_NOATIME, O_NOCTTY, O_NONBLOCK, O_PATH, O_RDONLY, O_RDWR, O_SYNC, O_TMPFILE, O_TRUNC, O_WRONLY};
use log::{error, trace, warn};

use crate::metadata_db::{FileRow, FILE_KIND_DIRECTORY};
use crate::sql_fs::SqlFileSystem;
use crate::utils::{current_timestamp, system_time_from_timestamp, timestamp_from_system_time};

const BLOCK_SIZE: u32 = 65536; // 64kb
const FINE_LOGGING: bool = false;

pub struct FuseFileSystem {
    pub fs: SqlFileSystem,
    pub open_files: HashMap<u64, u64>,
    pub fh_counter: u64,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct OpenFlags {
    pub read_only: bool,
    pub write_only: bool,
    pub read_write: bool,
    pub append: bool,
    pub create: bool,
    pub exclusive: bool,
    pub truncate: bool,
    pub no_tty: bool,
    pub non_block: bool,
    pub sync: bool,
    pub data_sync: bool,
    pub no_access_time: bool,
    pub path_only: bool,
    pub tmp_file: bool,
    pub other: i32,
}

impl OpenFlags {
    pub fn from(flags: i32) -> Self {
        OpenFlags {
            read_only: flags & 0x03 == O_RDONLY,
            write_only: flags & 0x03 == O_WRONLY,
            read_write: flags & 0x03 == O_RDWR,
            append: flags & O_APPEND != 0,
            create: flags & O_CREAT != 0,
            exclusive: flags & O_EXCL != 0,
            truncate: flags & O_TRUNC != 0,
            no_tty: flags & O_NOCTTY != 0,
            non_block: flags & O_NONBLOCK != 0,
            sync: flags & O_SYNC != 0,
            data_sync: flags & O_DSYNC != 0,
            no_access_time: flags & O_NOATIME != 0,
            path_only: flags & O_PATH != 0,
            tmp_file: flags & O_TMPFILE != 0,
            other: flags & !(O_WRONLY | O_APPEND | O_CREAT | O_EXCL | O_TRUNC | O_NOCTTY | O_NONBLOCK | O_SYNC | O_DSYNC | O_NOATIME | O_PATH | O_TMPFILE),
        }
    }

    pub fn to_safe_flags(&self) -> i32 {
        let mut flags = 0;
        if self.read_only {
            flags |= O_RDONLY;
        }
        if self.write_only {
            flags |= O_WRONLY;
        }
        if self.read_write {
            flags |= O_RDWR;
        }
        if self.append {
            flags |= O_APPEND;
        }
        if self.create {
            flags |= O_CREAT;
        }
        flags | self.other
    }
}

impl FuseFileSystem {
    pub fn new(fs: SqlFileSystem) -> Self {
        FuseFileSystem {
            fs,
            open_files: HashMap::new(),
            fh_counter: 0,
        }
    }

    pub fn get_ttl(&self) -> Duration {
        Duration::from_secs(1)
    }
}

impl Filesystem for FuseFileSystem {
    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        trace!("FS init");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        trace!("FS destroy");
    }

    fn lookup(&mut self, _req: &Request, parent: u64, os_name: &OsStr, reply: ReplyEntry) {
        if FINE_LOGGING {
            trace!("FS lookup(parent: {}, name: {:?})", parent, os_name);
        }
        let name = os_name.to_string_lossy();

        match self.fs.lookup(parent as i64, &name) {
            Ok(file) => {
                if let Some(file) = file {
                    let attr = FileAttr::from(&file);
                    reply.entry(&self.get_ttl(), &attr, 0);
                } else {
                    reply.error(ENOENT);
                }
            }
            Err(e) => {
                if e.code != ENOENT {
                    warn!("Error looking up file: {:?}", e.error);
                }
                reply.error(e.code);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if FINE_LOGGING {
            trace!("FS getattr(ino: {})", ino);
        }

        match self.fs.getattr(ino as i64) {
            Ok(file) => {
                let attr = FileAttr::from(&file);
                reply.attr(&self.get_ttl(), &attr);
            }
            Err(e) => {
                if e.code != ENOENT {
                    error!("Error getattr: {:#}", e.error);
                }
                reply.error(e.code);
            }
        }
    }

    fn setattr(&mut self, _req: &Request, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: UtimeSpec, mtime: UtimeSpec, fh: Option<u64>, crtime: Option<SystemTime>, chgtime: Option<SystemTime>, bkuptime: Option<SystemTime>, flags: Option<u32>, reply: ReplyAttr) {
        trace!("FS setattr(ino: {}, mode: {:?}, uid: {:?}, gid: {:?}, size: {:?}, atime: {:?}, mtime: {:?}, fh: {:?}, crtime: {:?}, chgtime: {:?}, bkuptime: {:?}, flags: {:?})", ino, mode, uid, gid, size, atime, mtime, fh, crtime, chgtime, bkuptime, flags);

        let atime = match atime {
            UtimeSpec::Now => Some(current_timestamp()),
            UtimeSpec::Omit => None,
            UtimeSpec::Time(t) => Some(timestamp_from_system_time(t))
        };

        let mtime = match mtime {
            UtimeSpec::Now => Some(current_timestamp()),
            UtimeSpec::Omit => None,
            UtimeSpec::Time(t) => Some(timestamp_from_system_time(t))
        };

        match self.fs.setattr(
            ino as i64, mode, uid, gid, size,
            atime,
            mtime,
            crtime.map(|i| timestamp_from_system_time(i)),
        ) {
            Ok(file) => {
                let attr = FileAttr::from(&file);
                reply.attr(&self.get_ttl(), &attr);
            }
            Err(e) => {
                if e.code != ENOENT {
                    error!("Error setattr: {:#}", e.error);
                }
                reply.error(e.code);
            }
        }
    }

    fn readlink(&mut self, _req: &Request, _ino: u64, reply: ReplyData) {
        trace!("FS readlink(ino: {})", _ino);
        warn!("Readlink not implemented");
        reply.error(ENOSYS);
    }

    fn mknod(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, _rdev: u32, reply: ReplyEntry) {
        trace!("FS mknod(parent: {}, name: {:?}, mode: {}, umask: {}, rdev: {})", parent, name, mode, _umask, _rdev);
        let name = name.to_string_lossy();
        match self.fs.mknod(parent as i64, &name, req.uid(), req.gid(), mode) {
            Ok(file) => {
                let attr = FileAttr::from(&file);
                reply.entry(&self.get_ttl(), &attr, 0);
            }
            Err(e) => {
                error!("Error creating file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        trace!("FS mkdir(parent: {}, name: {:?}, mode: {}, umask: {})", parent, name, mode, _umask);
        let name = name.to_string_lossy();
        match self.fs.mkdir(parent as i64, &name, req.uid(), req.gid(), mode) {
            Ok(file) => {
                let attr = FileAttr::from(&file);
                reply.entry(&self.get_ttl(), &attr, 0);
            }
            Err(e) => {
                error!("Error creating directory: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        trace!("FS unlink(parent: {}, name: {:?})", parent, name);
        let name = name.to_string_lossy();
        match self.fs.unlink(parent as i64, &name) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                if e.code != ENOENT {
                    error!("Error unlinking file: {:?}", e.error);
                }
                reply.error(e.code);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        trace!("FS rmdir(parent: {}, name: {:?})", parent, name);
        let name = name.to_string_lossy();
        match self.fs.rmdir(parent as i64, &name) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                if e.code != ENOENT {
                    error!("Error removing directory: {:?}", e.error);
                }
                reply.error(e.code);
            }
        }
    }

    fn symlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, _link: &Path, reply: ReplyEntry) {
        trace!("FS symlink(parent: {}, name: {:?}, link: {:?})", _parent, _name, _link);
        warn!("Symlink not implemented");
        reply.error(ENOSYS);
    }

    fn rename(&mut self, _req: &Request, parent: u64, os_name: &OsStr, new_parent_id: u64, new_os_name: &OsStr, reply: ReplyEmpty) {
        trace!("FS rename(parent: {}, name: {:?}, new_parent: {}, new_name: {:?})", parent, os_name, new_parent_id, new_os_name);

        if parent == new_parent_id && os_name == new_os_name {
            reply.ok();
            return;
        }

        let old_name = os_name.to_string_lossy();
        let new_name = new_os_name.to_string_lossy();

        let file = self.fs.lookup(parent as i64, old_name.as_ref()).unwrap();
        match file {
            Some(file) => {
                if self.open_files.values().any(|f| *f == file.id as u64) {
                    error!("Error renaming file {} {:?} to {:?}, file in use ({:?})", file.id, old_name, new_name, self.open_files);
                    reply.error(ENOSYS);
                    return;
                }
            }
            None => {
                reply.error(ENOENT);
                return;
            }
        }

        // Not allowed to move across directories
        if parent != new_parent_id {
            match self.fs.move_file(parent as i64, &old_name, new_parent_id as i64, &new_name) {
                Ok(_) => {
                    reply.ok();
                }
                Err(e) => {
                    error!("Error renaming file: {:?}", e.error);
                    reply.error(e.code);
                }
            }
            return;
        }

        match self.fs.rename(parent as i64, &old_name, &new_name) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                error!("Error renaming file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn link(&mut self, _req: &Request, _ino: u64, _newparent: u64, _newname: &OsStr, reply: ReplyEntry) {
        trace!("FS link(ino: {}, newparent: {}, newname: {:?})", _ino, _newparent, _newname);
        warn!("Link not implemented");
        reply.error(ENOSYS);
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        trace!("FS open(ino: {}, flags: {})", ino, flags);

        let open_flags = OpenFlags::from(flags as i32);
        let flags = open_flags.to_safe_flags() as u32;

        match self.fs.open(ino as i64, flags) {
            Ok(_) => {
                self.fh_counter += 1;
                let fh = self.fh_counter;
                self.open_files.insert(fh, ino);
                reply.opened(fh, flags);
            }
            Err(e) => {
                if e.code != ENOENT {
                    error!("Error opening file: {:?}", e.error);
                }
                reply.error(e.code);
            }
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyRead) {
        trace!("FS read(ino: {}, file_handle: {}, offset: {}, size: {})", ino, fh, offset, size);
        match self.fs.read(ino as i64, offset, size as usize) {
            Ok(data) => {
                reply.data(&data);
            }
            Err(e) => {
                    error!("Error reading file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn write(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, data: &[u8], flags: u32, reply: ReplyWrite) {
        trace!("FS write(ino: {}, file_handle: {}, offset: {}, data: {} B, flags: {})", ino, fh, offset, data.len(), flags);
        match self.fs.write(ino as i64, offset, data) {
            Ok(size) => {
                reply.written(size as u32);
            }
            Err(e) => {
                error!("Error writing file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn flush(&mut self, _req: &Request, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        trace!("FS flush(ino: {}, file_handle: {})", ino, _fh);
        match self.fs.flush(ino as i64) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                error!("Error flushing file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn release(&mut self, _req: &Request, ino: u64, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool, reply: ReplyEmpty) {
        trace!("FS release(ino: {}, file_handle: {}, flags: {})", ino, fh, _flags);
        match self.fs.release(ino as i64) {
            Ok(_) => {
                self.open_files.remove(&fh);
                reply.ok();
            }
            Err(e) => {
                error!("Error releasing file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        if FINE_LOGGING {
            trace!("FS fsync(ino: {}, file_handle: {}, datasync: {})", _ino, _fh, _datasync);
        }
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        if FINE_LOGGING {
            trace!("FS opendir(ino: {}, flags: {})", _ino, _flags);
        }
        reply.opened(0, 0);
    }

    fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if FINE_LOGGING {
            trace!("FS readdir(ino: {}, file_handle: {}, offset: {})", ino, fh, offset);
        }

        match self.fs.readdir(ino as i64, offset) {
            Ok(entries) => {
                let mut index = offset + 1;
                for e in entries {
                    let fuse_kind = if e.kind == FILE_KIND_DIRECTORY { FileType::Directory } else { FileType::RegularFile };
                    let ino = e.entry_file_id as u64;
                    if reply.add(ino, index, fuse_kind, e.name) {
                        break;
                    }
                    index += 1;
                }
                reply.ok();
            }
            Err(e) => {
                error!("Error reading directory: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: u32, reply: ReplyEmpty) {
        if FINE_LOGGING {
            trace!("FS releasedir(ino: {}, file_handle: {}, flags: {})", _ino, _fh, _flags);
        }
        reply.ok();
    }

    fn fsyncdir(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        if FINE_LOGGING {
            trace!("FS fsyncdir(ino: {}, file_handle: {}, datasync: {})", _ino, _fh, _datasync);
        }
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        trace!("FS statfs(ino: {})", _ino);
        let blocks = (1u64 << 40u64) / BLOCK_SIZE as u64;
        reply.statfs(
            blocks,
            blocks,
            blocks,
            9999999,
            9999999,
            BLOCK_SIZE,
            255,
            BLOCK_SIZE,
        );
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        trace!("FS access(ino: {}, mask: {})", _ino, _mask);
        warn!("Access not implemented");
        reply.error(ENOSYS);
    }

    fn create(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, flags: u32, reply: ReplyCreate) {
        trace!("FS create(parent: {}, name: {:?}, mode: {}, umask: {}, flags: {})", parent, name, mode, _umask, flags);

        let open_flags = OpenFlags::from(flags as i32);
        let flags = open_flags.to_safe_flags() as u32;

        let name = name.to_string_lossy();
        let file = match self.fs.lookup(parent as i64, &name) {
            Ok(Some(file)) => {
                file
            }
            Ok(None) => {
                let res = self.fs.mknod(parent as i64, &name, req.uid(), req.gid(), mode);

                match res {
                    Err(e) => {
                        error!("Error creating file: {:?}", e.error);
                        reply.error(e.code);
                        return;
                    }
                    Ok(file) => file
                }
            }
            Err(e) => {
                error!("Error looking up file: {:?}", e.error);
                reply.error(e.code);
                return;
            }
        };

        match self.fs.open(file.id, flags) {
            Ok(_) => {
                self.fh_counter += 1;
                let fh = self.fh_counter;
                self.open_files.insert(fh, file.id as u64);

                let attr = FileAttr::from(&file);
                reply.created(&self.get_ttl(), &attr, 0, fh, flags);
            }
            Err(e) => {
                error!("Error opening file: {:?}", e.error);
                reply.error(e.code);
            }
        }
    }

    fn getlk(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, _start: u64, _end: u64, _typ: u32, _pid: u32, reply: ReplyLock) {
        trace!("FS getlk(ino: {}, file_handle: {}, lock_owner: {}, start: {}, end: {}, typ: {}, pid: {})", _ino, _fh, _lock_owner, _start, _end, _typ, _pid);
        warn!("Getlk not implemented");
        reply.error(ENOSYS);
    }

    fn setlk(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, _start: u64, _end: u64, _typ: u32, _pid: u32, _sleep: bool, reply: ReplyEmpty) {
        trace!("FS setlk(ino: {}, file_handle: {}, lock_owner: {}, start: {}, end: {}, typ: {}, pid: {}, sleep: {})", _ino, _fh, _lock_owner, _start, _end, _typ, _pid, _sleep);
        warn!("Setlk not implemented");
        reply.error(ENOSYS);
    }

    fn bmap(&mut self, _req: &Request, _ino: u64, _blocksize: u32, _idx: u64, reply: ReplyBmap) {
        trace!("FS bmap(ino: {}, blocksize: {}, idx: {})", _ino, _blocksize, _idx);
        warn!("Bmap not implemented");
        reply.error(ENOSYS);
    }
}

impl From<&FileRow> for FileAttr {
    fn from(value: &FileRow) -> Self {
        FileAttr {
            ino: value.id as u64,
            size: value.size as u64,
            blocks: value.size as u64 / BLOCK_SIZE as u64,
            atime: system_time_from_timestamp(value.accessed_at),
            mtime: system_time_from_timestamp(value.updated_at),
            ctime: system_time_from_timestamp(value.updated_at),
            crtime: system_time_from_timestamp(value.created_at),
            kind: if value.kind == 1 { FileType::Directory } else { FileType::RegularFile },
            perm: value.perms as u16,
            nlink: if value.kind == 1 { 2 } else { 1 },
            uid: value.uid as u32,
            gid: value.gid as u32,
            rdev: 0,
            flags: 0,
        }
    }
}
