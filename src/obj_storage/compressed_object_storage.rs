use crate::obj_storage::{ObjInfo, ObjectStorage};
use crate::storage_interface::ObjInUseFn;
use crate::AnyError;
use flate2::Compression;
use std::io::{Read, Write};
use std::sync::Arc;
use crate::config::StorageConfig;

pub struct CompressedObjectStorage {
    pub config: Arc<StorageConfig>,
    pub proxy: Box<dyn ObjectStorage>,
}

impl CompressedObjectStorage {
    pub fn new(config: Arc<StorageConfig>, proxy: Box<dyn ObjectStorage>) -> CompressedObjectStorage {
        CompressedObjectStorage { config, proxy }
    }

    pub fn should_compress(&self, info: &ObjInfo, content: &[u8]) -> bool {
        // Only compress if the content is larger than 1KB
        if content.len() < 1024 {
            return false;
        }

        // Don't compress if the content is too large, it will be too slow
        // 100MB is a reasonable limit
        if content.len() > 100 * 1024 * 1024 {
            return false;
        }

        // Exclude file extensions that are already compressed or are not worth compressing
        let extensions = vec![
            // Already compressed
            "gz",
            "gzip",
            "tgz",
            "zip",
            "bz2",
            "xz",
            "lzma",
            "lz",
            "lz4",
            "zst",
            "rar",
            "7z",
            "tar",
            "cab",
            "arj",
            "udf",
            "txz",
            // Image
            "jpg",
            "jpeg",
            "png",
            "gif",
            "webp",
            "heic",
            "heif",
            "bmp",
            "tiff",
            "ico",
            "svg",
            "psd",
            "ai",
            "eps",
            "indd",
            "raw",
            "cr2",
            "nef",
            "orf",
            "cdr",
            "sketch",
            // Raw image
            "arw",
            "dng",
            "pef",
            "rw2",
            // Video
            "mp4",
            "mkv",
            "avi",
            "mov",
            "webm",
            "wmv",
            "flv",
            "swf",
            // Animation
            "mng",
            "apng",
            "gifv",
            // Audio
            "mp3",
            "flac",
            "wav",
            "ogg",
            "m4a",
            "wma",
            "aac",
            "alac",
            "ape",
            "aiff",
            // Document
            "pdf",
            "epub",
            "pptx",
            "docx",
            "xlsx",
            "odt",
            "xlsb",
            "numbers",
            "pages",
            "pages",
            "key",
            "ps",
            "ai",
            "pem",
            // Fonts
            "ttf",
            "otf",
            "woff",
            "woff2",
            "pfa",
            "pfb",
            // Binaries
            "exe",
            "dll",
            "so",
            "a",
            "o",
            "obj",
            "lib",
            "bin",
            "appimage",
            "dylib",
            "elf",
            // Binary storage
            "pak",
            "dat",
            "vpk",
            "xo3",
            "cpk",
            // 3D models
            "obj",
            "fbx",
            "blend",
            "3ds",
            "stl",
            "ply",
            "dxf",
            "lwo",
            "lws",
            "glb",
            "gltf",
            "usd",
            "usda",
            "prt",
            "catpart",
            "step",
            "stp",
            "iges",
            "igs",
            // Virtual Machines
            "iso",
            "img",
            "vdi",
            "vmdk",
            "ova",
            "ovf",
            "qcow2",
            "vhd",
            // Java
            "jar",
            "war",
            "ear",
            // Databases
            "db",
            "sqlite",
            "dbf",
            "mdb",
            "db-shm",
            "db-wal",
            "db-journal",
            "db-lock",
            "rdb",
            "frm",
            "ibd",
            "myd",
            "ndf",
            "mdf",
            "ldf",
            // Other
            "mobi",
            "azw",
            "azw3",
            "apk",
            "parquet",
            "orc",
            "enc",
            // Git
            "idx",
            "pack",
        ];

        for ext in extensions {
            // Ends in `.<ext>`
            if info.name.as_bytes().len() > ext.len() + 1
                && info.name.as_bytes()[info.name.as_bytes().len() - ext.len() - 1] == b'.'
                && info.name.ends_with(ext)
            {
                return false;
            }
        }

        true
    }

    pub fn add_to_metadata(compression: &str, name: &str, info: &str) -> String {
        let key = format!("[{}]{}", name, info);

        if compression.is_empty() {
            return key;
        }

        let parts: Vec<String> = compression.split(',').map(|i| i.to_string()).collect();
        let mut keys = vec![key];

        for part in parts {
            if part.starts_with("[") {
                // Tagged key
                let key_start = part.find("]").expect("Invalid settings");
                let key_name = &part[1..key_start];

                if key_name != name {
                    keys.push(part);
                }
            } else {
                // Untagged key
                keys.push(part);
            }
        }

        keys.join(",")
    }
}

impl ObjectStorage for CompressedObjectStorage {
    fn get(&mut self, info: &ObjInfo) -> Result<Vec<u8>, AnyError> {
        let bytes = self.proxy.get(info)?;

        // No compression was used for this object
        if info.compression.is_empty() {
            return Ok(bytes);
        }

        let mut buff = vec![];
        {
            let mut gz = flate2::read::GzDecoder::new(&bytes[..]);
            gz.read_to_end(&mut buff)?;
        }

        Ok(buff)
    }

    fn put(&mut self, info: &mut ObjInfo, content: &[u8]) -> Result<(), AnyError> {
        // Don't compress if it's not worth it
        if !self.should_compress(info, content) {
            info.compression = "".to_string();
            self.proxy.put(info, content)?;
            return Ok(());
        }

        let mut buff = vec![];
        {
            let mut gz = flate2::write::GzEncoder::new(&mut buff, Compression::new(self.config.compression_level));
            gz.write_all(content)?;
            gz.finish()?;
        }

        // Only store the compressed version if it's smaller than the original
        if buff.len() < content.len() {
            info.compression = Self::add_to_metadata(&info.compression, &self.config.name, &format!("gzip:{}:{}", self.config.compression_level, buff.len()));
            self.proxy.put(info, buff.as_slice())?;
        } else {
            info.compression = "".to_string();
            self.proxy.put(info, content)?;
        }
        Ok(())
    }

    fn remove(&mut self, info: &ObjInfo, is_in_use: ObjInUseFn) -> Result<(), AnyError> {
        self.proxy.remove(info, is_in_use)?;
        Ok(())
    }

    fn rename(&mut self, prev_info: &ObjInfo, new_info: &ObjInfo) -> Result<(), AnyError> {
        self.proxy.rename(prev_info, new_info)?;
        Ok(())
    }

    fn nuke(&mut self) -> Result<(), AnyError> {
        self.proxy.nuke()?;
        Ok(())
    }

    fn clone(&self) -> Box<dyn ObjectStorage> {
        Box::new(Self {
            proxy: self.proxy.clone(),
            config: self.config.clone(),
        })
    }
}
