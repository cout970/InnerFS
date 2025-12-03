use crate::config::{CompressionAlgorithm, StorageConfig};
use crate::obj_storage::BlobProcessor;
use crate::AnyError;
use anyhow::anyhow;
use flate2::Compression;
use std::io::{Read, Write};
use std::sync::Arc;
use zstd::zstd_safe::{max_c_level, min_c_level, CompressionLevel};

pub struct CompressedObjectStorage {
    pub config: Arc<StorageConfig>,
}

impl CompressedObjectStorage {
    pub fn new(config: Arc<StorageConfig>) -> CompressedObjectStorage {
        CompressedObjectStorage { config }
    }
}

impl BlobProcessor for CompressedObjectStorage {
    fn on_store(&self, blob: Vec<u8>) -> Result<Vec<u8>, AnyError> {
        if blob.len() < 1024 {
            return Ok(blob);
        }

        let mut buff = vec![];

        match &self.config.compression_algorithm {
            CompressionAlgorithm::Gzip => {
                let level = self.config.compression_level.clamp(0, 9) as u32;
                buff.extend(format!("|gzip:{}:{}|", level, blob.len()).as_bytes());
                {
                    let mut gz = flate2::write::GzEncoder::new(&mut buff, Compression::new(level));
                    gz.write_all(&blob)?;
                    gz.finish()?;
                }
            }
            CompressionAlgorithm::ZStd => {
                let level = (self.config.compression_level as CompressionLevel).clamp(min_c_level(), max_c_level());
                buff.extend(format!("|zstd:{}:{}|", level, blob.len()).as_bytes());
                let header_len = buff.len();

                let required_size = zstd::zstd_safe::compress_bound(blob.len());
                buff.extend(vec![0u8; required_size]);
                let size = zstd::zstd_safe::compress(&mut buff[header_len..], &blob, level)
                    .map_err(|e| anyhow!("Failed to compress blob with zstd, error code: {}", e))?;

                buff.truncate(header_len + size);
            }
            CompressionAlgorithm::Lzo => {
                buff.extend(format!("|lzo:{}:{}|", 0, blob.len()).as_bytes());
                let compressed =
                    minilzo::compress(&blob).map_err(|e| anyhow!("Failed to compress blob with lzo, error: {}", e))?;
                buff.extend_from_slice(&compressed);
            }
        };

        // Only store the compressed version if it's smaller than the original
        if buff.len() < blob.len() {
            Ok(buff)
        } else {
            Ok(blob)
        }
    }

    fn on_load(&self, blob: Vec<u8>) -> Result<Vec<u8>, AnyError> {
        if !blob.starts_with(b"|") {
            return Ok(blob);
        }

        // If the there is not even enough data for a header, return the original
        if blob.len() <= "|gzip:0:0|".len().min("|zstd:0:0|".len()).min("|lzo:0:0|".len()) {
            return Ok(blob);
        }

        // Find matching |
        let Some(header_end) = blob.iter().skip(1).position(|&b| b == b'|') else {
            return Ok(blob);
        };
        let header_end = header_end + 1; // Adjust for the skip

        // Skip | and split by :
        let header = blob[1..header_end]
            .split(|c| *c == b':')
            .map(|c| String::from_utf8_lossy(c).to_string())
            .collect::<Vec<String>>();

        if header.len() != 3 || (header[0] != "gzip" && header[0] != "zstd" && header[0] != "lzo") {
            return Ok(blob);
        }

        let Ok(_compression_level) = header[1].parse::<u32>() else {
            return Ok(blob);
        };
        let Ok(original_len) = header[2].parse::<usize>() else {
            return Ok(blob);
        };

        let mut buff = vec![];
        let content = &blob[(header_end + 1)..];

        match header[0].as_str() {
            "gzip" => {
                let mut gz = flate2::read::GzDecoder::new(content);
                gz.read_to_end(&mut buff)?;
            }
            "zstd" => {
                buff = zstd::decode_all(content)
                    .map_err(|e| anyhow!("Failed to decompress blob with zstd, error: {}", e))?;
            }
            "lzo" => {
                buff = minilzo::decompress(content, original_len)
                    .map_err(|e| anyhow!("Failed to decompress blob with lzo, error: {}", e))?;
            }
            _ => unreachable!(),
        }

        Ok(buff)
    }
}
