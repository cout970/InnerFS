# TODO

- [x] Store files in FS with content hash
- [x] Store files in sqlar
- [x] Store files in S3 compatible storage
- [x] Encryption of files
- [x] CLI Option to delete all data 
- [x] Export index to json/yaml
- [x] Export files to folder
- [x] Export files to tar
- [x] Export files to zip
- [x] Add option noempty by default
- [x] Warn if incompatible config changes are made
- [x] Add migrations and versioning
- [x] Add file change history table
- [x] Add file compression using gzip
- [x] Add multiple storage point for replication (primary replicas)
- Stats sub command
- Add local cache to speed up remote reads like catfs
- Test if is posible to run operation in an async context
- Implement methods from the newest FUSE ABI
- Add benchmarks showing the performance with different config parameters
- Verify integrity of files, check sha512 and size
- Import index from json/yaml, maybe?
- Sync between machines/instances
- Sync with folder, like rsync
- Encryption of index.db
- Automatic save to git every few minutes if changes are present
- Docker image
- Support RocksDB
- Support Redis
- Read only mode
- Export to sqlar, even if the files are stored in S3
- Export to .innerfs file, that is a sqlar file with the index and the files
- Mount .innerfs file with file explorer with double click, like any zip file

### Bugs
- Rename directory contents
- Support rename between directories
- Nuke not removing folders from S3
