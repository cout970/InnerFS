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
- Add file compression using GZ
- Stats sub command
- Add local cache to speed up remote reads like catfs
- Add multiple storage point for replication (primary replicas)
- Test if is posible to run operation in an async context
- Implement methods from the newest FUSE ABI
- Add benchmarks showing the performance with different config parameters
- Verify integrity of files, check sha512 and size
- Import index from json/yaml
- Sync between machines/instances
- Sync with folder, like rsync
- Encryption of index.db
- Automatic save to git every few minutes if changes are present
- Docker image

### Bugs
- Rename directory contents
- Support rename between directories