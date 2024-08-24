# InnerFS

InnerFS is a command line utility to mount with FUSE a virtual file system that uses a sqlite database to store metadata
and a storage backend to store the file contents.

Several use cases can benefit from InnerFS:

- Store files in S3 with encryption and deduplication
- Keep all files in a single file, while still being able to access them as if they were in a directory
- Store files in a SQL database that allows querying with SQL to generate statistics or performing complex searches
- Reduce storage space by deduplicating and compressing files
- Provide ease access to files while keeping them encrypted, allowing to safely sync the encrypted files to the cloud

### Backend

Filesystem: Is the most basic backend, stores files in the specific path.

Sqlar: Stores files in a SQLite database, see [sqlar](https://sqlite.org/sqlar.html) for more information.

S3: Stores files in an S3 compatible storage.

### Features

- File de-duplication based on content
- File encryption with AES-256-GCM
- Metadata sqlite database that can be queried with SQL
- File name mangling with the content SHA512 hash

### Usage

- Generate a configuration file

```bash
innerfs generate-config
```

Will generate a configuration file `config.yml` in the current directory.

- Mount the filesystem

```bash
innerfs mount
```

Will mount the filesystem in the path specified in the configuration file. To unmount the filesystem use `umount` with
the mount point.

- Nuke all data

```bash
innerfs nuke
```

Will remove all data from the filesystem, including the metadata database. Handle with care.

- Export index as JSON/YAML

```bash
innerfs export-index --format json
```

Will generate a JSON file with a hierarchical representation of the filesystem (without the file contents).

- Export files

```bash
innerfs export-files --path ./output --format zip
```

Will export all files in the filesystem to the specified path in the specified format. Supports `zip`, `tar`
and `directory`.

### Configuration

The default configuration file contains comments that explain the options, can be seen [here](./src/default_config.yml).

### Planned features

- [ ] File compression
- [ ] File verification
- [ ] Sync between instances
- [ ] Encryption of the metadata database
