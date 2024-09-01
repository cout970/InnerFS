use crate::config::{check_config_changes, read_config};
use crate::fuse_fs::FuseFileSystem;
use crate::metadata_db::{MetadataDB, NO_BINDINGS};
use crate::obj_storage::{create_object_storage, ObjectStorage};
use anyhow::{Context};
use env_logger::Env;
use fs::File;
use log::{error, info, warn};
use std::ffi::OsStr;
use std::io::Write;
use std::path::{PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::{env, fs, thread};

mod config;
mod metadata_db;
mod fuse_fs;
mod sql_fs;
mod storage;
mod obj_storage;
mod storage_interface;
mod fs_tree;
mod utils;
mod cli;

use crate::cli::{Cli, Commands, FileExportFormat, IndexExportFormat};
use crate::fs_tree::{FsTree, FsTreeKind};
use crate::obj_storage::replicated_object_storage::ReplicatedObjectStorage;
use crate::sql_fs::SqlFileSystem;
use crate::storage_interface::StorageInterface;
use crate::utils::humanize_bytes_binary;
use clap::{Parser};
use flate2::{write::GzEncoder, Compression};
use serde_json::json;
use signal_hook::{consts::SIGINT, iterator::Signals};
use utils::ask_for_confirmation;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub type AnyError = anyhow::Error;

fn main() {
    // Parse command line arguments
    let cli = Cli::parse();

    // Init logger
    env_logger::Builder::from_env(Env::default())
        .filter(Some("fuse"), log::LevelFilter::Info)
        .filter(Some("InnerFS::proxy_fs"), if cli.debug { log::LevelFilter::Trace } else { log::LevelFilter::Info })
        .filter(Some("InnerFS"), if cli.debug { log::LevelFilter::Trace } else { log::LevelFilter::Info })
        .init();

    // Config path is required, if not provided, use the default one
    let config_path = cli.config.or_else(|| Some(PathBuf::from("./config.yml"))).unwrap();

    // The generate-config command is a special case, it doesn't need the config file
    // and must be handled before trying to read the config file
    if let Some(Commands::GenerateConfig) = &cli.command {
        if fs::metadata(&config_path).is_ok() {
            warn!("Config file already exists at {:?}", &config_path);
            if !ask_for_confirmation("Type 'yes' or 'y' to override this file") {
                info!("Operation cancelled");
                return;
            }
        }

        let data = include_str!("./default_config.yml");
        fs::write(&config_path, data).expect("Unable to write config file");
        info!("Config file generated at {:?}", config_path);
        return;
    }

    info!("Starting v{}", VERSION);

    let config = read_config(&config_path).expect("Unable to read config");
    info!("Config loaded");

    let sql = Rc::new(MetadataDB::open(&config.database_file));
    sql.run_migrations().expect("Unable to run migrations");

    // Check if the nuke command is being executed
    let is_nuke = match cli.command {
        Some(Commands::Nuke { .. }) => true,
        _ => false,
    };

    // Check if the config file has changed in incompatible ways (except for the nuke command)
    if !is_nuke {
        check_config_changes("primary", config.primary.clone(), sql.clone()).unwrap();
    }

    // Select the appropriate storage backend
    let mut obj_storage: Box<dyn ObjectStorage> = create_object_storage(config.primary.clone(), sql.clone());

    // Add replicas
    if !config.replicas.is_empty() {
        let mut rep = ReplicatedObjectStorage {
            primary: obj_storage,
            replicas: vec![],
        };

        for (index, replica) in config.replicas.iter().enumerate() {
            if !is_nuke {
                check_config_changes(&format!("replica_{}", index), replica.clone(), sql.clone()).unwrap();
            }

            rep.replicas.push(
                create_object_storage(replica.clone(), sql.clone())
            );
        }

        obj_storage = Box::new(rep);
    }

    // Wrap the storage backend in a StorageInterface, which provides a higher-level API
    let storage = Box::new(StorageInterface::new(obj_storage));
    let fs = SqlFileSystem::new(sql, config.clone(), storage);

    let cmd = cli.command.unwrap_or_else(|| Commands::Mount);

    match cmd {
        Commands::Mount => mount(fs).unwrap(),
        Commands::Nuke { force } => nuke(fs, force).unwrap(),
        Commands::ExportIndex { format } => export_index(fs, format).unwrap(),
        Commands::ExportFiles { format, path } => export_files(fs, format, path).unwrap(),
        Commands::GenerateConfig => unreachable!(),
        Commands::Stats => stats(fs).unwrap(),
    }
}

/// Mount the filesystem
fn mount(fs: SqlFileSystem) -> Result<(), AnyError> {
    let mount_point = fs.config.mount_point.clone();

    // Create a FUSE proxy filesystem to access the StorageInterface
    let proxy = FuseFileSystem::new(fs);

    // Try to unmount the filesystem, it may be already mounted form a previous run
    // This must be performed before trying to check if the file exists
    info!("Attempting to unmount {} before trying to mount", &mount_point);
    let _ = Command::new("umount").arg(&mount_point).status();

    // Check if the mount point exists and is a directory
    let stat = fs::metadata(&mount_point).expect("Mount point does not exist");
    if !stat.is_dir() {
        panic!("Mount point is not a directory");
    }

    let mut signals = Signals::new([SIGINT])?;

    let mount_point_copy = mount_point.clone();
    thread::spawn(move || {
        // Wait for a SIGINT signal to unmount the filesystem
        for _ in signals.forever() {
            info!("Received SIGINT, trying to unmount the filesystem");
            let _ = Command::new("umount").arg(&mount_point_copy).status();

            // Finish this thread
            break;
        }
    });

    info!("Mounting filesystem at {}", &mount_point);
    match fuse::mount(proxy, &mount_point, &[OsStr::new("noempty"), OsStr::new("default_permissions")]) {
        Ok(_) => {}
        Err(e) => {
            error!("Unable to mount filesystem: {}", e);
            error!("Maybe is was mounted before?, try `umount {}`", &mount_point);
            error!("If it says `target is busy`, close the programs that are using the mount point");
            error!("Existing");
            std::process::exit(-1);
        }
    }

    info!("Folder was unmounted successfully, exiting");
    Ok(())
}

/// Delete all data stored
fn nuke(mut fs: SqlFileSystem, force: bool) -> Result<(), AnyError> {
    if !force {
        warn!("Are you sure you want to delete all data?");
        if !ask_for_confirmation("This operation is irreversible. Type 'yes' or 'y' to proceed") {
            info!("Operation cancelled");
            return Ok(());
        }
    }

    info!("Deleting all data");
    fs.sql.nuke()?;
    fs.storage.nuke()?;
    let _ = fs::remove_file(PathBuf::from(&fs.config.database_file).with_extension("db-wal"));
    let _ = fs::remove_file(PathBuf::from(&fs.config.database_file).with_extension("db-shm"));
    let _ = fs::remove_file(PathBuf::from(&fs.config.database_file));
    info!("Done");
    Ok(())
}

/// Export the file metadata index to a file
fn export_index(fs: SqlFileSystem, format: IndexExportFormat) -> Result<(), AnyError> {
    info!("Exporting index");
    let tree = fs.sql.get_tree()?;

    let data = match format {
        IndexExportFormat::Json => serde_json::to_string_pretty(&tree)?,
        IndexExportFormat::Yaml => serde_yml::to_string(&tree)?,
    };

    let path = format!("./index.{}", format);
    fs::write(&path, data).context("Unable to write index file")?;
    info!("Index exported to {}", &path);
    Ok(())
}

/// Export the whole filesystem to a file
fn export_files(mut fs: SqlFileSystem, format: FileExportFormat, mut path: PathBuf) -> Result<(), AnyError> {
    info!("Exporting files to {:?}", &path);
    let tree = fs.sql.get_tree()?;

    match format {
        FileExportFormat::Directory => {
            fs::create_dir_all(&path)?;

            FsTree::for_each(tree, |child, child_path| {
                let child_path = path.join(child_path);

                if child.kind == FsTreeKind::Directory {
                    fs::create_dir_all(&child_path)?;
                } else {
                    let data = fs.read_all(child.id)?;
                    fs::write(&child_path, data).context("Unable to write file")?;
                }

                Ok(())
            })?;
        }
        FileExportFormat::Tar => {
            if !path.ends_with(".tar.gz") {
                path = path.with_extension("tar.gz");
            }

            let file = File::create(&path)?;
            let mut gz = GzEncoder::new(file, Compression::default());
            let mut tar = tar::Builder::new(&mut gz);

            FsTree::for_each(tree, |child, child_path| {
                let mut header = tar::Header::new_gnu();
                header.set_size(child.size as u64);
                header.set_mtime(child.updated_at as u64);
                header.set_mode(child.perms as u32);
                header.set_uid(child.uid as u64);
                header.set_gid(child.gid as u64);
                header.set_entry_type(if child.kind == FsTreeKind::Directory { tar::EntryType::Directory } else { tar::EntryType::Regular });
                header.set_cksum();

                if child.kind == FsTreeKind::Directory {
                    tar.append_data(&mut header, &child_path, &mut std::io::empty())?;
                } else {
                    let data = fs.read_all(child.id)?;
                    tar.append_data(&mut header, &child_path, data.as_slice())?;
                }
                Ok(())
            })?;

            tar.finish()?;
        }
        FileExportFormat::Zip => {
            if !path.ends_with(".zip") {
                path = path.with_extension("zip");
            }
            let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
            let mut zip = ZipWriter::new(File::create(&path)?);

            FsTree::for_each(tree, |child, child_path| {
                if child.kind == FsTreeKind::Directory {
                    zip.add_directory_from_path(&child_path, options)?;
                } else {
                    let data = fs.read_all(child.id)?;
                    zip.start_file_from_path(child_path, options)?;
                    zip.write_all(&data)?;
                }
                Ok(())
            })?;

            zip.finish()?;
        }
    };

    info!("Files exported successfully");
    Ok(())
}

/// Print stats about the filesystem
fn stats(fs: SqlFileSystem) -> Result<(), AnyError> {
    let [total, directories, regular] = fs.sql.get_row(
        "
        SELECT count(*)                      AS total,
               count(iif(kind = 0, 1, NULL)) AS directories,
               count(iif(kind = 1, 1, NULL)) AS regular
        FROM files",
        NO_BINDINGS.as_ref(),
        |row| {
            Ok([
                row.read::<i64, _>("total")?,
                row.read::<i64, _>("directories")?,
                row.read::<i64, _>("regular")?,
            ])
        },
    )?.unwrap();

    let top_largest_files = fs.sql.get_rows(
        "
        SELECT name, size
        FROM files
        ORDER BY size DESC
        LIMIT 5",
        NO_BINDINGS.as_ref(),
        |row| {
            Ok(json!({
                "name": row.read::<String, _>("name")?,
                "size": row.read::<i64, _>("size")?,
            }))
        },
    )?;

    let top_used_extensions = fs.sql.get_rows(
        "
        SELECT replace(name, rtrim(name, replace(name, '.', '')), '') AS extension,
               count(*)                                               AS count
        FROM files
        WHERE name LIKE '%.%'
          AND name NOT LIKE '.%'
          AND kind = 0
        GROUP BY extension
        ORDER BY count DESC
        LIMIT 10;",
        NO_BINDINGS.as_ref(),
        |row| {
            Ok(json!({
                "extension": row.read::<String, _>("extension")?,
                "count": row.read::<i64, _>("count")?,
            }))
        },
    )?;

    let [sqlar_total, sqlar_size, sqlar_size_real] = fs.sql.get_row(
        "
        SELECT count(*)          AS total,
               sum(sz)           AS size,
               sum(length(data)) AS size_real
        FROM sqlar",
        NO_BINDINGS.as_ref(),
        |row| {
            Ok([
                row.read::<i64, _>("total")?,
                row.read::<i64, _>("size")?,
                row.read::<i64, _>("size_real")?,
            ])
        },
    )?.unwrap();

    let stats = json!({
        "files": {
            "total": total,
            "directories": directories,
            "regular": regular,
        },
        "summary": {
            "top_largest_files": top_largest_files,
            "top_used_extensions": top_used_extensions,
        },
        "sqlar": {
            "total": sqlar_total,
            "original_size": humanize_bytes_binary(sqlar_size  as usize),
            "original_size_bytes": sqlar_size,
            "computed_size": humanize_bytes_binary(sqlar_size_real as usize),
            "computed_size_bytes": sqlar_size_real,
        }
    });

    println!("{}", serde_json::to_string_pretty(&stats)?);
    Ok(())
}
