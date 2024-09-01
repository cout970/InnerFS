use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use clap::{Parser, Subcommand, ValueEnum};

/// Utility to mount a shadow filesystem, supports encryption and multiple storage backends: S3, Sqlar and FileSystem
#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Turn debugging information on
    #[arg(short, long, default_value_t = false)]
    pub debug: bool,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Mount the filesystem
    Mount,
    /// Delete all data stored
    Nuke {
        /// Force the deletion without asking for confirmation
        #[arg(short, long, default_value_t = false)]
        force: bool,
    },
    /// Export the file metadata index to a file
    ExportIndex {
        /// Export format: json or yaml
        #[arg(short, long, value_enum, default_value_t = IndexExportFormat::Json)]
        format: IndexExportFormat,
    },
    /// Export the whole filesystem to a file
    ExportFiles {
        /// Export format: tar or zip
        #[arg(short, long, value_enum, default_value_t = FileExportFormat::Directory)]
        format: FileExportFormat,

        /// Export path
        #[arg(short, long, value_name = "FILE")]
        path: PathBuf,
    },
    /// Generate a default config file
    GenerateConfig,
    /// Print stats about the filesystem
    Stats,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum IndexExportFormat {
    Json,
    Yaml,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FileExportFormat {
    Directory,
    Tar,
    Zip,
}

impl Display for IndexExportFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexExportFormat::Json => write!(f, "json"),
            IndexExportFormat::Yaml => write!(f, "yaml"),
        }
    }
}

impl Display for FileExportFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileExportFormat::Directory => write!(f, "directory"),
            FileExportFormat::Tar => write!(f, "tar"),
            FileExportFormat::Zip => write!(f, "zip"),
        }
    }
}