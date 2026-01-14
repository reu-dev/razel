use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use url::Url;

#[derive(Clone, Subcommand, Serialize, Deserialize)]
pub enum Task {
    /// Write a value captured with a regex to a file
    CaptureRegex(CaptureRegexTask),
    /// Concatenate multiple csv files - headers must match
    CsvConcat(CsvConcatTask),
    /// Filter a csv file - keeping only the specified cols
    CsvFilter(CsvFilterTask),
    /// Write a text file
    WriteFile(WriteFileTask),
    /// Download a file
    DownloadFile(DownloadFileTask),
    /// Ensure that two files are equal
    EnsureEqual(EnsureEqualTask),
    /// Ensure that two files are not equal
    EnsureNotEqual(EnsureNotEqualTask),
    /// Post a HTTP multipart form for remote execution
    HttpRemoteExec(HttpRemoteExecTask),
    /// Instruct CMake to create file-based API. To be called before cmake.
    CmakeEnableApi(CmakeEnableApiTask),
    GitLfsPullCmakeDeps(GitLfsPullCmakeDepsTask),
    GitLfsPullCtestDeps(GitLfsPullCtestDepsTask),
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct CmakeEnableApiTask {
    /// Directory in which CMake will be executed
    pub cmake_binary_dir: PathBuf,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct GitLfsPullCmakeDepsTask {
    /// Directory in which CMake was executed
    pub cmake_binary_dir: PathBuf,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct GitLfsPullCtestDepsTask {
    /// Directory in which CTest will be executed. Typically directory in which CMake was executed.
    pub ctest_dir: PathBuf,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct CaptureRegexTask {
    /// Input file to read
    pub input: String,
    /// File to write the captured value to
    pub output: String,
    /// Regex containing a single capturing group
    pub regex: String,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct CsvConcatTask {
    /// Input csv files
    #[clap(required = true)]
    pub input: Vec<String>,
    /// Concatenated file to create
    pub output: String,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct CsvFilterTask {
    #[clap(short, long)]
    pub input: String,
    #[clap(short, long)]
    pub output: String,
    /// Col names to keep - all other cols are dropped
    #[clap(short, long = "col", num_args = 0..)]
    pub cols: Vec<String>,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct WriteFileTask {
    /// File to create
    pub file: String,
    /// Lines to write
    pub lines: Vec<String>,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct DownloadFileTask {
    #[clap(short, long)]
    pub url: String,
    #[clap(short, long)]
    pub output: String,
    #[clap(short, long)]
    pub executable: bool,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct EnsureEqualTask {
    pub file1: String,
    pub file2: String,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct EnsureNotEqualTask {
    pub file1: String,
    pub file2: String,
}

#[derive(Args, Clone, Serialize, Deserialize)]
pub struct HttpRemoteExecTask {
    /// url for HTTP multipart form POST
    #[clap(short, long)]
    pub url: Url,
    /// files to attach to the form
    #[clap(short, long)]
    pub files: Vec<String>,
    /// file names to use in the form
    #[clap(short = 'n', long)]
    pub file_names: Vec<String>,
}
