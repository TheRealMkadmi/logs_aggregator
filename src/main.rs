use aho_corasick::AhoCorasick;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memmap2::Mmap;
use num_cpus;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::FromRawHandle;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Sender};
use tokio::task;
use walkdir::WalkDir;
use windows::core::PCWSTR;
use windows::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
use windows::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_SEQUENTIAL_SCAN, FILE_GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING,
};

#[derive(Parser, Debug)]
#[command(
    name = "Blacklist Filter",
    version = "1.0",
    author = "TheRealMkadmi",
    about = "Recursively concatenates files in a directory excluding lines with blacklisted tokens."
)]
struct Args {
    #[arg(short, long, value_parser)]
    directory: PathBuf,

    #[arg(short, long, value_parser)]
    blacklist: PathBuf,

    #[arg(short, long, value_parser, default_value = "output.txt")]
    output: PathBuf,

    #[arg(short = 'b', long, default_value_t = 1000)]
    buffer_size: usize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> io::Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    // Load blacklist tokens and build Aho-Corasick automaton for case-insensitive matching
    let blacklist = load_blacklist(&args.blacklist)?;
    let aho = AhoCorasick::builder()
        .ascii_case_insensitive(true)
        .build(&blacklist)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let aho = Arc::new(aho);

    // Collect all file paths recursively in the specified directory
    let files = collect_files_recursive(&args.directory)?;
    let total_files = files.len();

    // Setup progress bars
    let m = MultiProgress::new();
    let pb = m.add(ProgressBar::new(total_files as u64));
    let style = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files processed ({eta} remaining)")
        .unwrap()
        .progress_chars("#>-");
    pb.set_style(style.clone());

    // Statistics
    let lines_processed = Arc::new(Mutex::new(0u64));
    let lines_filtered = Arc::new(Mutex::new(0u64));

    // Create an asynchronous channel for sending filtered lines to writer tasks
    let (tx, mut rx) = mpsc::channel::<Vec<String>>(args.buffer_size);

    // Clone pb for writer task
    let pb_writer = pb.clone();

    // Spawn writer tasks
    let writer_handle = {
        let output_path = args.output.clone();
        let m = m.clone();
        task::spawn(async move {
            let output_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&output_path)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let mut writer = io::BufWriter::new(output_file);

            while let Some(lines) = rx.recv().await {
                for line in lines {
                    writeln!(writer, "{}", line)?;
                }
            }

            writer.flush()?;
            m.remove(&pb_writer);
            Ok::<(), io::Error>(())
        })
    };

    let processing_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()
        .unwrap();

    let aho_clone = aho.clone();
    let tx_clone = tx.clone();
    let lines_processed_clone = lines_processed.clone();
    let lines_filtered_clone = lines_filtered.clone();

    processing_pool.scope(|s| {
        for file_path in files {
            let aho = aho_clone.clone();
            let tx = tx_clone.clone();
            let lines_processed = lines_processed_clone.clone();
            let lines_filtered = lines_filtered_clone.clone();
            let pb = pb.clone();

            s.spawn(move |_| {
                if let Err(e) =
                    process_file(&file_path, &aho, &tx, &lines_processed, &lines_filtered)
                {
                    eprintln!("Error processing file {:?}: {}", file_path, e);
                }
                pb.inc(1);
            });
        }
    });

    drop(tx);

    writer_handle.await??;

    pb.finish_with_message("Processing complete.");

    let total_lines = *lines_processed.lock();
    let total_filtered = *lines_filtered.lock();
    println!("Total lines processed: {}", total_lines);
    println!("Total lines filtered out: {}", total_filtered);
    println!(
        "Total lines written to output: {}",
        total_lines.saturating_sub(total_filtered)
    );

    Ok(())
}

fn load_blacklist(blacklist_path: &Path) -> io::Result<Vec<String>> {
    let file = File::open(blacklist_path)?;
    let reader = BufReader::new(file);
    let tokens = reader
        .lines()
        .filter_map(Result::ok)
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect();
    Ok(tokens)
}

fn collect_files_recursive(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(dir)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        files.push(entry.into_path());
    }
    Ok(files)
}

fn process_file(
    file_path: &Path,
    aho: &AhoCorasick,
    sender: &Sender<Vec<String>>,
    lines_processed: &Arc<Mutex<u64>>,
    lines_filtered: &Arc<Mutex<u64>>,
) -> io::Result<()> {
    let wide_path: Vec<u16> = file_path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide_path.as_ptr()),
            FILE_GENERIC_READ.0,
            FILE_SHARE_READ,
            None,
            OPEN_EXISTING,
            FILE_FLAG_SEQUENTIAL_SCAN,
            HANDLE::default(),
        )
    };

    if handle == Ok(INVALID_HANDLE_VALUE) {
        return Err(io::Error::last_os_error());
    }

    let file = unsafe { File::from_raw_handle(handle.unwrap().0 as _) };
    let mmap = unsafe { Mmap::map(&file)? };
    let content = String::from_utf8_lossy(&mmap);

    let mut filtered_lines = Vec::new();

    for line in content.lines() {
        {
            let mut lp = lines_processed.lock();
            *lp += 1;
        }

        if aho.is_match(line) {
            let mut lf = lines_filtered.lock();
            *lf += 1;
            continue;
        }

        filtered_lines.push(line.to_string());

        if filtered_lines.len() >= 1000 {
            let batch = std::mem::take(&mut filtered_lines);
            let sender_clone = sender.clone();
            task::spawn(async move {
                if let Err(e) = sender_clone.send(batch).await {
                    eprintln!("Error sending batch to writer: {}", e);
                }
            });
        }
    }

    if !filtered_lines.is_empty() {
        let batch = std::mem::take(&mut filtered_lines);
        let sender_clone = sender.clone();
        task::spawn(async move {
            if let Err(e) = sender_clone.send(batch).await {
                eprintln!("Error sending final batch to writer: {}", e);
            }
        });
    }

    Ok(())
}