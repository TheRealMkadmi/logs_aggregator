use aho_corasick::AhoCorasick;
use clap::Parser;
use crossbeam_channel::{unbounded, Sender};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memmap2::Mmap;
use num_cpus;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::NamedTempFile;
use walkdir::WalkDir;

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

    #[arg(short = 's', long, default_value_t = 1000)]
    buffer_size: usize,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let blacklist = load_blacklist(&args.blacklist)?;
    let aho = AhoCorasick::builder()
        .ascii_case_insensitive(true)
        .build(&blacklist)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let aho = Arc::new(aho);

    let files = collect_files_recursive(&args.directory)?;
    let total_files = files.len();

    let m = MultiProgress::new();
    let pb = m.add(ProgressBar::new(total_files as u64));
    let style = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files processed ({eta} remaining)")
        .unwrap()
        .progress_chars("#>-");
    pb.set_style(style.clone());

    let lines_processed = Arc::new(AtomicU64::new(0));
    let lines_filtered = Arc::new(AtomicU64::new(0));

    let (tx, rx) = unbounded::<Vec<String>>();

    let pb_writer = pb.clone();
    let m_writer = m.clone();
    let buffer_size = args.buffer_size;
    let output_path = args.output.clone();
    let writer_handle = {
        let m = m_writer;
        let pb_writer = pb_writer;
        let handle = std::thread::spawn(move || -> io::Result<()> {
            let mut temp_output = NamedTempFile::new()?;
            {
                let mut writer = BufWriter::new(temp_output.as_file_mut());

                for lines in rx {
                    for line in lines {
                        writeln!(writer, "{}", line)?;
                    }
                }

                writer.flush()?;
            }

            m.remove(&pb_writer);

            if output_path.exists() {
                fs::remove_file(&output_path)?;
            }

            temp_output.persist(&output_path)?;
            Ok(())
        });
        handle
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
            let buffer_size = buffer_size;

            s.spawn(move |_| {
                if let Err(e) = process_file(
                    &file_path,
                    &aho,
                    &tx,
                    &lines_processed,
                    &lines_filtered,
                    buffer_size,
                ) {
                    eprintln!("Error processing file {:?}: {}", file_path, e);
                }
                pb.inc(1);
            });
        }
    });

    drop(tx);

    writer_handle.join().expect("Writer thread panicked")?;

    pb.finish_with_message("Processing complete.");

    let total_lines = lines_processed.load(Ordering::Relaxed);
    let total_filtered = lines_filtered.load(Ordering::Relaxed);
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
    lines_processed: &Arc<AtomicU64>,
    lines_filtered: &Arc<AtomicU64>,
    buffer_size: usize,
) -> io::Result<()> {
    use std::ffi::c_void;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;
    use windows::core::PCWSTR;
    use windows::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows::Win32::Storage::FileSystem::{
        CreateFileW, FILE_FLAG_SEQUENTIAL_SCAN, FILE_GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING,
    };

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
            None,
        )
    };

    if handle == Ok(INVALID_HANDLE_VALUE) {
        return Err(io::Error::last_os_error());
    }

    let file = unsafe { File::from_raw_handle(handle.unwrap().0 as *mut c_void) };
    let mmap = unsafe { Mmap::map(&file)? };
    let content = String::from_utf8_lossy(&mmap);

    let mut filtered_lines = Vec::with_capacity(buffer_size);

    for line in content.lines() {
        lines_processed.fetch_add(1, Ordering::Relaxed);

        if aho.is_match(line) {
            lines_filtered.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        filtered_lines.push(line.to_string());

        if filtered_lines.len() >= buffer_size {
            let batch = std::mem::take(&mut filtered_lines);
            if sender.send(batch).is_err() {
                eprintln!("Error sending batch to writer");
            }
        }
    }

    if !filtered_lines.is_empty() {
        let batch = std::mem::take(&mut filtered_lines);
        if sender.send(batch).is_err() {
            eprintln!("Error sending final batch to writer");
        }
    }

    Ok(())
}
