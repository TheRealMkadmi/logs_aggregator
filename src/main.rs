use aho_corasick::AhoCorasick;
use clap::Parser;
use crossbeam_channel::{bounded, Sender};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use num_cpus;
use rayon::prelude::*;
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

    #[arg(short = 's', long, default_value_t = 10_000)]
    buffer_size: usize,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let blacklist = match load_blacklist(&args.blacklist) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Failed to load blacklist: {}", e);
            return Err(e);
        }
    };

    let aho = match AhoCorasick::builder()
        .ascii_case_insensitive(true)
        .build(&blacklist)
    {
        Ok(a) => Arc::new(a),
        Err(e) => {
            eprintln!("Failed to build Aho-Corasick automaton: {}", e);
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }
    };

    let files = match collect_files_recursive(&args.directory) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to collect files: {}", e);
            return Err(e);
        }
    };
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

    let (tx, rx) = bounded::<Vec<String>>(100);

    let pb_writer = pb.clone();
    let m_writer = m.clone();
    let buffer_size = args.buffer_size;
    let output_path = args.output.clone();
    let writer_handle = {
        let m = m_writer;
        let pb_writer = pb_writer;
        let output_path = output_path.clone();
        std::thread::spawn(move || -> io::Result<()> {
            let mut temp_output = NamedTempFile::new().map_err(|e| {
                eprintln!("Failed to create temporary file: {}", e);
                e
            })?;
            let mut writer = BufWriter::with_capacity(16 * 1024, temp_output.as_file_mut());

            for lines in rx {
                if lines.is_empty() {
                    continue;
                }

                let total_length: usize = lines.iter().map(|line| line.len() + 1).sum();
                let mut batch_string = String::with_capacity(total_length);
                for line in lines {
                    batch_string.push_str(&line);
                    batch_string.push('\n');
                }

                if let Err(e) = writer.write_all(batch_string.as_bytes()) {
                    eprintln!("Failed to write to temporary file: {}", e);
                    return Err(e);
                }
            }

            if let Err(e) = writer.flush() {
                eprintln!("Failed to flush writer: {}", e);
                return Err(e);
            }
            drop(writer);

            m.remove(&pb_writer);

            if output_path.exists() {
                if let Err(e) = fs::remove_file(&output_path) {
                    eprintln!("Failed to remove existing output file: {}", e);
                    return Err(e);
                }
            }

            if let Err(e) = temp_output.persist(&output_path) {
                eprintln!("Failed to persist temporary file to output: {}", e);
                return Err(e.into());
            }

            Ok(())
        })
    };

    let aho_clone = aho.clone();
    let tx_clone = tx.clone();
    let lines_processed_clone = lines_processed.clone();
    let lines_filtered_clone = lines_filtered.clone();

    let processing_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()
        .unwrap();

    processing_pool.scope(|s| {
        files.par_iter().for_each(|file_path| {
            if let Err(e) = process_file(
                file_path,
                &aho_clone,
                &tx_clone,
                &lines_processed_clone,
                &lines_filtered_clone,
                buffer_size,
            ) {
                eprintln!("Error processing file {:?}: {}", file_path, e);
            }
            pb.inc(1);
        });
    });

    drop(tx);

    if let Err(e) = writer_handle.join() {
        eprintln!("Writer thread panicked: {:?}", e);
    }

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
    let reader = BufReader::with_capacity(16 * 1024, file);
    let tokens = reader
        .lines()
        .filter_map(|line| match line {
            Ok(l) => {
                let trimmed = l.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            }
            Err(e) => {
                eprintln!("Error reading blacklist line: {}", e);
                None
            }
        })
        .collect();
    Ok(tokens)
}

fn collect_files_recursive(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(dir)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| {
            if let Err(e) = e {
                eprintln!("Error reading directory entry: {}", e);
                None
            } else {
                Some(e.unwrap())
            }
        })
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
    let file = File::open(file_path).map_err(|e| {
        eprintln!("Failed to open file {:?}: {}", file_path, e);
        e
    })?;
    let mut reader = BufReader::with_capacity(16 * 1024, file);
    let mut buffer = Vec::new();

    let mut filtered_lines = Vec::with_capacity(buffer_size);

    loop {
        buffer.clear();
        let bytes_read = reader.read_until(b'\n', &mut buffer).map_err(|e| {
            eprintln!("Failed to read from file {:?}: {}", file_path, e);
            e
        })?;
        if bytes_read == 0 {
            break;
        }

        let line = String::from_utf8_lossy(&buffer);

        lines_processed.fetch_add(1, Ordering::Relaxed);

        if aho.is_match(line.as_ref()) {
            lines_filtered.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        filtered_lines.push(line.to_string());

        if filtered_lines.len() >= buffer_size {
            let batch = std::mem::take(&mut filtered_lines);
            if let Err(e) = sender.send(batch) {
                eprintln!("Error sending batch to writer: {}", e);
                break;
            }
        }
    }

    if !filtered_lines.is_empty() {
        if let Err(e) = sender.send(std::mem::take(&mut filtered_lines)) {
            eprintln!("Error sending final batch to writer: {}", e);
        }
    }

    Ok(())
}
