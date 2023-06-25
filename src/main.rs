use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use chrono::{Local, DateTime, TimeZone, Utc};
use csv::WriterBuilder;
use getopts::Options;
use std::time::UNIX_EPOCH;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use uuid::Uuid;

// Struct to hold information about a file
#[derive(Debug)]
struct FileInfo {
    id: Uuid,  // Unique identifier
    name: String,
    directory: String,
    create_date: DateTime<Utc>,
    modify_date: DateTime<Utc>,
    size: u64,
    details: Vec<(&'static str, usize)>, // Vector of function results
}

// This trait will be implemented by any function that wants to parse files
trait ParseFunction: Sync + Send {
    // This function will return a description of the function
    fn description(&self) -> &'static str;
    // This function will parse the file and return a result
    fn parse(&self, path: &PathBuf) -> io::Result<usize>;
}

// This struct represents a function that counts the number of lines in a file
struct LineCount;
impl ParseFunction for LineCount {
    fn description(&self) -> &'static str {
        "line_count"
    }
    fn parse(&self, path: &PathBuf) -> io::Result<usize> {
        let file = fs::File::open(path)?;
        let reader = BufReader::new(file);
        Ok(reader.lines().count())
    }
}

// This struct represents a function that counts the number of SQL blocks in a file
struct SqlCount;
impl ParseFunction for SqlCount {
    fn description(&self) -> &'static str {
        "sql_count"
    }
    fn parse(&self, path: &PathBuf) -> io::Result<usize> {
        let content = fs::read_to_string(path)?;
        let re = Regex::new(r"(?is)PROC\sSQL.*?QUIT;").unwrap();
        Ok(re.find_iter(&content).count())
    }
}

// This function calculates the total number of files in a directory
fn compute_total_files(dir: &str) -> io::Result<u64> {
    let mut file_count = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = fs::metadata(entry.path())?;
        if metadata.is_file() {
            file_count += 1;
        } else if metadata.is_dir() {
            file_count += compute_total_files(entry.path().to_str().unwrap())?;
        }
    }
    Ok(file_count)
}

// This function processes a directory
fn process_dir(dir: &str, file_data: &mut Vec<FileInfo>, pb: &ProgressBar, parse_functions: &Vec<Box<dyn ParseFunction>>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = fs::metadata(entry.path())?;
        if metadata.is_file() {
            // Collect file metadata and add to file_data
            let file_name = String::from(entry.file_name().to_str().unwrap());
            let file_directory = String::from(dir);

            let create_date = Utc.timestamp_opt(metadata.created()?.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs() as i64, 0).unwrap();
            let modify_date = Utc.timestamp_opt(metadata.modified()?.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs() as i64, 0).unwrap();
        
            let size = metadata.len();

            // Apply each parse function to the file and store the results
            let mut details = vec![];
            for function in parse_functions {
                let result = function.parse(&entry.path())?;
                details.push((function.description(), result));
            }

            let file_info = FileInfo {
                id: Uuid::new_v4(),
                name: file_name,
                directory: file_directory,
                create_date: create_date,
                modify_date: modify_date,
                size: size,
                details: details,
            };
            file_data.push(file_info);
            pb.inc(1);
        } else if metadata.is_dir() {
            // Recursively process subdirectory
            process_dir(entry.path().to_str().unwrap(), file_data, pb, parse_functions)?;
        }
    }
    Ok(())
}

// This function prints a usage message
fn print_usage(opts: &Options) {
    let brief = "Usage: sas_parser_rust -i INPUTDIR -o OUTPUTDIR";
    print!("{}", opts.usage(&brief));
}

fn main() -> io::Result<()> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();
    opts.optopt("i", "input", "set input directory", "INPUT");
    opts.optopt("o", "output", "set output directory", "OUTPUT");
    opts.optflag("h", "help", "print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!("{}", f.to_string()) }
    };
    if matches.opt_present("h") {
        print_usage(&opts);
        return Ok(());
    }
    let input_dir = matches.opt_str("i").unwrap();
    let output_dir = matches.opt_str("o").unwrap();

    // Check if input and output directories exist
    if !Path::new(&input_dir).exists() {
        panic!("Input directory does not exist");
    }
    if !Path::new(&output_dir).exists() {
        panic!("Output directory does not exist");
    }

    // Compute total number of files
    let total_files = compute_total_files(&input_dir)?;
    // Create a new progress bar
    let pb = ProgressBar::new(total_files);
    pb.set_style(
        ProgressStyle::default_bar()
        .progress_chars("#>-")
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta})")
        .unwrap()
    );

    // Initialize a vector of parse functions
    let parse_functions: Vec<Box<dyn ParseFunction>> = vec![
        Box::new(LineCount),
        Box::new(SqlCount),
    ];

    // Collect file data
    let mut file_data: Vec<FileInfo> = vec![];
    process_dir(&input_dir, &mut file_data, &pb, &parse_functions)?;

    // Write the file data to the CSV file
    let now = Local::now();
    let output_file_path = format!("{}/summary_{}.csv", output_dir, now.format("%Y%m%d%H%M%S"));
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(output_file_path)?;
    wtr.write_record(&["id", "file_nm", "file_dir", "create_dt", "modify_dt", "size_bytes"])?;
    for file_info in &file_data {
        wtr.write_record(&[
            &file_info.id.to_string(),
            &file_info.name,
            &file_info.directory,
            &file_info.create_date.format("%Y-%m-%d %H:%M:%S").to_string(),
            &file_info.modify_date.format("%Y-%m-%d %H:%M:%S").to_string(),
            &file_info.size.to_string(),
        ])?;
    }

    // Write detail file
    let output_file_path = format!("{}/detail_{}.csv", output_dir, now.format("%Y%m%d%H%M%S"));
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(output_file_path)?;
    wtr.write_record(&["id", "function", "result"])?;
    for file_info in file_data {
        for detail in file_info.details {
            wtr.write_record(&[
                &file_info.id.to_string(),
                detail.0,
                &detail.1.to_string(),
            ])?;
        }
    }

    // Finish writing to the CSV file
    wtr.flush()?;

    // Finish the progress bar
    pb.finish_with_message("done");
    Ok(())
}

