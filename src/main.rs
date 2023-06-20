// Import necessary modules
use std::env;
use std::fs;
use std::io::{self, BufRead};
use std::path::Path;
use chrono::{Local, DateTime, TimeZone, Utc};
use csv::WriterBuilder;
use getopts::Options;
use std::time::UNIX_EPOCH;

// Define a struct to hold file information
#[derive(Debug)]
struct FileInfo {
    name: String,
    directory: String,
    create_date: DateTime<Utc>,
    modify_date: DateTime<Utc>,
    size: u64,
    lines: usize,
}

// Main function
fn main() -> io::Result<()> {
    // Get command line arguments
    let args: Vec<String> = env::args().collect();

    // Define and set command line options
    let mut opts = Options::new();
    opts.optopt("i", "input", "set input directory", "INPUT");
    opts.optopt("o", "output", "set output directory", "OUTPUT");
    opts.optflag("h", "help", "print this help menu");

    // Match command line arguments to the defined options
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!("{}", f.to_string()) }
    };

    // If help option is specified, print help and return
    if matches.opt_present("h") {
        print_usage(&opts);
        return Ok(());
    }

    // Get the input and output directories from command line arguments
    let input_dir = matches.opt_str("i").unwrap();
    let output_dir = matches.opt_str("o").unwrap();

    // Check if input and output directories exist
    if !Path::new(&input_dir).exists() {
        panic!("Input directory does not exist");
    }
    if !Path::new(&output_dir).exists() {
        panic!("Output directory does not exist");
    }

    // Initialize an empty vector to hold the file information
    let mut file_data: Vec<FileInfo> = vec![];

    // Process the directory and get file information
    process_dir(&input_dir, &mut file_data)?;

    // Create the output CSV file
    let now = Local::now();
    let output_file_path = format!("{}/summary_{}.csv", output_dir, now.format("%Y%m%d%H%M%S"));
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(output_file_path)?;

    // Write headers to the CSV file
    wtr.write_record(&["file_nm", "file_dir", "create_dt", "modify_dt", "size_bytes", "line_ct"])?;

    // Write file information to the CSV file
    for file_info in file_data {
        wtr.write_record(&[
            &file_info.name,
            &file_info.directory,
            &file_info.create_date.format("%Y-%m-%d %H:%M:%S").to_string(),
            &file_info.modify_date.format("%Y-%m-%d %H:%M:%S").to_string(),
            &file_info.size.to_string(),
            &file_info.lines.to_string(),
        ])?;
    }

    // Flush the writer
    wtr.flush()?;

    // Return result
    Ok(())
}

// Function to process directory and get file information
fn process_dir(dir: &str, file_data: &mut Vec<FileInfo>) -> io::Result<()> {
    // Iterate over each entry in the directory
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = fs::metadata(entry.path())?;

        // Check if the entry is a file
        if metadata.is_file() {
            let file_name = String::from(entry.file_name().to_str().unwrap());
            let file_directory = String::from(dir);

            println!("Reading metadata for: {:?}", entry.path());

            // Get the creation and modification times
            let create_date = Utc.timestamp_opt(metadata.created()?.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs() as i64, 0).unwrap();
            let modify_date = Utc.timestamp_opt(metadata.modified()?.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs() as i64, 0).unwrap();

            // Get the file size
            let size = metadata.len();

            println!("Opening file: {:?}", entry.path());
            let file = fs::File::open(&entry.path())?;

            // Count the lines in the file
            let reader = io::BufReader::new(file);
            let lines = reader.lines().count();

            // Create a FileInfo struct and push it to the vector
            let file_info = FileInfo {
                name: file_name,
                directory: file_directory,
                create_date: create_date,
                modify_date: modify_date,
                size: size,
                lines: lines,
            };
            file_data.push(file_info);
        } else if metadata.is_dir() {
            // If the entry is a directory, recursively call the process_dir function
            process_dir(entry.path().to_str().unwrap(), file_data)?;
        }
    }
    // Return result
    Ok(())
}

// Function to print usage of the program
fn print_usage(opts: &Options) {
    let brief = "Usage: sas_parser_rust -i INPUTDIR -o OUTPUTDIR";
    print!("{}", opts.usage(&brief));
}
