/*
* sas_parser_rust
*
* Author: Gary Cattabriga
* Date: 6/23/2023
* Last updated: 6/30/2023
*               add timer so we can get more precise timing of the parsing routine
* Version: 0.7
* This program analyzes a directory of text files, providing several output metrics
* including line count, count of SQL statements, and extracting all SQL blocks.
*
* Inputs:
* -i, --input : Path to the directory to analyze
* -o, --output : Path to the directory where the output CSV files will be written
*
* Outputs:
* Two CSV files in the specified output directory:
* 1. summary.csv - includes information about each file such as UUID, name, directory, creation date, modification date, and size
* 2. detail.csv - includes the results of parsing functions such as line_count, sql_count, and get_sql
*
*/

use chrono::{DateTime, Local, TimeZone, Utc};
use csv::WriterBuilder;
use getopts::Options;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::time::UNIX_EPOCH;
use uuid::Uuid;
use std::time::{Duration, Instant};

#[derive(Debug)]
// Define structure to hold information about each file
struct FileInfo {
    uuid: String,
    name: String,
    directory: String,
    create_date: DateTime<Utc>,
    modify_date: DateTime<Utc>,
    size: u64,
}

// ParseFunction is a function that takes a file_id and a file_path,
// and returns a Vec of results where each result is a tuple of (Function name, File ID, Result)
type ParseFunction = fn(&String, &String) -> Vec<(String, String, String)>;

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


// Define our parse functions here:
/* -------------------------
* Parse Functions: These are the functions that will be used to parse the files.
* They each perform a unique analysis on the file:
* - line_count: Counts the number of lines in a file.
* - sql_count: Counts the number of SQL blocks in a file.
* - get_sql: Extracts SQL blocks from a file.
* --------------------------- */

fn line_count(file_id: &String, file_path: &String) -> Vec<(String, String, String)> {
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);
    let line_count = reader.lines().count();
    vec![(file_id.clone(), "line_count".to_string(), line_count.to_string())]
}

fn sql_count(file_id: &String, file_path: &String) -> Vec<(String, String, String)> {
    let content = fs::read_to_string(file_path).unwrap();
    let content = content.to_uppercase();
    let re = Regex::new(r"(?s)PROC\s+SQL.*?QUIT;").unwrap();
    let sql_count = re.find_iter(&content).count();
    vec![(file_id.clone(), "sql_count".to_string(), sql_count.to_string())]
}

fn get_sql(file_id: &String, file_path: &String) -> Vec<(String, String, String)> {
    let mut results: Vec<(String, String, String)> = Vec::new();
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);
    let mut inside_sql_block = false;
    let mut sql_block: Vec<String> = Vec::new();
    let mut sql_start_line = 0;
    for (line_number, line_result) in reader.lines().enumerate() {
        let line = line_result.unwrap();
        let upper_line = line.to_uppercase();
        if !inside_sql_block && upper_line.contains("PROC SQL") {
            inside_sql_block = true;
            sql_start_line = line_number + 1;
        }
        if inside_sql_block {
            sql_block.push(line);
            if upper_line.contains("QUIT;") {
                inside_sql_block = false;
                results.push((
                    String::from(file_id),
                    String::from("get_sql"),
                    format!("({}, {})", sql_start_line, sql_block.join("\n")),
                ));
                sql_block.clear();
            }
        }
    }
    results
}

fn get_libname(file_id: &String, file_path: &String) -> Vec<(String, String, String)> {
    let content = fs::read_to_string(file_path).unwrap();
    let mut results: Vec<(String, String, String)> = Vec::new();
    for (line_number, line) in content.lines().enumerate() {
        if line.to_uppercase().starts_with("LIBNAME") {
            results.push((file_id.clone(), "get_libname".to_string(), format!("({})", line)));
        }
    }
    results
}

fn get_password(file_id: &String, file_path: &String) -> Vec<(String, String, String)> {
    let content = fs::read_to_string(file_path).unwrap();
    let mut results: Vec<(String, String, String)> = Vec::new();
    for (line_number, line) in content.lines().enumerate() {
        let modified_line = line.to_uppercase().replace(char::is_whitespace, "");
        if modified_line.contains("PASSWORD=") && !modified_line.contains("&PASSWORD") {
            results.push((file_id.clone(), "get_password".to_string(), format!("({}, {})", line_number + 1, modified_line)));
        }
    }
    results
}


fn export_count(file_id: &String, file_path: &String) -> Vec<(String, String, String)> {
    let content = fs::read_to_string(file_path).unwrap();
    let count = content.to_uppercase().matches("EXPORT").count();
    vec![(file_id.clone(), "export_count".to_string(), count.to_string())]
}

fn null_count(file_id: &String, file_path: &String) -> Vec<(String, String, String)> {
    let content = fs::read_to_string(file_path).unwrap();
    let content = content.to_uppercase();
    let count = content.matches("_NULL_").count();
    vec![(file_id.clone(), "null_count".to_string(), count.to_string())]
}

fn find_date(file_id: &String, file_path: &String) -> Vec<(String, String, String)> {
    let re = Regex::new(r"\b\d{4}-\d{2}-\d{2}\b").unwrap();
    let content = fs::read_to_string(file_path).unwrap();
    let mut results: Vec<(String, String, String)> = Vec::new();
    for (line_number, line) in content.lines().enumerate() {
        if re.is_match(line) {
            results.push((file_id.clone(), "find_date".to_string(), format!("({}, {})", line_number + 1, line)));
        }
    }
    results
}

fn find_file_name(file_id: &String, file_path: &String, file_list: &Vec<String>) -> Vec<(String, String, String)> {
    let content = fs::read_to_string(file_path).unwrap();
    let mut results: Vec<(String, String, String)> = Vec::new();
    for (line_number, line) in content.lines().enumerate() {
        for file_name in file_list {
            if line.contains(file_name) {
                results.push((file_id.clone(), "find_file_name".to_string(), format!("({}):{}", line_number + 1, line)));
                break;
            }
        }
    }
    results
}



/* -------------------------
* Main Function: This is where the program execution begins.
* This function does the following:
* 1. Parse command line arguments
* 2. Create progress bar
* 3. Process each file and directory
* 4. Write file metadata to CSV
* 5. Write file parse results to CSV
* 6. Finish progress bar
* --------------------------- */
fn main() -> io::Result<()> {
    // Command line argument handling
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

    if !Path::new(&input_dir).exists() {
        panic!("Input directory does not exist");
    }
    if !Path::new(&output_dir).exists() {
        panic!("Output directory does not exist");
    }

    let total_files = compute_total_files(&input_dir)?;
    let pb = ProgressBar::new(total_files);
    pb.set_style(
        ProgressStyle::default_bar()
        .progress_chars("#>-")
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta})")
        .unwrap()
    );


    let mut file_data: Vec<FileInfo> = vec![];

    let start_time = Instant::now(); // Start the timer

    process_dir(&input_dir, &mut file_data, &pb)?;

    let elapsed_time = start_time.elapsed(); // Calculate the elapsed time

    let now = Local::now();
    let output_file_path = format!("{}/summary_{}.csv", output_dir, now.format("%Y%m%d%H%M%S"));
    let mut wtr_summary = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_file_path)?;

    wtr_summary.write_record(&["uuid", "file_nm", "file_dir", "create_dt", "modify_dt", "size_bytes"])?;
    for file_info in &file_data {
        wtr_summary.write_record(&[
            &file_info.uuid,
            &file_info.name,
            &file_info.directory,
            &file_info.create_date.format("%Y-%m-%d %H:%M:%S").to_string(),
            &file_info.modify_date.format("%Y-%m-%d %H:%M:%S").to_string(),
            &file_info.size.to_string(),
        ])?;
    }

    wtr_summary.flush()?;

    let output_file_path = format!("{}/detail_{}.csv", output_dir, now.format("%Y%m%d%H%M%S"));
    let mut wtr_detail = WriterBuilder::new()
        .has_headers(true)
        .from_path(output_file_path)?;

    wtr_detail.write_record(&["uuid", "func_nm", "result"])?;

    let parse_functions: Vec<ParseFunction> = vec![
        line_count, 
        sql_count, 
        get_sql, 
        get_libname, 
        get_password,
        export_count,
        null_count,
        find_date
    ];

    for file_info in &file_data {
        let file_path = format!("{}/{}", &file_info.directory, &file_info.name);
        for parse_function in &parse_functions {
            let results = parse_function(&file_info.uuid, &file_path);
            for result in results {
                let record: Vec<String> = vec![result.0, result.1, result.2];
                wtr_detail.write_record(&record)?;
            }
        }
    }

    wtr_detail.flush()?;

    pb.finish_with_message("done");
    println!("Total time elapsed: {:?}", elapsed_time);

    Ok(())
}

/* -------------------------
* This function processes a single file, it does the following:
* 1. Get the metadata
* 2. Create and store a FileInfo structure
* 3. Update the progress bar
* --------------------------- */
fn process_dir(dir: &str, file_data: &mut Vec<FileInfo>, pb: &ProgressBar) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = fs::metadata(entry.path())?;
        if metadata.is_file() {
            let file_name = String::from(entry.file_name().to_str().unwrap());
            let file_directory = String::from(dir);

            let create_date = Utc.timestamp_opt(metadata.created()?.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs() as i64, 0).unwrap();
            let modify_date = Utc.timestamp_opt(metadata.modified()?.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs() as i64, 0).unwrap();

            let size = metadata.len();
            let uuid = Uuid::new_v4().to_string();

            let file_info = FileInfo {
                uuid: uuid,
                name: file_name,
                directory: file_directory,
                create_date: create_date,
                modify_date: modify_date,
                size: size,
            };

            file_data.push(file_info);
            pb.inc(1);
        } else if metadata.is_dir() {
            process_dir(entry.path().to_str().unwrap(), file_data, pb)?;
        }
    }
    Ok(())
}

/* -------------------------
* This function prints usage of the command-line tool.
* It's called when the command line arguments are not valid.
* --------------------------- */
fn print_usage(opts: &Options) {
    let brief = format!("Usage: ./text_file_analyzer [options]");
    print!("{}", opts.usage(&brief));
}
