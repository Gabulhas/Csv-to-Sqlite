mod csv_detect;
mod import_csv;
use import_csv::create_db;

use crate::csv_detect::DetectedCsv;
use std::fs::read_dir;
use std::path::PathBuf;

fn main() {
    let directory = "./sample/2017_OP";

    let path = PathBuf::from("/path/to/file.txt");
    let paths = read_dir(directory).unwrap();
    let dirname = path.parent().unwrap();

    let detected_csvs: Vec<DetectedCsv> = paths
        .into_iter()
        .filter_map(|path| {
            let is_path = match path.as_ref() {
                Ok(a) => a.file_name().to_str().unwrap().to_string().contains(".csv"),
                Err(_) => false,
            };
            if is_path {
                match DetectedCsv::new(path.unwrap()) {
                    Ok(a) => Some(a),
                    Err(e) => {
                        println!("Could not detect csv: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        })
        .collect::<_>();
    create_db(dirname.to_str().unwrap().to_string(), detected_csvs)
}
