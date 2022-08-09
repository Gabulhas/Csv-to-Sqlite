use csv::Reader;
use csv::StringRecord;
use sqlite::Type;
use sqlite::Type as St;
use std::fs::DirEntry;
use std::fs::File;

//const SAMPLE_SIZE: i32 = 10;

pub struct DetectedCsv {
    pub tablename: String,
    pub headers: Vec<String>,
    pub types: Vec<Type>,
    pub reader: Reader<File>,
}

impl DetectedCsv {
    pub fn new(pathname: DirEntry) -> Result<DetectedCsv, String> {
        let path_str = pathname.path().to_str().unwrap().to_string();

        let mut reader = csv::Reader::from_path(path_str.clone()).unwrap();

        let headers = match reader.headers() {
            Ok(result) => {
                let record = result;
                record
                    .clone()
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>()
            }
            Err(_) => {
                panic!("Could not read headers")
            }
        };

        let mut records = reader.records();

        let first_types = if let Some(result) = records.next() {
            let record = result.unwrap();
            detect_row_types(record)
        } else {
            panic!("CSV File {} Contains No Lines", path_str)
        };

        let result_types = reader.records().fold(first_types, |acc, el| {
            ambiguity_solve_between_two(acc, detect_row_types(el.unwrap()))
        });

        Ok(DetectedCsv {
            tablename: get_db_mame(pathname),
            headers,
            types: result_types,
            reader: csv::Reader::from_path(path_str).unwrap(),
        })
    }
}

fn detect_row_types(record: StringRecord) -> Vec<St> {
    record.iter().map(detect_column_type).collect()
}

fn detect_column_type(s: &str) -> St {
    if s.parse::<i32>().is_ok() {
        St::Integer
    } else if s.parse::<f64>().is_ok() {
        St::Float
    } else if s.eq("") {
        St::Null
    } else {
        St::String
    }
}

fn type_ambiguity_solve(a: &St, b: &St) -> St {
    match (*a, *b) {
        (St::Integer, St::Float) => St::Float,
        (St::Float, St::Integer) => St::Float,
        (_, St::String) => St::String,
        (St::String, _) => St::String,
        (st, St::Null) => st,
        (St::Null, st) => st,
        (st, _) => st,
    }
}

fn ambiguity_solve_between_two(a: Vec<St>, b: Vec<St>) -> Vec<St> {
    a.iter()
        .zip(b.iter())
        .map(|it| {
            let (ai, bi) = it;
            type_ambiguity_solve(ai, bi)
        })
        .collect()
}

fn get_db_mame(pathname: DirEntry) -> String {
    pathname.file_name().to_str().unwrap().to_string().replace(".csv", "")
}
