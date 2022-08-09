use sqlite::Type as St;

use crate::csv_detect::DetectedCsv;
use csv::StringRecord;
use sqlite::OpenFlags;

pub fn create_db(dbname: String, detected_csvs: Vec<DetectedCsv>) {
    println!("Dbname: {}", dbname);
    let connection_string = format!("./out/{}.db", dbname);
    println!("{}", connection_string);

    let connection = sqlite::Connection::open_with_flags(
        connection_string,
        OpenFlags::set_read_write(OpenFlags::set_create(sqlite::OpenFlags::new())),
    )
    .unwrap();

    for dc in detected_csvs {
        let table_name = dc.tablename.clone();
        match connection.execute(create_table_string(dc)) {
            Ok(_) => println!("Succesfully created table {}", table_name),
            Err(k) => panic!("Could not create table {}: {}", table_name, k),
        }
    }
}

pub fn create_table_string(mut detected_csv: DetectedCsv) -> String {
    let header_part = format!(
        "CREATE TABLE {} ({});",
        detected_csv.tablename,
        create_table_header(detected_csv.headers.clone(), detected_csv.types.clone())
    );
    let rows_part = detected_csv
        .reader
        .records()
        .filter_map(|row| {
            if let Ok(r) = row {
                Some(csv_row_to_sql(
                    detected_csv.tablename.clone(),
                    detected_csv.types.clone(),
                    r,
                ))
            } else {
                None
            }
        })
        .collect::<Vec<String>>()
        .join("\n");
    println!("{}\n{}", header_part, rows_part);
    format!("{}\n{}", header_part, rows_part)
}

pub fn csv_row_to_sql(tablename: String, header_types: Vec<St>, csv_row: StringRecord) -> String {
    let values_part = csv_row
        .iter()
        .zip(header_types.iter())
        .map(|col| {
            let (colv, colt) = col;
            csv_column_to_string(*colt, colv.to_string())
        })
        .collect::<Vec<String>>()
        .join(",");
    format!("INSERT INTO {} VALUES ({});", tablename, values_part)
}

pub fn csv_column_to_string(header_type: St, value: String) -> String {
    match header_type {
        St::Null => "NULL".to_string(),
        St::String => format!(
            "'{}'",
            value
                .trim_start_matches('"')
                .trim_end_matches('"')
        ),
        _ => value,
    }
}

pub fn create_table_header(headers: Vec<String>, header_types: Vec<St>) -> String {
    headers
        .iter()
        .zip(header_types.iter())
        .map(|col| {
            let (name, t) = col;
            format!("{} {}", name, type_to_string(*t))
        })
        .collect::<Vec<String>>()
        .join(",")
}

pub fn type_to_string(t: St) -> String {
    match t {
        St::Null => "NULL",
        St::String => "TEXT",
        St::Integer => "INTEGER",
        St::Float => "REAL",
        St::Binary => "Binary",
    }
    .to_string()
}
