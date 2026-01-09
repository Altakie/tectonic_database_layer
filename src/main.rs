#![allow(dead_code, unused_variables)]

use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::num::ParseIntError;
use std::time::{self};

use rocksdb::Error;

#[derive(Debug)]
struct Insert {
    key: String,
    value: String,
}

impl Insert {
    fn new(key: String, value: String) -> Self {
        Self { key, value }
    }
}

#[derive(Debug)]
struct Update {
    key: String,
    value: String,
}

impl Update {
    fn new(key: String, value: String) -> Self {
        Self { key, value }
    }
}

#[derive(Debug)]
struct PointQuery {
    key: String,
}

impl PointQuery {
    fn new(key: String) -> Self {
        Self { key }
    }
}

#[derive(Debug)]
struct RangeQuery {
    start_key: String,
    range: usize,
}

impl RangeQuery {
    fn new(start_key: String, range: usize) -> Self {
        Self { start_key, range }
    }
}

fn main() {
    // Ask for an input file (workflow file) and database layer
    // Init database layer
    // Read through the workflow file line by line and call operations from intialized database
    // layer
    //
    //
    // TODO: Keep track of:
    // average operation latency and for each operation
    // Total Throughout
    // Successful operations (think about point queries)
    // Number of operations for each operation and total
    // Min latency
    // Max latency
    // 50th percentile latency for operations
    // 95th percentile latency for operations
    // 99th percentile latency for operations
    //
    // TODO: Windowed version of operations for printing status?
    //
    // Easiest way to do this is a hashmap

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        println!("Expected 2 args, got {} args", args.len());
        return;
    }

    let file_path = args[1].clone();

    let file = File::open(file_path).expect("Need a proper file path");
    let buf_reader = BufReader::new(file);

    let db_layer: Box<dyn DBTranslationLayer> = match RocksDB::new() {
        Ok(db) => Box::new(db),
        Err(err) => {
            eprintln!("Failed to create db because of error {err}");
            return;
        }
    };

    let mut operation_statistics = HashMap::<&str, Statistics>::new();

    for line in buf_reader.lines() {
        let res = parse_line(line, db_layer.as_ref(), &mut operation_statistics);
        if res.is_err() {
            println!("{:#?}", res);
            return;
        }
    }
}

#[derive(Default)]
struct Statistics {
    operation_latencies: Vec<u128>,
    count: u128,
    sum: u128,
    min: Option<u128>,
    max: Option<u128>,
}

impl Statistics {
    fn add_latency(&mut self, latency: u128) {
        self.count += 1;
        self.sum += latency;
        match &mut self.min {
            Some(min) => {
                if *min > latency {
                    *min = latency;
                }
            }
            None => self.min = Some(latency),
        }

        match &mut self.max {
            Some(max) => {
                if *max > latency {
                    *max = latency;
                }
            }
            None => self.max = Some(latency),
        }

        self.operation_latencies.push(latency);
    }
}

#[derive(Debug)]
enum LineParseError {
    MissingArgument,
    WrongArgumentType(ParseIntError),
    LineCouldNotBeRead(std::io::Error),
    UnknownOperation,
}

fn parse_line(
    line: Result<String, std::io::Error>,
    db_layer: &dyn DBTranslationLayer,
    operation_statistics_map: &mut HashMap<&str, Statistics>,
) -> Result<(), LineParseError> {
    let line = line.map_err(LineParseError::LineCouldNotBeRead)?;
    let mut line_iter = line.split_whitespace();
    let operation = line_iter.next().unwrap();

    let start_time: std::time::Instant;
    let key: &str;

    match operation {
        "I" => {
            let insert_op = Insert::new(
                line_iter
                    .next()
                    .ok_or(LineParseError::MissingArgument)?
                    .to_string(),
                line_iter
                    .next()
                    .ok_or(LineParseError::MissingArgument)?
                    .to_string(),
            );

            key = "I";
            start_time = std::time::Instant::now();
            db_layer.insert(insert_op.key, insert_op.value);
        }
        "P" => {
            let point_query_op = PointQuery::new(
                line_iter
                    .next()
                    .ok_or(LineParseError::MissingArgument)?
                    .to_string(),
            );
            key = "P";
            start_time = std::time::Instant::now();
            db_layer.point_query(point_query_op.key)
        }
        "U" => {
            let update_op = Update::new(
                line_iter
                    .next()
                    .ok_or(LineParseError::MissingArgument)?
                    .to_string(),
                line_iter
                    .next()
                    .ok_or(LineParseError::MissingArgument)?
                    .to_string(),
            );
            key = "U";
            start_time = std::time::Instant::now();
            db_layer.update(update_op.key, update_op.value)
        }
        "S" => {
            let scan_op = RangeQuery::new(
                line_iter
                    .next()
                    .ok_or(LineParseError::MissingArgument)?
                    .to_string(),
                line_iter
                    .next()
                    .ok_or(LineParseError::MissingArgument)?
                    .parse::<usize>()
                    .map_err(LineParseError::WrongArgumentType)?,
            );
            key = "S";
            start_time = time::Instant::now();
            db_layer.range_query(scan_op.start_key, scan_op.range)
        }
        "M" => {
            // Merge
        }
        "R" => {
            // Range delete
        }
        _ => return Err(LineParseError::UnknownOperation),
    };

    let latency = start_time
        .duration_since(std::time::Instant::now())
        .as_millis();
    let map = operation_statistics_map.entry(key).or_default();
    map.add_latency(latency);

    // Do two matches maybe
    // Also can do lazy evaluation here

    Ok(())
}

trait DBTranslationLayer {
    // Setup
    fn init(&mut self) -> Result<(), Error>;
    fn cleanup(self);

    // Operations
    fn insert(&self, key: String, value: String);
    fn update(&self, key: String, value: String);
    fn merge(&self, key: String, value: String);
    fn point_delete(&self, key: String);
    fn point_query(&self, key: String);
    // TODO: Range query count and range delete count
    fn range_query(&self, start_key: String, end_key: String);
    fn range_query_count(&self, start_key: String, range: usize);
    fn range_delete(&self, start_key: String, end_key: String);
    fn range_delete_count(&self, start_key: String, range: usize);
}

struct PrintDB {}

impl DBTranslationLayer for PrintDB {
    fn init(&mut self) -> Result<(), Error> {
        println!("Initialized");
        Ok(())
    }

    fn cleanup(self) {
        println!("Done");
    }

    fn point_query(&self, key: String) {
        println!("PointQuery: {{key = {key}}}")
    }

    fn update(&self, key: String, value: String) {
        println!("Update: {{key = {key}, value = {value}}}");
        todo!()
    }

    fn insert(&self, key: String, value: String) {
        println!("Insert: {{key = {key}, value = {value}}}");
    }

    fn range_query(&self, start_key: String, end_key: String) {
        println!("Range Query: {{start_key = {start_key}, end_key = {end_key}}}");
    }

    fn range_query_count(&self, start_key: String, range: usize) {
        println!("Range Query: {{key = {start_key}, count = {range}}}");
    }

    fn point_delete(&self, key: String) {
        println!(" Delete: {{key = {key}}}");
    }

    fn range_delete(&self, start_key: String, end_key: String) {
        println!("Range Delete: {{start_key = {start_key}, end_key = {end_key}}}");
    }

    fn range_delete_count(&self, start_key: String, range: usize) {
        println!("Range Query: {{key = {start_key}, count = {range}}}");
    }

    fn merge(&self, key: String, value: String) {
        println!("Merge: {{key = {key}, value = {value}}}");
    }
}

struct RocksDB {
    db: rocksdb::DB,
    print: PrintDB,
}

impl RocksDB {
    fn new() -> Result<Self, Error> {
        let dir = temp_dir();
        Ok(Self {
            db: rocksdb::DB::open_default(dir.as_path())?,
            print: PrintDB {},
        })
    }
}

impl DBTranslationLayer for RocksDB {
    fn init(&mut self) -> Result<(), Error> {
        let dir = temp_dir();
        self.db = rocksdb::DB::open_default(dir.as_path())?;

        Ok(())
    }

    fn cleanup(self) {
        std::mem::drop(self);
    }

    fn point_query(&self, key: String) {
        let _ = self.db.get(key);
    }

    fn update(&self, key: String, value: String) {
        self.insert(key, value);
    }

    fn insert(&self, key: String, value: String) {
        let _ = self.db.put(key.clone(), value.clone());
        self.print.insert(key, value);
    }

    fn range_query(&self, start_key: String, end_key: String) {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_upper_bound(end_key);
        let mut db_iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward),
            opts,
        );
    }

    fn range_query_count(&self, start_key: String, range: usize) {
        let mut db_iter = self.db.raw_iterator();
        let mut res: Vec<Option<String>> = vec![];
        db_iter.seek(start_key.clone());
        let mut buf = String::new();
        if let Some(mut item) = db_iter.item() {
            // let mut key = String::new();
            // item.0.read_to_string(&mut buf);
            // std::mem::swap(&mut key, &mut buf);
            // println!("Key: {:#?}", key);
            item.1
                .read_to_string(&mut buf)
                .expect("Item could not be read to string");
            let mut item = String::new();
            std::mem::swap(&mut item, &mut buf);
            res.push(Some(item));
        };
        for i in 0..range {
            db_iter.next();
            if let Some(mut item) = db_iter.item() {
                // let mut key = String::new();
                // item.0.read_to_string(&mut buf);
                // std::mem::swap(&mut key, &mut buf);
                // println!("Key: {:#?}", key);
                item.1
                    .read_to_string(&mut buf)
                    .expect("Item could not be read to string");
                let mut item = String::new();
                std::mem::swap(&mut item, &mut buf);
                res.push(Some(item));
            };
        }
    }

    fn point_delete(&self, key: String) {
        todo!()
    }

    fn merge(&self, key: String, value: String) {
        todo!()
    }

    fn range_delete(&self, start_key: String, end_key: String) {
        todo!()
    }

    fn range_delete_count(&self, start_key: String, range: usize) {
        todo!()
    }
}
