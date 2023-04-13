use anyhow::{Error, Result};
use chrono::prelude::*;
use chrono::NaiveDateTime;
use clap::Parser;
use log::{debug, info};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use walkdir::DirEntry;
use walkdir::WalkDir;
#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref DATA_MAP: Mutex<HashMap<usize, Vec<Point>>> = {
        let mut map: HashMap<usize, Vec<Point>> = HashMap::new();
        Mutex::new(map)
    };
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Config {
    #[clap(short, long, required = true)]
    input_dir: Vec<String>,

    #[clap(short, long)]
    output_dir: String,
}

#[derive(Debug)]
pub struct Point {
    id: usize,
    time: NaiveDateTime,
    lat: String,
    lng: String,
}

pub fn get_args() -> Result<Config> {
    let args = Config::parse();

    return Ok(args);
}

pub fn run(config: Config) -> Result<()> {
    debug!("config is {:?}", config);

    // process each file
    let ret: Result<()> = config
        .input_dir
        .iter()
        .map(|x| walk_dir(x))
        .collect::<Result<_>>();

    // sort point
    sort_dict()?;

    persistent2file(config)?;

    return ret;
}

fn sort_dict() -> Result<()> {
    let mut map = DATA_MAP.lock().unwrap();
    for (_, value) in map.iter_mut() {
        value.sort_by(|a, b| a.time.partial_cmp(&b.time).expect("in sorting dict"));
    }

    Ok(())
}

fn persistent2file(config: Config) -> Result<()> {
    if !Path::new(&config.output_dir).try_exists()? {
        fs::create_dir(&config.output_dir)?;
    }
    let map = DATA_MAP.lock().unwrap();

    let ret = map
        .par_iter()
        .map(|(key, value)| {
            let path = Path::new(&config.output_dir).join(key.to_string() + ".txt");
            let mut f = BufWriter::new(File::create(path)?);
            for line in value {
                let datetime = format!("{}", line.time.format("%Y-%m-%d %H:%M:%S"));

                writeln!(f, "{},{},{},{}", key, datetime, line.lat, line.lng)?;
            }
            return Ok(());
        })
        .collect();

    return ret;
}

fn walk_dir(dir: &str) -> Result<()> {
    let mut files: Vec<String> = Vec::new();
    for entry in WalkDir::new(dir) {
        let entry = entry?;
        if !is_valid(&entry) {
            continue;
        }

        let filename = entry.path().to_string_lossy();

        // process
        files.push(filename.to_string());
    }
    let ret = files.par_iter().map(|x| process(x)).collect::<Result<_>>();

    return ret;
}

fn process(filename: &str) -> Result<()> {
    let file = File::open(filename)?;

    for line in BufReader::new(file).lines() {
        let parts: Vec<String> = line?.split(",").map(|x| x.to_string()).collect();
        if parts.len() < 5 {
            continue;
        }

        match parts.last() {
            Some(v) => {
                if v.parse::<usize>()? == 0 {
                    // 0 is invalid
                    continue;
                }
            }
            None => continue,
        }
        let lat = parts[4].trim().to_string();
        let lng = parts[5].trim().to_string();
        if lat == "0.0000000" || lng == "0.0000000" {
            continue;
        }

        let datetime = NaiveDateTime::parse_from_str(&parts[3], "%Y%m%d%H%M%S")?;
        let point = Point {
            id: parts[0].parse::<usize>()?,
            time: datetime,
            lat,
            lng,
        };
        let mut dest = DATA_MAP.lock().unwrap();
        match dest.get_mut(&point.id) {
            Some(v) => v.push(point),
            None => {
                dest.insert(point.id, vec![point]);
            }
        }
    }

    Ok(())
}

fn is_valid(entry: &DirEntry) -> bool {
    entry.file_type().is_file()
}
