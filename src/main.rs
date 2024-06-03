use std::fs::{self, DirEntry};

use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::str::FromStr;

use filetime::{set_file_mtime, FileTime};
use kv::*;
use regex::Regex;
use sha2::Digest;
use std::{io, thread};

extern crate byte_unit;
use byte_unit::Byte;

use tempfile::TempDir;

extern crate queues;

use std::sync::mpsc;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use rayon::prelude::*;

use clap::{CommandFactory, Parser};
use num_cpus;

fn help() -> i32 {
    let mut cmd = Args::command();
    cmd.print_help().unwrap();
    -1
}


const READ_BLK_SIZE: usize = 4096;

struct FileScanStats {
    is_file: bool,
    is_duplicate: bool,
    is_dry_run: bool,
    orig_file_len: u64,
    bytes_created: u64,
}

fn process_single_file(
    store: &Store,
    args: &mut ScanDirArgs,
    dentry: &DirEntry,
) -> Result<FileScanStats, String> {
    let mut res = FileScanStats {
        is_file: false,
        is_duplicate: false,
        is_dry_run: false,
        orig_file_len: 0,
        bytes_created: 0,
    };

    if args.show_progress {
        print!("\r scanned files: {}        \r", args.files_scanned);
    }

    let mut file = fs::File::open(dentry.path()).unwrap();
    //TODO: handle errors !
    res.orig_file_len = file.metadata().unwrap().len();

    //let mut hasher = Sha256::new();
    let mut hasher = md5::Md5::new();

    _ = io::copy(&mut file, &mut hasher).unwrap();
    let hash = hasher.finalize();

    //println!("FILE : {}   {:?}", dentry.path().display(), hash.to_ascii_lowercase());

    let a = hash.to_vec().try_into().unwrap_or_else(|v: Vec<u8>| {
        panic!("Expected a Vec of length {} but it was {}", 16, v.len())
    });
    let h = u128::from_be_bytes(a);

    let key: Integer = Integer::from(h);

    //println!("FILE : {}   md5:{}", dentry.path().display(), h);

    // A Bucket provides typed access to a section of the key/value store
    let bucket = store.bucket::<Integer, String>(Some("md5s_db")).unwrap();

    if bucket.get(&key).unwrap() == None {
        if args.verbose {
            println!("++++ NEW FILE : {}   md5:{}", dentry.path().display(), h);
        }

        //Copy/process file and when done add key into kv store ..Use transactions
        //https://docs.rs/kv/latest/kv/

        let mut b = [0; READ_BLK_SIZE];
        file.seek(io::SeekFrom::Start(0))
            .expect(" could not seek into file");

        //create dst file
        let dst_path = Path::new(&args.dst_dir);
        let mut p: PathBuf;
        if dentry.path().is_absolute() {
            p = dst_path.join(dentry.path().strip_prefix(&args.src_dir).unwrap());
        } else {
            p = dst_path.join(dentry.path().strip_prefix("..").unwrap());
        }

        if args.name_prefix_checksum {
            let mut n = String::new();
            n += &h.to_string();
            n += "-";
            n += p.file_name().unwrap().to_str().unwrap();

            p = p.parent().unwrap().join(Path::new(&n));
        }

        if args.verbose {
            println!("going to create {}", p.as_path().display());
        }

        res.bytes_created += file.metadata().unwrap().len();

        if args.dry_run {
            res.is_dry_run = true;
            let value = dentry.path().display().to_string();
            match bucket.set(&key, &value) {
                Ok(_) => (),

                Err(e) => {
                    eprint!("Err while updating kv: {:?}", e);
                    return Err("Err while updating kv".to_string());
                }
            }

            return Ok(res);
        }

        if !p.parent().unwrap().exists() {
            fs::create_dir_all(p.parent().unwrap()).expect("could not create dir");
        }

        if p.exists() {
            if args.keepcopy {
                //rename existing dst file
                let mut dcp = String::new();

                dcp += p.parent().unwrap().to_str().unwrap();
                dcp += "/Copy-<time_t+ms>";
                dcp += p.file_name().unwrap().to_str().unwrap();

                eprintln!("Saving previous copy {} ", dcp);
                return Ok(res);

                //TODO
                //1. update k/v path->md5 and md5-> path
                //2. and create copy

                //fs::rename(p, pcopy)
            } else if !args.overwrite {
                res.is_duplicate = true;
                // by default skip overwriting existing file
                eprintln!(
                    "Skipping, dst file {} exists will not overwrite ... Use {} or {} ",
                    "--overwrite",
                    "--keep-copy",
                    p.display()
                );
                return Ok(res);
            }
        }

        let mut f_dst = fs::File::create(&p).expect("could not cerate file");

        let mtime = FileTime::from_system_time(file.metadata().unwrap().modified().unwrap());
        match set_file_mtime(&p, mtime) {
            Ok(_) => (),
            Err(e) => {
                eprint!("could not set mtime!!! {}..", e.to_string())
            }
        }

        loop {
            match file.read(&mut b[..]) {
                Ok(n) => {
                    if 0 == n {
                        break;
                    }

                    match f_dst.write(&b[..n]) {
                        Ok(_) => (),
                        Err(e) => {
                            eprint!("Err : could not write into {},  {:?}", p.display(), e);
                            exit(e.raw_os_error().unwrap());
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Err while reading from src file {}", e.to_string());
                }
            }
        }

        let value = dentry.path().display().to_string();
        match bucket.set(&key, &value) {
            Ok(_) => (),

            Err(e) => {
                eprint!("Err while updating kv: {:?}", e);
                return Err("Err while updating kv".to_string());
            }
        }
    } else {
        res.is_duplicate = true;

        if args.verbose {
            println!("DUPLICATE  FILE : {}   md5:{}", dentry.path().display(), h);
        }
    }

    Ok(res)
}

fn scan_dir(
    args: &mut ScanDirArgs,
    files: &mut Vec<DirEntry>,
    dir: &String,
    tx: mpsc::Sender<FileScanStats>,
) -> Result<i32, io::Error> {
    args.dirs_scanned += 1;

    tx.send(FileScanStats {
        is_file: (false),
        is_duplicate: (false),
        is_dry_run: (false),
        orig_file_len: (0),
        bytes_created: (0),
    })
    .unwrap();

    if args.show_progress && args.mt {
        print!(
            "\r found files: {} ,  processed: {}  , workers:{}  \r",
            files.len(),
            args.files_scanned,
            args.threads_num
        );
    }

    let res = fs::read_dir(&dir);

    match res {
        Ok(paths) => {
            for p in paths {
                let entry = p.unwrap();

                if entry.path().is_dir() {
                    //process dir
                    let s = entry.path().as_path().to_str().unwrap().to_string();
                    match scan_dir(args, files, &s, tx.clone()) {
                        Ok(_) => (),
                        Err(e) => {
                            println!("Err {} : {}", &s, e)
                        }
                    }
                } else {
                    //process file only if it matches --name (if provided)

                    match &args.name {
                        Some(re) => {
                            if re.is_match(entry.file_name().to_ascii_lowercase().to_str().unwrap())
                            {
                                _ = files.push(entry);

                                tx.send(FileScanStats {
                                    is_file: (true),
                                    is_duplicate: (false),
                                    is_dry_run: (false),
                                    orig_file_len: (0),
                                    bytes_created: (0),
                                })
                                .unwrap();
                            } else {
                                if args.verbose {
                                    eprintln!(
                                        "~~~~~~~~~ Ignoring file {}",
                                        entry.file_name().to_str().unwrap()
                                    );
                                }
                            }
                        }
                        None => {
                            _ = files.push(entry);

                            tx.send(FileScanStats {
                                is_file: (true),
                                is_duplicate: (false),
                                is_dry_run: (false),
                                orig_file_len: (0),
                                bytes_created: (0),
                            })
                            .unwrap();
                        }
                    }
                }
            }
        }

        Err(e) => return Err(e),
    }

    Ok(0)
}


#[derive(Debug, Clone)]
struct ScanDirArgs {
    mt: bool,
    threads_num: u32,
    src_dir: String,
    dst_dir: String,
    dry_run: bool,
    show_progress: bool,
    verbose: bool,
    overwrite: bool,
    keepcopy: bool,
    name_prefix_checksum: bool,
    name: Option<Regex>,
    bytes_created: u64,
    files_scanned: u64,
    dirs_scanned: u64,
    duplicates_no: u64,
    duplicates_total_size: u64,
}

#[derive(Debug, Clone)]

struct InitArgs<'a> {
    store: &'a Store,
    args: &'a ScanDirArgs,
    tx: mpsc::Sender<FileScanStats>,
}

fn process_files(store: &Store, files: &Vec<DirEntry>, args: &mut ScanDirArgs) {
    let (tx, rx) = mpsc::channel::<FileScanStats>();

    let ia = InitArgs {
        store: store,
        args: args,
        tx: tx,
    };

    let files_total = u64::try_from(files.len()).unwrap();

    let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {wide_msg}")
        .unwrap()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ");

    let h = thread::spawn(move || {
        let m = MultiProgress::new();

        let pb = m.add(ProgressBar::new(50));
        pb.set_style(spinner_style.clone());
        pb.set_prefix(format!("[2/2]"));

        {
            let mut files: u64 = 0;
            // let mut dirs: u64 = 0;
            for _msg in rx {
                files += 1;
                // if msg.is_file {
                //     files += 1;
                // } else {
                //     dirs += 1;
                // }

                pb.set_message(format!("{}: files:{}/{}", "copying", files, files_total));
                pb.inc(1);
            }

            pb.finish_with_message(format!("finished copying {} unique files", files));
        }

        //m.clear().unwrap();
    });

    files.into_par_iter().for_each_with(ia, |s, x| {
        //println!("{}", String::from_str(x.path().to_str().unwrap()).unwrap());
        let res = process_single_file(s.store, &mut s.args.clone(), x);

        if s.tx.send(res.unwrap()).is_err() {
            eprintln!("XREHOTA!!!!! cant send!!!");
        }
    });

    h.join().unwrap();

    //rx
}

fn scan_dir_progress(
    rx: mpsc::Receiver<FileScanStats>,
) -> Result<thread::JoinHandle<()>, io::Error> {
    let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {wide_msg}")
        .unwrap()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ");

    let h = thread::spawn(move || {
        let m = MultiProgress::new();

        let pb = m.add(ProgressBar::new(50));
        pb.set_style(spinner_style.clone());
        pb.set_prefix(format!("[1/2]"));

        {
            let mut files: u64 = 0;
            let mut dirs: u64 = 0;
            for msg in rx {
                if msg.is_file {
                    files += 1;
                } else {
                    dirs += 1;
                }

                pb.set_message(format!("{}: dirs:{} files:{}", "searching", dirs, files));
                pb.inc(1);
            }

            pb.finish_with_message(format!(
                "finished scanning ...found {} dirs, {} files",
                dirs, files
            ));
        }

        //m.clear().unwrap();
    });

    Ok(h)
}

#[derive(Parser)]
#[command(name = "ufc", version = "0.1", author = "Alexey Shvechkov")]
#[command(about = "ufc (unique file copy) - Copies unique files from src_dir to dst_dir. Destination has k/v store containing pairs md5<->path", long_about = None)]
struct Args {
    /// source directory with potential duplicates
    #[arg(short, long)]
    src_dir: Option<String>,

    /// destination directory where unique files will be stored
    #[arg(short, long)]
    dst_dir: Option<String>,

    /// simulate work w/o moving data + show stats
    #[arg(short = 'r', long)]
    dry_run: Option<bool>,

    /// dump additional info on the screen
    #[arg(short, long)]
    verbose: Option<bool>,

    /// **TBD** keep previous versions of the file (in case of overwrites)
    #[arg(short, long)]
    keep_copy: Option<bool>,

    /// allow overwriting files in destination dir. By default no overwrite will happen
    #[arg(short, long)]
    overwrite: Option<bool>,

    /// add prefix checksum  into destination file name (i.e. creates unique file name)
    #[arg(long)]
    name_prefix_checksum: Option<bool>,

    /// copies only files with names matching regular exp. E.g --name ".jpg|.png|.gif"
    #[arg(short, long)]
    name: Option<String>,

    /// copies only images: jpg/gif/png/...
    #[arg(short, long)]
    images_only: Option<bool>,

    /// show progress ...
    #[arg(short, long)]
    progress: Option<bool>,

    /// multithreaded
    #[arg(short, long)]
    mt: Option<bool>,

    /// threads number
    #[arg(short, long)]
    tn: Option<u32>,
}

fn main() {
    let src_dir;
    let dst_dir;

    let cli = Args::parse();

    match cli.src_dir {
        Some(x) => src_dir = x,
        None => exit(help()),
    }

    match cli.dst_dir {
        Some(x) => dst_dir = x,
        None => exit(help()),
    }

    //println!("src_dir: {src_dir}   dst_dir: {dst_dir}");

    if !Path::new(&dst_dir).exists() {
        eprintln!("Err: looks like destination dir does not exist?");
        exit(-1);
    }

    // Configure k/v store
    let mut cfg = Config::new(dst_dir.clone() + "/" + ".kv.db");
    // Create a directory inside of `std::env::temp_dir()`
    let tmp_dir = TempDir::new().expect("could not create temp dir for dry run stats");

    match cli.dry_run {
        Some(true) => {
            let mut tmp_db_path = String::from_str(tmp_dir.path().to_str().unwrap()).unwrap();
            tmp_db_path += "/.kv.db";
            //println!("Stats will go into {tmp_db_path}");
            cfg = Config::new(tmp_db_path);
        }
        Some(false) => {}
        None => {}
    }

    // Open the key/value store
    let store = Store::new(cfg).unwrap();

    let mut files: Vec<DirEntry> = Vec::new();

    let mut args = ScanDirArgs {
        dst_dir: String::from(dst_dir.clone()),
        src_dir: String::from(src_dir.clone()),
        //store: store,
        dry_run: cli.dry_run.is_some_and(|v| v),
        verbose: cli.verbose.is_some_and(|v| v),
        overwrite: cli.overwrite.is_some_and(|v| v),
        keepcopy: cli.keep_copy.is_some_and(|v| v),
        name_prefix_checksum: cli.name_prefix_checksum.is_some_and(|v| v),
        name: None,
        bytes_created: 0,
        dirs_scanned: 0,
        duplicates_no: 0,
        duplicates_total_size: 0,
        files_scanned: 0,
        show_progress: cli.progress.is_some_and(|v| v),
        mt: cli.mt.is_some_and(|v| v),
        threads_num: 0,
    };

    if args.mt {
            match cli.tn{
                Some(x) => args.threads_num = x,
                None => args.threads_num = num_cpus::get() as u32, // get CPU numbers
            }
    }

    match cli.name {
        Some(x) => {
            let re = Regex::new(&x.to_lowercase()).unwrap();
            args.name = Some(re);
        }
        None => {
            //gif, jpg, jpeg, png, pdf, mp3, tif, tiff
            if cli.images_only.is_some_and(|v| v) {
                let x = String::from_str(r#"\.gif$|\.jpg$|\.jpeg$|\.png$|\.tif$|\.tiff$|\.webp$|\.psd$|\.raw$|\.bmp$|\.heif$|\.indd$|\.svg$|\.ai$|\.eps$"#).unwrap();
                let re = Regex::new(&x).unwrap();
                args.name = Some(re);
            }
        }
    }

    let (tx, rx) = mpsc::channel::<FileScanStats>();

    let h = scan_dir_progress(rx).unwrap();

    match scan_dir(&mut args, &mut files, &src_dir, tx) {
        Err(e) => println!("{e}"),
        Ok(_) => (),
    }
    h.join().unwrap();

    process_files(&store, &files, &mut args);

    println!("****** STATS ******* ({})", store.path().unwrap().display());

    println!("\tdirs scanned :{} ", args.dirs_scanned);
    println!("\tfiles scanned :{} ", args.files_scanned);
    println!("\tduplicates :{} ", args.duplicates_no);
    println!(
        "\tduplicates size :{} ",
        bytes2human(args.duplicates_total_size)
    );
    println!("\tcreated :{} ", bytes2human(args.bytes_created));

    if args.dry_run {
        println!(
            "\t(** Stats from temp file {} - will be deleted)",
            store.path().unwrap().display()
        );
    }
}

fn bytes2human(n: u64) -> String {
    let byte = Byte::from_bytes(u128::from(n));
    let adjusted_byte = byte.get_appropriate_unit(false);

    adjusted_byte.format(1)
}
