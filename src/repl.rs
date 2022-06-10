use crate::worterbuch::Worterbuch;
use anyhow::{Context, Result};
use std::{
    io::{self, BufRead},
    path::Path,
    process,
    sync::Arc,
    thread,
};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        RwLock,
    },
};

const SET: &str = "set";
const GET: &str = "get";
const SUBSCRIBE: &str = "subscribe";
const LOAD: &str = "load";
const EXPORT: &str = "export";
const QUIT: &str = "quit";
const EXIT: &str = "exit";

pub async fn repl(worterbuch: Arc<RwLock<Worterbuch>>) {
    let (tx, mut rx) = unbounded_channel();
    thread::spawn(|| read(tx));

    log::info!("Starting REPL:");

    while let Some(line) = rx.recv().await {
        if let Err(e) = interpret(line, &worterbuch).await {
            log::error!("{e}: {}", e.root_cause());
        }
    }
}

fn read(tx: UnboundedSender<String>) -> Result<()> {
    for line in io::stdin().lock().lines() {
        let line = line.context("Failed read line.")?;
        if !line.trim().is_empty() {
            tx.send(line).context("Failed to send read line.")?;
        }
    }

    Ok(())
}

async fn interpret(line: String, worterbuch: &RwLock<Worterbuch>) -> Result<()> {
    let mut split = line.split(" ");

    match split.next() {
        Some(SET) => set(split, worterbuch).await,
        Some(GET) => get(split, worterbuch).await,
        Some(SUBSCRIBE) => subscribe(split, worterbuch).await,
        Some(LOAD) => load(split, worterbuch).await,
        Some(EXPORT) => export(split, worterbuch).await,
        Some(QUIT) | Some(EXIT) => quit(),
        Some(cmd) => {
            eprintln!("Unknown command: {cmd}");
            Ok(())
        }
        None => Ok(()),
    }
}

async fn set<'s>(
    mut split: impl Iterator<Item = &'s str>,
    worterbuch: &RwLock<Worterbuch>,
) -> Result<()> {
    let key = split.next();

    if key.is_none() {
        eprintln!("please specify a key");
        return Ok(());
    }

    let mut value = String::new();
    for part in split {
        value.push_str(part);
        value.push(' ');
    }

    let key = key.expect("we checked this for none").to_owned();
    let value = value.trim().to_owned();

    let mut wb = worterbuch.write().await;

    wb.set(key, value)?;

    Ok(())
}

async fn get<'s>(
    mut split: impl Iterator<Item = &'s str>,
    worterbuch: &RwLock<Worterbuch>,
) -> Result<()> {
    let key = split.next();

    if key.is_none() {
        eprintln!("please specify a key");
        return Ok(());
    }

    let key = key.expect("we checked this for none").to_owned();

    let wb = worterbuch.write().await;

    let result = wb.get_all(key)?;

    for val in result {
        eprintln!("{}={}", val.0, val.1);
    }

    Ok(())
}

async fn subscribe<'s>(
    _split: impl Iterator<Item = &'s str>,
    _worterbuch: &RwLock<Worterbuch>,
) -> Result<()> {
    todo!()
}

async fn load<'s>(
    mut split: impl Iterator<Item = &'s str>,
    worterbuch: &RwLock<Worterbuch>,
) -> Result<()> {
    let filename = split.next();

    if filename.is_none() {
        eprintln!("please specify a file to load");
        return Ok(());
    }

    let filename = filename.expect("we checked this for none").to_owned();
    let path = Path::new(&filename);
    let contents = fs::read_to_string(path).await?;

    log::debug!(
        "Read file {}",
        fs::canonicalize(&path).await?.as_os_str().to_string_lossy()
    );

    log::debug!("Acquiring write lock on store …");
    let mut wb = worterbuch.write().await;

    log::debug!("Done. Importing data …");
    let result = wb.import(&contents)?;
    eprintln!(
        "Imported {} key/value pairs from {}",
        result.len(),
        fs::canonicalize(&path).await?.as_os_str().to_string_lossy()
    );

    Ok(())
}

async fn export<'s>(
    mut split: impl Iterator<Item = &'s str>,
    worterbuch: &RwLock<Worterbuch>,
) -> Result<()> {
    let filename = split.next();

    if filename.is_none() {
        eprintln!("please specify a file to export to");
        return Ok(());
    }

    let filename = filename.expect("we checked this for none").to_owned();
    let path = Path::new(&filename);

    let wb = worterbuch.read().await;

    let json = wb.export()?;

    let mut file = File::create(path).await?;
    file.write_all(json.to_string().as_bytes()).await?;
    eprintln!(
        "Dumped all values to {}",
        fs::canonicalize(&path).await?.as_os_str().to_string_lossy()
    );

    Ok(())
}

fn quit() -> Result<(), anyhow::Error> {
    process::exit(0)
}
