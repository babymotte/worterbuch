use crate::error::{WorterbuchError, WorterbuchResult};
use inferno::flamegraph::{Direction, Palette, color::MultiPalette};
use mappings::MAPPINGS;
use pprof_util::{FlamegraphOptions, StackProfile, parse_jeheap};
use regex::Regex;
use std::{
    env,
    fs::File,
    io::{self, BufReader},
    path::PathBuf,
};
use tokio::fs;
use tracing::{Level, info, instrument};

#[instrument(level=Level::DEBUG, err)]
pub async fn get_live_heap_profile() -> WorterbuchResult<Vec<u8>> {
    let prof_ctl = if let Some(it) = jemalloc_pprof::PROF_CTL.as_ref() {
        it
    } else {
        return Err(WorterbuchError::FeatureDisabled(
            "jemalloc profiling is not enabled".to_owned(),
        ))?;
    };
    let mut prof_ctl = prof_ctl.lock().await;
    require_profiling_activated(&prof_ctl)?;
    let pprof = match prof_ctl.dump_pprof() {
        Ok(it) => it,
        Err(e) => {
            let meta = format!("error generating heap dump: {e}");
            return Err(WorterbuchError::IoError(
                io::Error::new(io::ErrorKind::Other, e),
                meta,
            ))?;
        }
    };

    Ok(pprof)
}

#[instrument(level=Level::DEBUG, err)]
pub async fn get_live_flamegraph() -> WorterbuchResult<String> {
    let prof_ctl = if let Some(it) = jemalloc_pprof::PROF_CTL.as_ref() {
        it
    } else {
        return Err(WorterbuchError::FeatureDisabled(
            "jemalloc profiling is not enabled".to_owned(),
        ))?;
    };
    let mut prof_ctl = prof_ctl.lock().await;
    require_profiling_activated(&prof_ctl)?;
    match prof_ctl.dump() {
        Ok(f) => {
            let dump_reader = BufReader::new(f);
            let profile = parse_jeheap(dump_reader, MAPPINGS.as_deref()).map_err(|e| {
                let meta = format!("error generating heap dump: {e}");
                WorterbuchError::IoError(io::Error::new(io::ErrorKind::Other, e), meta)
            })?;
            to_flame_graph(profile).await
        }
        Err(e) => {
            let meta = format!("error generating flame graph: {e}");
            return Err(WorterbuchError::IoError(
                io::Error::new(io::ErrorKind::Other, e),
                meta,
            ))?;
        }
    }
}

#[instrument(level=Level::DEBUG, err)]
pub async fn get_heap_profile_from_file(filename: &str) -> WorterbuchResult<Option<Vec<u8>>> {
    if let Some((dir, _)) = get_profile_dir() {
        let file = File::open(dir.join(filename)).map_err(|e| {
            WorterbuchError::IoError(e, format!("could not open profile file {filename}"))
        })?;
        let dump_reader = BufReader::new(file);
        let profile = parse_jeheap(dump_reader, MAPPINGS.as_deref()).map_err(|e| {
            WorterbuchError::IoError(
                io::Error::new(io::ErrorKind::InvalidData, e),
                "could not parse profile file".to_owned(),
            )
        })?;
        let pprof = profile.to_pprof(("inuse_space", "bytes"), ("space", "bytes"), None);
        Ok(Some(pprof))
    } else {
        Ok(None)
    }
}

#[instrument(level=Level::DEBUG, err)]
pub async fn get_flamegraph_from_file(filename: &str) -> WorterbuchResult<Option<String>> {
    if let Some((dir, _)) = get_profile_dir() {
        let file = File::open(dir.join(filename)).map_err(|e| {
            WorterbuchError::IoError(e, format!("could not open profile file {filename}"))
        })?;
        let dump_reader = BufReader::new(file);
        let profile = parse_jeheap(dump_reader, MAPPINGS.as_deref()).map_err(|e| {
            WorterbuchError::IoError(
                io::Error::new(io::ErrorKind::InvalidData, e),
                "could not parse profile file".to_owned(),
            )
        })?;

        let svg = to_flame_graph(profile).await?;

        Ok(Some(svg))
    } else {
        Ok(None)
    }
}

async fn to_flame_graph(profile: StackProfile) -> WorterbuchResult<String> {
    let mut opts = FlamegraphOptions::default();
    opts.title = "Wörterbuch Memory Flamegraph".to_string();
    opts.count_name = "bytes".to_string();
    opts.deterministic = true;
    opts.direction = Direction::Inverted;
    opts.colors = Palette::Multi(MultiPalette::Rust);
    let flamegraph = profile.to_flamegraph(&mut opts).map_err(|e| {
        WorterbuchError::IoError(
            io::Error::new(io::ErrorKind::InvalidData, e),
            "could not generate flamegraph".to_owned(),
        )
    })?;
    let svg = String::from_utf8_lossy(&flamegraph).to_string();
    Ok(svg)
}

#[instrument(level=Level::DEBUG)]
pub async fn list_heap_profile_files() -> Option<Vec<String>> {
    let (dir, prefix) = get_profile_dir()?;
    let mut files = vec![];
    let mut content = fs::read_dir(dir).await.ok()?;
    while let Ok(Some(file)) = content.next_entry().await {
        info!("{:?}", file);
        if let Ok(meta) = file.metadata().await {
            if meta.is_file() {
                let filename = file.file_name().to_string_lossy().to_string();
                if filename.starts_with(&prefix) && filename.ends_with(".heap") {
                    files.push(filename);
                }
            }
        }
    }
    Some(files)
}

fn get_profile_dir() -> Option<(PathBuf, String)> {
    let re = Regex::new(r".*prof_prefix:(.+)\/(.+)").ok()?;
    let env = env::var("MALLOC_CONF").ok()?;
    let path = re.captures(&env).iter().next().map(|c| {
        (
            PathBuf::from(c.extract::<2>().1[0]),
            c.extract::<2>().1[1].to_owned(),
        )
    });
    path.or_else(|| Some((env::current_dir().ok()?, "jeprof".to_owned())))
}

fn require_profiling_activated(prof_ctl: &jemalloc_pprof::JemallocProfCtl) -> WorterbuchResult<()> {
    if prof_ctl.activated() {
        Ok(())
    } else {
        Err(WorterbuchError::FeatureDisabled(
            "profiling is disabled".into(),
        ))
    }
}
