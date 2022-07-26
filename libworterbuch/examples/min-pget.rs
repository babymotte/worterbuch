use libworterbuch::{
    client::{config::Config, tcp},
    codec::KeyValuePair,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Config::new()?;

    let mut con = tcp::connect(&config.proto, &config.host_addr, config.port).await?;

    if let Some(values) = con.pget_values("hello/#").await? {
        for KeyValuePair { key, value } in values {
            println!("{key}={value}");
        }
    } else {
        eprintln!("[no value]");
    }

    Ok(())
}
