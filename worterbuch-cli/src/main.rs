fn main() {
    dotenv::dotenv().ok();
    let mut counter = 0;

    for line in io::stdin().lock().lines() {
        let line = line?;
        let mut split = line.split(":");
        let key = split.next().expect("no key provided");
        let value = split
            .next()
            .expect(&format!("no value provided for key {key}"));
        con.set(key, value)?;
    }
    Ok(())
}
