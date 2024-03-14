use std::sync::mpsc;

use random_word::Lang;
use serde_json::{json, Value};

pub trait KeyValueConsumer {
    type Error;
    fn accept(&mut self, key: Vec<String>, value: Value) -> Result<(), Self::Error>;
}

impl KeyValueConsumer for mpsc::SyncSender<(Vec<String>, Value)> {
    type Error = mpsc::SendError<(Vec<String>, Value)>;
    fn accept(&mut self, key: Vec<String>, values: Value) -> Result<(), Self::Error> {
        self.send((key, values))
    }
}

pub fn generate_dummy_data<E>(
    n_ary_keys: usize,
    key_length: u32,
    values_per_key: usize,
    mut consumer: impl KeyValueConsumer<Error = E>,
) -> Result<(), E> {
    let mut n = Vec::with_capacity(key_length as usize);
    for _ in 0..key_length {
        n.push(0);
    }
    let mut prefix = Vec::new();
    'outer: while n[prefix.len()] < n_ary_keys {
        while prefix.len() < key_length as usize {
            let segment = random_word::gen(Lang::En);
            prefix.push(segment);
        }

        let path: Vec<String> = prefix.iter().map(ToString::to_string).collect();
        for _ in 0..values_per_key {
            consumer.accept(path.clone(), json!(random_word::gen(Lang::En)))?;
        }

        prefix.pop();
        n[prefix.len()] += 1;
        while n[prefix.len()] >= n_ary_keys {
            n[prefix.len()] = 0;
            if prefix.pop().is_some() {
                n[prefix.len()] += 1;
            } else {
                break 'outer;
            }
        }
    }

    Ok(())
}
