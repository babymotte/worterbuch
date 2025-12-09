use hashbrown::HashMap;
use random_word::Lang;
use serde_json::{Value, json};
use std::sync::mpsc;

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

impl KeyValueConsumer for tokio::sync::mpsc::Sender<(Vec<String>, Value)> {
    type Error = tokio::sync::mpsc::error::SendError<(Vec<String>, Value)>;
    fn accept(&mut self, key: Vec<String>, values: Value) -> Result<(), Self::Error> {
        self.blocking_send((key, values))
    }
}

pub fn generate_dummy_data(
    n_ary_keys: usize,
    key_length: u32,
    values_per_key: usize,
) -> HashMap<Vec<String>, Value> {
    let mut data = HashMap::new();
    let mut n: Vec<usize> = vec![0; key_length as usize];
    let mut prefix = Vec::new();
    'outer: while n[prefix.len()] < n_ary_keys {
        while prefix.len() < key_length as usize {
            let segment = random_word::get(Lang::En);
            prefix.push(segment);
        }

        let path: Vec<String> = prefix.iter().map(ToString::to_string).collect();
        for _ in 0..values_per_key {
            data.insert(path.clone(), json!(random_word::get(Lang::En)));
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

    data
}
