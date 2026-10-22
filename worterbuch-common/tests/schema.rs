/*
 *  Worterbuch protocol schema tests
 *
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

//! Checks that the hand-written YAML schemas in `schema/` agree with the Rust protocol types, which are the
//! source of truth, and that the JSON fixtures in `schema/test/` are classified correctly by both.

#![allow(clippy::unwrap_used)]
#![allow(clippy::as_conversions)]

use jsonschema::Validator;
use schemars::schema_for;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};
use uuid::Uuid;
use worterbuch_common::{
    Protocol,
    protocol::v1::{
        Ack, AuthorizationRequest, CSet, CState, CStateEvent, ClientInfo, ClientMessage, Delete,
        Err, ErrorCode, Get, InternalAction, KeyValuePair, Lock, LockLost, Ls, LsState, Method,
        PDelete, PGet, PLs, PState, PStateEvent, PSubscribe, ProtocolSwitchRequest,
        ProtocolVersion, Publish, SPub, SPubInit, ServerInfo, ServerMessage, Set, State,
        StateEvent, Subscribe, SubscribeLs, Trace, Unsubscribe, UnsubscribeLs, Welcome,
    },
};

// ---------------------------------------------------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Kind {
    Client,
    Server,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Client => "client",
            Kind::Server => "server",
        }
    }

    fn yaml_schema(self) -> Value {
        let path = schema_dir().join(format!("{}.schema.yaml", self.name()));
        serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap()
    }

    fn rust_schema(self) -> Value {
        match self {
            Kind::Client => serde_json::to_value(schema_for!(ClientMessage)).unwrap(),
            Kind::Server => serde_json::to_value(schema_for!(ServerMessage)).unwrap(),
        }
    }

    fn fixtures(self, valid: bool) -> Vec<(PathBuf, Value)> {
        let dir = schema_dir().join("test").join(self.name()).join(if valid {
            "valid"
        } else {
            "invalid"
        });
        let mut fixtures: Vec<(PathBuf, Value)> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "json"))
            .map(|p| {
                let json = serde_json::from_str(&fs::read_to_string(&p).unwrap())
                    .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", p.display()));
                (p, json)
            })
            .collect();
        assert!(
            !fixtures.is_empty(),
            "no fixtures found in {}",
            dir.display()
        );
        fixtures.sort_by(|a, b| a.0.cmp(&b.0));
        fixtures
    }
}

fn schema_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("schema")
}

fn validator(schema: &Value) -> Validator {
    jsonschema::draft202012::new(schema).unwrap()
}

fn validation_errors(validator: &Validator, instance: &Value) -> Vec<String> {
    validator
        .iter_errors(instance)
        .map(|e| format!("{e} (at {})", e.instance_path()))
        .collect()
}

fn file_name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}

/// Top level message names (i.e. enum variant names as they appear on the wire) a schema accepts
fn message_names(schema: &Value) -> BTreeSet<String> {
    shape(schema).props.into_keys().collect()
}

// ---------------------------------------------------------------------------------------------------------------------
// fixtures vs. yaml schema
// ---------------------------------------------------------------------------------------------------------------------

fn check_fixtures_against_yaml_schema(kind: Kind) {
    let validator = validator(&kind.yaml_schema());
    let mut failures = vec![];

    for (path, json) in kind.fixtures(true) {
        let errors = validation_errors(&validator, &json);
        if !errors.is_empty() {
            failures.push(format!(
                "valid/{} is rejected: {errors:#?}",
                file_name(&path)
            ));
        }
    }

    for (path, json) in kind.fixtures(false) {
        if validator.is_valid(&json) {
            failures.push(format!("invalid/{} is accepted", file_name(&path)));
        }
    }

    assert!(
        failures.is_empty(),
        "{} fixtures do not match {}.schema.yaml:\n{}",
        kind.name(),
        kind.name(),
        failures.join("\n")
    );
}

#[test]
fn client_fixtures_match_yaml_schema() {
    check_fixtures_against_yaml_schema(Kind::Client);
}

#[test]
fn server_fixtures_match_yaml_schema() {
    check_fixtures_against_yaml_schema(Kind::Server);
}

// ---------------------------------------------------------------------------------------------------------------------
// fixtures vs. rust types
// ---------------------------------------------------------------------------------------------------------------------

fn check_valid_fixtures_against_rust<T: DeserializeOwned>(kind: Kind) {
    let validator = validator(&kind.rust_schema());
    let mut failures = vec![];

    for (path, json) in kind.fixtures(true) {
        if let Err(e) = serde_json::from_value::<T>(json.clone()) {
            failures.push(format!(
                "valid/{} cannot be deserialized: {e}",
                file_name(&path)
            ));
        }
        let errors = validation_errors(&validator, &json);
        if !errors.is_empty() {
            failures.push(format!(
                "valid/{} is rejected by the generated schema: {errors:#?}",
                file_name(&path)
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{} fixtures do not match the rust types:\n{}",
        kind.name(),
        failures.join("\n")
    );
}

#[test]
fn valid_client_fixtures_are_accepted_by_rust_types() {
    check_valid_fixtures_against_rust::<ClientMessage>(Kind::Client);
}

#[test]
fn valid_server_fixtures_are_accepted_by_rust_types() {
    check_valid_fixtures_against_rust::<ServerMessage>(Kind::Server);
}

#[test]
fn every_message_type_has_a_valid_fixture() {
    for kind in [Kind::Client, Kind::Server] {
        let covered: BTreeSet<String> = kind
            .fixtures(true)
            .into_iter()
            .flat_map(|(_, json)| {
                json.as_object()
                    .unwrap()
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect();
        let missing: Vec<String> = message_names(&kind.rust_schema())
            .difference(&covered)
            .cloned()
            .collect();
        assert!(
            missing.is_empty(),
            "{} message types without a valid fixture: {missing:?}",
            kind.name()
        );
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// serialized rust messages vs. yaml schema
// ---------------------------------------------------------------------------------------------------------------------

fn client_info() -> ClientInfo {
    ClientInfo {
        client_id: Uuid::new_v4(),
        protocol: Protocol::Tcp,
    }
}

fn traces() -> Vec<Trace> {
    let client_request = Trace::ClientRequest {
        client_info: client_info(),
        transaction_id: 1,
        method: Method::PSubscribe,
    };
    let cause = || Box::new(client_request.clone());
    vec![
        client_request.clone(),
        Trace::ProtocolSwitch {
            client_info: client_info(),
            protocol_version: 1,
        },
        Trace::InternalAction(InternalAction::ClientConnected(ClientInfo {
            client_id: Uuid::new_v4(),
            protocol: Protocol::Proxied(Box::new(Protocol::Ws)),
        })),
        Trace::InternalAction(InternalAction::ClientDisconnected(client_info())),
        Trace::InternalAction(InternalAction::SubscriptionsChanged { cause: cause() }),
        Trace::InternalAction(InternalAction::LocksChanged { cause: cause() }),
        Trace::InternalAction(InternalAction::ApplyingGraveGoods { cause: cause() }),
        Trace::InternalAction(InternalAction::ApplyingLastWill { cause: cause() }),
        Trace::InternalAction(InternalAction::Startup),
        Trace::InternalAction(InternalAction::LeaderSync),
        Trace::InternalAction(InternalAction::Shutdown),
    ]
}

fn client_messages() -> Vec<ClientMessage> {
    let key = || "hello/world".to_owned();
    let pattern = || "hello/?".to_owned();
    let lock = || Lock {
        transaction_id: 1,
        key: key(),
    };
    let get = || Get {
        transaction_id: 1,
        key: key(),
    };

    let mut messages = vec![
        ClientMessage::ProtocolSwitchRequest(ProtocolSwitchRequest { version: 1 }),
        ClientMessage::AuthorizationRequest(AuthorizationRequest {
            auth_token: "123456".to_owned(),
        }),
        ClientMessage::Get(get()),
        ClientMessage::CGet(get()),
        ClientMessage::PGet(PGet {
            transaction_id: 1,
            request_pattern: pattern(),
        }),
        ClientMessage::Set(Set {
            transaction_id: 1,
            key: key(),
            value: json!({"some": ["complex", 1, true, null]}),
        }),
        ClientMessage::CSet(CSet {
            transaction_id: 1,
            key: key(),
            value: json!(1),
            version: 2,
        }),
        ClientMessage::SPubInit(SPubInit {
            transaction_id: 1,
            key: key(),
        }),
        ClientMessage::SPub(SPub {
            transaction_id: 1,
            value: json!("value"),
        }),
        ClientMessage::Publish(Publish {
            transaction_id: 1,
            key: key(),
            value: json!(null),
        }),
        ClientMessage::Unsubscribe(Unsubscribe { transaction_id: 1 }),
        ClientMessage::Delete(Delete {
            transaction_id: 1,
            key: key(),
        }),
        ClientMessage::UnsubscribeLs(UnsubscribeLs { transaction_id: 1 }),
        ClientMessage::Lock(lock()),
        ClientMessage::AcquireLock(lock()),
        ClientMessage::ReleaseLock(lock()),
    ];

    // messages with optional fields, once with and once without them
    for some in [true, false] {
        let flag = some.then_some(true);
        messages.extend([
            ClientMessage::Subscribe(Subscribe {
                transaction_id: 1,
                key: key(),
                unique: flag,
                live_only: flag,
                send_traces: flag,
            }),
            ClientMessage::PSubscribe(PSubscribe {
                transaction_id: 1,
                request_pattern: pattern(),
                unique: flag,
                live_only: flag,
                send_traces: flag,
                aggregate_events: some.then_some(10),
            }),
            ClientMessage::PDelete(PDelete {
                transaction_id: 1,
                request_pattern: pattern(),
                quiet: flag,
            }),
            ClientMessage::Ls(Ls {
                transaction_id: 1,
                parent: some.then(key),
            }),
            ClientMessage::PLs(PLs {
                transaction_id: 1,
                parent_pattern: some.then(pattern),
            }),
            ClientMessage::SubscribeLs(SubscribeLs {
                transaction_id: 1,
                parent: some.then(key),
                send_traces: flag,
            }),
        ]);
    }

    messages
}

fn server_messages() -> Vec<ServerMessage> {
    let kvps = || {
        vec![
            KeyValuePair::of("hello/world", 1),
            KeyValuePair::of("hello/there", json!({"a": [1, 2]})),
        ]
    };

    let mut messages = vec![
        ServerMessage::Welcome(Welcome {
            info: ServerInfo::new(
                "1.7.0".to_owned(),
                vec![ProtocolVersion(0, 11), ProtocolVersion(1, 2)].into_boxed_slice(),
                true,
            ),
            client_id: Uuid::new_v4(),
        }),
        ServerMessage::Ack(Ack { transaction_id: 1 }),
        ServerMessage::Authorized(Ack { transaction_id: 0 }),
        ServerMessage::CState(CState {
            transaction_id: 1,
            event: CStateEvent {
                value: json!("value"),
                version: 3,
            },
        }),
        ServerMessage::LockLost(LockLost {
            transaction_id: 1,
            key: "hello/world".to_owned(),
        }),
    ];

    messages.extend(ErrorCode::ALL.into_iter().map(|error_code| {
        ServerMessage::Err(Err {
            transaction_id: 1,
            error_code,
            metadata: "something went wrong".to_owned(),
        })
    }));

    for trace in traces().into_iter().map(Some).chain([None]) {
        messages.extend([
            ServerMessage::State(State {
                transaction_id: 1,
                event: StateEvent::Value(json!(1)),
                trace: trace.clone(),
            }),
            ServerMessage::State(State {
                transaction_id: 1,
                event: StateEvent::Deleted(json!(1)),
                trace: trace.clone(),
            }),
            ServerMessage::PState(PState {
                transaction_id: 1,
                request_pattern: "hello/?".to_owned(),
                event: PStateEvent::KeyValuePairs(kvps()),
                trace: trace.clone(),
            }),
            ServerMessage::PState(PState {
                transaction_id: 1,
                request_pattern: "hello/?".to_owned(),
                event: PStateEvent::Deleted(kvps()),
                trace: trace.clone(),
            }),
            ServerMessage::LsState(LsState {
                transaction_id: 1,
                children: vec!["hello".to_owned(), "world".to_owned()],
                trace,
            }),
        ]);
    }

    messages
}

fn check_serialized_messages_against_yaml_schema<T: serde::Serialize>(kind: Kind, messages: &[T]) {
    let validator = validator(&kind.yaml_schema());
    let mut covered = BTreeSet::new();
    let mut failures = vec![];

    for msg in messages {
        let json = serde_json::to_value(msg).unwrap();
        covered.extend(json.as_object().unwrap().keys().cloned());
        let errors = validation_errors(&validator, &json);
        if !errors.is_empty() {
            failures.push(format!("{json}: {errors:#?}"));
        }
    }

    assert!(
        failures.is_empty(),
        "serialized {} messages are rejected by {}.schema.yaml:\n{}",
        kind.name(),
        kind.name(),
        failures.join("\n")
    );

    // make sure the samples above are updated when new message types are added
    let missing: Vec<String> = message_names(&kind.rust_schema())
        .difference(&covered)
        .cloned()
        .collect();
    assert!(
        missing.is_empty(),
        "no sample {} messages for: {missing:?}",
        kind.name()
    );
}

#[test]
fn serialized_client_messages_match_yaml_schema() {
    check_serialized_messages_against_yaml_schema(Kind::Client, &client_messages());
}

#[test]
fn serialized_server_messages_match_yaml_schema() {
    check_serialized_messages_against_yaml_schema(Kind::Server, &server_messages());
}

#[test]
fn error_code_schema_lists_all_error_codes() {
    let schema = serde_json::to_value(schema_for!(ErrorCode)).unwrap();
    let in_schema: BTreeSet<u64> = schema["enum"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    let deserializable: BTreeSet<u64> = (0..=u8::MAX)
        .filter(|c| serde_json::from_value::<ErrorCode>(json!(c)).is_ok())
        .map(u64::from)
        .collect();
    assert_eq!(
        in_schema, deserializable,
        "ErrorCode::ALL is missing variants"
    );
}

// ---------------------------------------------------------------------------------------------------------------------
// yaml schema vs. generated rust schema
// ---------------------------------------------------------------------------------------------------------------------

/// The shape of an object schema, with "exactly one of these properties" constraints (as produced by externally tagged
/// or flattened enums) collected into `one_of`, regardless of whether they are expressed as `oneOf` branches with
/// their own properties (schemars) or as `oneOf` branches that only list required properties (yaml schemas).
#[derive(Default)]
struct Shape {
    props: BTreeMap<String, Value>,
    required: BTreeSet<String>,
    one_of: BTreeSet<BTreeSet<String>>,
    /// non-object alternatives
    others: Vec<Value>,
}

impl Shape {
    fn into_value(self) -> Value {
        let mut object = Map::new();
        object.insert(
            "object".to_owned(),
            Value::Object(self.props.into_iter().collect()),
        );
        object.insert("required".to_owned(), json!(self.required));
        object.insert("oneOf".to_owned(), json!(self.one_of));
        let object = Value::Object(object);

        if self.others.is_empty() {
            object
        } else {
            let mut alternatives = self.others;
            alternatives.push(object);
            alternatives.sort_by_key(Value::to_string);
            json!({ "anyOf": alternatives })
        }
    }
}

/// Reduces a schema to a canonical form that only retains what is relevant for the wire format, so that two
/// schemas describing the same messages compare equal, even if they are structured differently. Descriptions,
/// non-numeric formats and length constraints are ignored, refs are inlined and optional fields (`Option<T>` in
/// rust) are treated as their inner type.
struct Normalizer<'a> {
    root: &'a Value,
    ref_stack: Vec<String>,
}

impl<'a> Normalizer<'a> {
    fn new(root: &'a Value) -> Self {
        Self {
            root,
            ref_stack: vec![],
        }
    }

    fn resolve(&self, reference: &str) -> &'a Value {
        let pointer = reference
            .strip_prefix('#')
            .unwrap_or_else(|| panic!("unsupported $ref: {reference}"));
        self.root
            .pointer(pointer)
            .unwrap_or_else(|| panic!("unresolvable $ref: {reference}"))
    }

    fn normalize(&mut self, schema: &'a Value) -> Value {
        let Some(obj) = schema.as_object() else {
            return match schema {
                Value::Bool(true) => json!("any"),
                Value::Bool(false) => json!("never"),
                _ => panic!("invalid schema: {schema}"),
            };
        };

        if let Some(reference) = obj.get("$ref").and_then(Value::as_str) {
            let name = reference.rsplit('/').next().unwrap().to_owned();
            if self.ref_stack.contains(&name) {
                return json!({ "recursive": name });
            }
            self.ref_stack.push(name);
            let normalized = self.normalize(self.resolve(reference));
            self.ref_stack.pop();
            return normalized;
        }

        if let Some(values) = obj.get("enum").and_then(Value::as_array) {
            let mut values = values.clone();
            values.sort_by_key(Value::to_string);
            return json!({ "enum": values });
        }

        if let Some(branches) = obj.get("anyOf").and_then(Value::as_array) {
            let non_null: Vec<&Value> = branches.iter().filter(|b| !is_null_type(b)).collect();
            if let [single] = non_null.as_slice() {
                return self.normalize(single);
            }
        }

        let ty = match obj.get("type") {
            Some(Value::String(ty)) => Some(ty.as_str()),
            Some(Value::Array(types)) => {
                let non_null: Vec<&str> = types
                    .iter()
                    .filter_map(Value::as_str)
                    .filter(|t| *t != "null")
                    .collect();
                match non_null.as_slice() {
                    [single] => Some(*single),
                    _ => panic!("unsupported type: {types:?}"),
                }
            }
            _ => None,
        };

        match ty {
            Some("integer") => {
                let bits = obj
                    .get("format")
                    .and_then(Value::as_str)
                    .map(|f| f.trim_start_matches(|c: char| c.is_alphabetic()));
                json!({ "integer": bits })
            }
            Some("number") | Some("string") | Some("boolean") | Some("null") => json!(ty),
            Some("array") => {
                if let Some(items) = obj.get("prefixItems").and_then(Value::as_array) {
                    json!({ "tuple": items.iter().map(|i| self.normalize(i)).collect::<Vec<_>>() })
                } else {
                    let items = obj.get("items").unwrap_or(&Value::Bool(true));
                    json!({ "array": self.normalize(items) })
                }
            }
            Some("object") => self.shape(schema).into_value(),
            Some(other) => panic!("unsupported type: {other}"),
            None if ["properties", "required", "oneOf", "anyOf", "allOf"]
                .iter()
                .any(|k| obj.contains_key(*k)) =>
            {
                self.shape(schema).into_value()
            }
            None => json!("any"),
        }
    }

    fn shape(&mut self, schema: &'a Value) -> Shape {
        let mut schema = schema;
        while let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            schema = self.resolve(reference);
        }

        let mut shape = Shape::default();

        if let Some(props) = schema.get("properties").and_then(Value::as_object) {
            for (name, prop) in props {
                let prop = self.normalize(prop);
                shape.props.insert(name.clone(), prop);
            }
        }
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            shape
                .required
                .extend(required.iter().map(|r| r.as_str().unwrap().to_owned()));
        }

        for branch in schema
            .get("allOf")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let branch = self.shape(branch);
            shape.props.extend(branch.props);
            shape.required.extend(branch.required);
            shape.one_of.extend(branch.one_of);
            shape.others.extend(branch.others);
        }

        for key in ["oneOf", "anyOf"] {
            for branch in schema
                .get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if is_null_type(branch) {
                    continue;
                }
                if !is_object_like(self.root, branch) {
                    let branch = self.normalize(branch);
                    shape.others.push(branch);
                    continue;
                }
                let branch = self.shape(branch);
                shape.props.extend(branch.props);
                if branch.one_of.is_empty() {
                    shape.one_of.insert(branch.required);
                } else {
                    for group in branch.one_of {
                        shape
                            .one_of
                            .insert(group.union(&branch.required).cloned().collect());
                    }
                }
                shape.others.extend(branch.others);
            }
        }

        shape
    }
}

fn is_null_type(schema: &Value) -> bool {
    schema.get("type").and_then(Value::as_str) == Some("null")
}

fn is_object_like(root: &Value, schema: &Value) -> bool {
    let mut schema = schema;
    while let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        schema = root.pointer(reference.trim_start_matches('#')).unwrap();
    }
    match schema.get("type") {
        Some(ty) => ty == "object",
        None => schema.get("properties").is_some() || schema.get("required").is_some(),
    }
}

fn shape(schema: &Value) -> Shape {
    Normalizer::new(schema).shape(schema)
}

fn check_yaml_schema_matches_rust_schema(kind: Kind) {
    let yaml = kind.yaml_schema();
    let rust = kind.rust_schema();

    let yaml_shape = Normalizer::new(&yaml).shape(&yaml);
    let rust_shape = Normalizer::new(&rust).shape(&rust);

    let yaml_names: BTreeSet<&String> = yaml_shape.props.keys().collect();
    let rust_names: BTreeSet<&String> = rust_shape.props.keys().collect();
    assert_eq!(
        yaml_names,
        rust_names,
        "{}.schema.yaml defines different message types than the rust code",
        kind.name()
    );

    let yaml_alternatives: BTreeSet<String> =
        yaml_shape.one_of.iter().map(|g| format!("{g:?}")).collect();
    let rust_alternatives: BTreeSet<String> =
        rust_shape.one_of.iter().map(|g| format!("{g:?}")).collect();
    assert_eq!(
        yaml_alternatives,
        rust_alternatives,
        "the top level oneOf in {}.schema.yaml does not list exactly one entry per message type",
        kind.name()
    );

    let mut failures = vec![];
    for (name, yaml_msg) in &yaml_shape.props {
        let rust_msg = &rust_shape.props[name];
        if yaml_msg != rust_msg {
            failures.push(format!(
                "{name}:\n  yaml: {}\n  rust: {}",
                serde_json::to_string(yaml_msg).unwrap(),
                serde_json::to_string(rust_msg).unwrap()
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{}.schema.yaml does not match the rust types:\n{}",
        kind.name(),
        failures.join("\n")
    );
}

#[test]
fn client_yaml_schema_matches_rust_schema() {
    check_yaml_schema_matches_rust_schema(Kind::Client);
}

#[test]
fn server_yaml_schema_matches_rust_schema() {
    check_yaml_schema_matches_rust_schema(Kind::Server);
}
