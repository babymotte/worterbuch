# Wörterbuch

Wörterbuch is an in-memory key/value store that solves a use case somewhere between a database and a message broker. It's specifically designed to serve as a batteries included web server and backend data store for smaller web applications or home automation systems.
You can think of it as an alternative to MQTT where you can simply look up messages without having to make a subscription or an alternative to Redis where any SET operation automatically produces a pub/sub message with the key as channel and value as message.

Keys are hierarchical paths in Wörterbuch, similarly to topics in MQTT. The default separator is a `/`, so a key looks something like `some/worterbuch/key`. Values can be retrieved by looking up the key or subscribing to it. In both cases wildcards are supported. The default single level wildcard is `?`, the default multi level wildcard is `#` (all of these can be configured).

Getting `my/key/#` would return a map of all key/value pairs where the key starts with `my/key/`, subscribing to `my/key/#` will produce events for each key/value pair already in store where the key starts with `my/key/` and subsequently an additional event for each SET operation for a key that starts with `my/key/`.

## Comparison with similar solutions

|                              | Wörterbuch | Redis    | MQTT     | Kafka | SQL | CouchDB\*\*\* |
| ---------------------------- | ---------- | -------- | -------- | ----- | --- | ------------- |
| Publish/Subscribe            | Yes        | Optional | Yes      | Yes   | No  | ?             |
| Query                        | Yes        | Yes      | No       | No    | Yes | Yes           |
| JOINed Queries\*             | Yes        | No       | No       | No    | Yes | Yes           |
| GraphQL-like queries\*\*     | Planned    | No       | No       | No    | No  | No            |
| Delete                       | Yes        | Yes      | Yes      | No    | Yes | Yes           |
| In-memory store              | Yes        | Yes      | Yes      | No    | No  | No            |
| Persistent store             | Optional   | Optional | Optional | Yes   | Yes | Yes           |
| TCP API                      | Yes        | Yes      | Yes      | Yes   | Yes | Yes           |
| WebSocket API                | Yes        | No       | Optional | No    | No  | ?             |
| HTTP API                     | Yes        | No       | No       | No    | No  | Yes           |
| Unix Domain Socket API       | Yes        | No       | No       | No    | No  | No            |
| Built in user management     | No         | Yes      | Yes      | Yes   | Yes | Yes           |
| Token based authorization    | Yes        | ?        | No       | No    | No  | ?             |
| Accessible from web frontend | Yes        | No       | No       | No    | No  | Yes           |

\*&nbsp;Queries across different data sources (e.g. tables, topics, keys, etc.) that produce a single result containing data from those data sources

\*\*&nbsp;Queries where the expected result data structure is encoded in the request

\*\*\*&nbsp;My experience with CouchDB is very limited so please forgive mistakes here. I'm happy to correct any that are pointed out to me

## Versioning

There are four different things in Wörterbuch that are versioned:

1. the persistence schema
1. the client protocol
1. the cluster sync protocol
1. the application itself

### Persistence Schema

When persistence is enabled in Wörterbuch, it periodically writes the content of its data store to disk so it can be loaded again on the next restart. The schema that is used to write data to disk may change over time and must thus be versioned. Whenever the schema changes, a new loader mechanism is added so Wörterbuch retains the ability to read persistence files that were written using an older schema. The persistence schema only needs a single version number (`1`, `2`, `3`, etc.) since it is safe to assume that there won't be any kind of compatibility between them anyway.

### Client Protocol

In order for a client to talk to a Wörterbuch server, both the client and the server need to use compatible protocol versions. The client protocol is versioned using a two part version number (`<major>.<minor>`, e.g. `1.6`, `2.1`, etc.) where the minor version is incremented whenever new client and server messages are added or new optional fields are added to existing client or server mesages, the major version is incremented every time support for certain client or server messages is dropped, the server uses a different server message to respond to an existing client message, new non-optional fields are added to or removed from server or client messages or the type of a field in a client or server message changes, or if handshake and/or keepalive mechanisms change in some incompatible way.

The server implementes separate handlers for each major version it supports and each handler has a maximum minor version it supports. So if a client wants to connect to the server using protocol version `x.y`, the server will be able to handle it, if it has a handler for major version `x` and the maximum minor version that handler supports is equal to or greater than `y`.

As a part of its welcome message, the server will send a list of the versions, it supports, e.g. `[1.6, 2.1, 3.0]`. This implicitly means that the server supports all of these protocol versions:

- `1.0`
- `1.1`
- `1.2`
- `1.3`
- `1.4`
- `1.5`
- `1.6`
- `2.0`
- `2.1`
- `3.0`

The client can then decide if the major version it implements is included in that list and if the server's respective minor version is equal to or greater than the minor version the client implements. If that is the case - let's say the client implements `1.5` - the client should send a protocol selection request for the major version it implements (`1` in this example) to the server, otherwise (e.g. if the client implements `2.2`) it must disconnect. If the client does not send a protocol selection request, the server will assume the client implements the latest major version (`3` in this example).

Clients should only ever implement a single protocol version to make sure there is a clean mapping between the client's API and the respective protocol version, however it can be perfectly reasonable to put multiple client implementations for different protocol versions in the same library.

### Cluster Sync Protocol

The cluster sync protocol is much less nuanced, since the protocol is much less complex. It is also generally assumed that all nodes in a cluster will have the same version, however that assumption will not hold during updates (or rollbacks), where one node is updated (or rolled back) at a time to assure minimal downtime, so we cannot ditch versioning alltogether.

Similar to the persistence versioning schema, the cluster sync protocol uses a single version number that gets incremented whenever there is any kind of change in the sync protocol. In order to ensure compatibility, the server implements multiple handlers for different protocol versions and when two nodes connect, they perform a protocol negotiation.

### Application

The server application itself uses a three part version number (`<major>.<minor>.<patch>`, e.g. `1.2.4`, `3.1.5`, etc.).

The patch version is incremented whenever there is a mere optimization ot bug fix that does not require a change in persistence schema or communication protocols.

The minor version is incremented whenever support for a new persistence schema version, client protocol version or cluster sync protocol version is added.

The major version is incremented whenever support for a persistence schema version, client protocol version or cluster sync protocol version is dropped.

Each major version of the server application must support at least the latest persistence schema and cluster sync protocol versions plus additionaly the respective previous verions. This guarantees that when upgrading from one major version to the next there will never be any compatibility issues, as long as no major version is skipped (e.g. upgrading from `2.5.1` to `3.1.0` is safe, upgrading from `2.5.1` to `4.0.1` is NOT).

Note that this guarantee does NOT hold for rollbacks to the previous major version, since even though the server can read the previous persistence schema, it will only write in the current one, so it may be necessary to manually convert the persistence file to the previous schema.

## License

Wörterbuch is Free and Open Source Software available under the [GNU Affero General Public License v3 or later](https://www.gnu.org/licenses/agpl-3.0.html). Proprietary commercial licenses for use in non-free software products are also available, please feel free to contact me, if you are interested, I will forward your request to my sales partner.

## Contributing

Because Wörterbuch is available under both FOSS and proprietary commercial licenses, contributions are tricky to handle without breaking some country's copyright laws. Therefore we unfortunately cannot currently accept any unsolicited pull requests. If you want to contribute anyway, there are of course ways to do that without writing actual code: testing, benchmarking, profiling and sharing the results, offering useful advice and filing bug reports or feature requests are all very welcome.

If you really want to contribute code, please contact me first so we can discuss any potential licensing/copyright issues. And of course you always have the option of forking the repository and publishing your own modified version under the AGPLv3-or-later license.
