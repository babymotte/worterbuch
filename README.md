# Wörterbuch

Wörterbuch is an in-memory key/value store that solves a use case somewhere between a database and a message broker. It's specifically designed to serve as a batteries included web server and backend data store for smaller web applications or home automation systems.
You can think of it as an alternative to MQTT where you can simply look up messages without having to make a subscription or an alternative to Redis where any SET operation automatically produces a pub/sub message with the key as channel and value as message.

Keys are hierarchical paths in Wörterbuch, similarly to topics in MQTT. The default separator is a `/`, so a key looks something like `some/worterbuch/key`. Values can be retrieved by looking up the key or subscribing to it. In both cases wildcards are supported. The default single level wildcard is `?`, the default multi level wildcard is `#` (all of these can be configured).

Getting `my/key/#` would return a map of all key/value pairs where the key starts with `my/key/`, subscribing to `my/key/#` will produce events for each key/value pair already in store where the key starts with `my/key/` and subsequently an additional event for each SET operation for a key that starts with `my/key/`.

## Comparison with similar solutions


|                              | Wörterbuch | Redis    | MQTT     | Kafka | SQL | CouchDB*** |
| ---------------------------- | ---------- | -------- | -------- | ----- | --- | ------- |
| Publish/Subscribe            | Yes        | Optional | Yes      | Yes   | No  | ?       |
| Query                        | Yes        | Yes      | No       | No    | Yes | Yes     |
| JOINed Queries*              | Yes        | No       | No       | No    | Yes | Yes     |
| GraphQL-like queries**       | Planned    | No       | No       | No    | No  | No      |
| Delete                       | Yes        | Yes      | Yes      | No    | Yes | Yes     |
| In-memory store              | Yes        | Yes      | Yes      | No    | No  | No      |
| Persistent store             | Optional   | Optional | Optional | Yes   | Yes | Yes     |
| TCP API                      | Yes        | Yes      | Yes      | Yes   | Yes | Yes     |
| WebSocket API                | Yes        | No       | Optional | No    | No  | ?       |
| HTTP API                     | Yes        | No       | No       | No    | No  | Yes     |
| Unix Domain Socket API       | Yes        | No       | No       | No    | No  | No      |
| Built in user management     | No         | Yes      | Yes      | Yes   | Yes | Yes     |
| Token based authorization    | Yes        | ?        | No       | No    | No  | ?       |
| Accessible from web frontend | Yes        | No       | No       | No    | No  | Yes     |

*&nbsp;Queries across different data sources (e.g. tables, topics, keys, etc.) that produce a single result containing data from those data sources

**&nbsp;Queries where the expected result data structure is encoded in the request

***&nbsp;My experience with CouchDB is very limited so please forgive mistakes here. I'm happy to correct any that are pointed out to me

## License
Wörterbuch is Free and Open Source Software available under the [GNU Affero General Public License v3 or later](https://www.gnu.org/licenses/agpl-3.0.html). Proprietary commercial licenses for use in non-free software products are also available, please feel free to contact me, if you are interested, I will forward your request to my sales partner.

## Contributing
Because Wörterbuch is available under both FOSS and proprietary commercial licenses, contributions are tricky to handle without breaking some country's copyright laws. Therefore unfortunately we cannot currently accept any unsolicited pull requests. If you really want to be part of the project, feel free to contact me, maybe we can figure something out.

Of course you always have the option of forking the repository and publishing your own modified version under the AGPLv3-or-later license.
