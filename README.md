# Wörterbuch

Wörterbuch is an in-memory key/value store that solves a use case somewhere between a database and a message broker. It's specifically designed to serve as a batteries included web server and backend data store for smaller web applications or home automation systems.
You can think of it as an alternative to MQTT where you can simply look up messages without having to make a subscription or an alternative to Redis where any SET operation automatically produces a pub/sub message with the key as channel and value as message.

Keys are hierarchical paths in Wörterbuch, similarly to topics in MQTT. The default separator is a `/`, so a key looks something like `some/worterbuch/key`. Values can be retrieved by looking up the key or subscribing to it. In both cases wildcards are supported. The default single level wildcard is `?`, the default multi level wildcard is `#` (all of these can be configured).

Getting `my/key/#` would return a map of all key/value pairs where the key starts with `my/key/`, subscribing to `my/key/#` will produce events for each key/value pair already in store where the key starts with `my/key/` and subsequently an additional event for each SET operation for a key that starts with `my/key/`.
