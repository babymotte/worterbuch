# Wörterbuch TCP Protocol

Wörterbuch follows a client/server model. All data is stored on the server, clients can manipulate or query that data, very similar to how a database works. The server exposes a TCP port clients can connect to. Optionally the server can also provide a WebSocket to which clients can connect, e.g. because their security policy does not allow raw TCP connections (e.g. web applications).

Within a SESSION a client can make an arbitrary number of GET, PGET, SET, SUBSCRIBE and PSUBSCRIBE requests to query or update data on the server.

## Term definitions

- MESSAGE: a set of bytes transferred between the client and the server. Every message must follow the specifications defined in [Message Format](#message-format)
- CLIENT MESSAGE: a MESSAGE sent to the server by the client. Can be GET, SET or SUBSCRIBE:
  - GET: a CLIENT MESSAGE that contains a unique TRANSACTION ID and a KEY.
  - PGET: a CLIENT MESSAGE that contains a unique TRANSACTION ID and a REQUEST PATTERN.
  - SET: a CLIENT MESSAGE that contains a unique TRANSACTION ID and at least one KEY/VALUE pair.
  - SUBSCRIBE: a CLIENT MESSAGE that contains a unique TRANSACTION ID and a KEY.
  - PSUBSCRIBE: a CLIENT MESSAGE that contains a unique TRANSACTION ID and a REQUEST PATTERN.
- SERVER MESSAGE: a MESSAGE sent to the client by the SERVER. Can be STATE, ACK, ERR or EVENT:
  - STATE: a SERVER MESSAGE that transports exactly one KEY/VALUE pair from the server to the client. This will be either a response to a GET or the result of a SET message if there are KEY SUBSCRIPTIONs matching the SET key. Immediately after a SUBSCRIPTION is made an according STATE event will be issued containing all matching KEY/VALUE pairs, if any.
  - PSTATE: a SERVER MESSAGE that transports at least one KEY/VALUE pair from the server to the client. This will be either a response to a PGET message or the result of a SET message if there are PATTERN SUBSCRIPTIONs matching the SET key. Immediately after a PATTERN SUBSCRIPTION is made an according STATE event will be issued containing all matching KEY/VALUE pairs, if any.
  - ACK: a SERVER MESSAGE that is a response to a SET, SUBSCRIBE or PSUBSCRIBE message containing the original message's TRANSACTION ID.
  - ERR: a server message that is a response to a GET, SET or SUBSCRIBE message and indicates that something went wrong on the server side. It contains the original message's TRANSACTION ID, an ERROR CODE and some ERROR CODE specific metadata.
- ERROR CODE: an unsigned byte used to identify a specific server side error, see [Errors](#errors)
- SESSION: a session is initiated by the client implicitly by connecting to the server's TCP port. The session implicitly ends with the client disconnecting from the server.
- REQUEST PATTERN: either a concrete KEY or a KEY that has some of its ELEMENTS replaced with WILDCARDs
- KEY: Wörterbuch is essentially a key/value map where the keys have an hierachical order, comparable to topics in MQTT. They are composed of multiple ELEMENTS separated by a SEPARATOR. Every key must start and end with an ELEMENT, SEPARATORS at the beginning or end of a key are not allowed
- VALUE: an arbitrary UTF-8 string used as value in Wörterbuch's key/value store.
- (KEY) ELEMENT: a UTF-8 string representing a single hierarchical level of a KEY. ELEMENTS may contain any UTF-8 characters except WILDCARDs and may by empty strings as long as they are not the first or the last element in the KEY.
- SEPARATOR: a single ASCII char that separates KEY ELEMENTS from one another. The default separator character is `/` however this can be changed to any other ASCII character in the configuration of the server. Keys must never start with or end with a separator.
- WILDCARD: can be either a SINGLE LEVEL WILDCARD or a MULTI LEVEL WILDCARD
- SINGLE LEVEL WILDCARD: a single ASCII char that can be used as a placeholder for any single KEY ELEMENT in a REQUEST PATTERN. The default character for a single level wildcard is `?` however this can be changed to any other ASCII character in the configuration of the server. A REQUEST PATTERN can contain any number of single level wildcards.
- MULTI LEVEL WILDCARD: a single ASCII character that can be used as a placeholder for any number of KEY ELEMENTs in a REQUEST PATTERN. The default character for a single level wildcard is `#` however this can be changed to any other ASCII character in the configuration of the server. A multi level wildcard can only be used at the very end of a REQUEST PATTERN, as a result a REQUEST PATTERN cannot contain more than one multi level wildcard.
- TRANSACTION ID: transaction IDs are used to match server messages to the client message that triggered them. Transaction ids used by the client must be unique within the current SESSION, i.e. for every message the client sends it must use a new transaction id. Uniqueness across SESSIONs or across multiple clients is not necessary. Transaction ids must always be an unsigned 64 Bit integer. A reasonable strategy for client implementations is to start with 0 in a new SESSION and simply count up for every message sent.
- SUBSCRIPTION: clients can receive asynchronous notifications from the server whenever certain VALUEs change on the server. This is done by sending a SUBSCRIBE message containing a REQUEST PATTERN. For every change of a VALUE whose KEY matches the subscription's REQUEST PATTERN the server will send an EVENT message containing the REQUEST PATTERN, the concrete KEY and the new VALUE to the client.

## Client Actions

### GET

A GET message is sent by the client to the server in order to query a value. It contains a TRANSACTION ID and a KEY. When the server receives a GET message it will look up the provided KEY's VALUE and send it back to the client in a STATE message using the GET message's TRANSACTION ID. GET messages are one shot actions, they will return a snapshot of the server's current state and never trigger more than one response message from the server. KEYs used in a GET request are not allowed to contain any wildcards.

### PGET

A PGET message is sent by the client to the server in order to query values. It contains a TRANSACTION ID and a REQUEST PATTERN. When the server receives a PGET message it will collect all its stored KEY/VALUE pairs whose KEY matches the REQUEST PATTERN and send them back to the client in a STATE message using the GET message's TRANSACTION ID. GET messages are one shot actions, they will return a snapshot of the server's current state and never trigger more than one response message from the server.

### SET

A SET message is sent by the client to the server in order to update a KEY's value or to insert a new KEY/VALUE pair into the server's store. It contains a TRANSACTION ID and a KEY and a VALUE. The server will update its internal store by adding the new KEY and VALUE or by updating the VALUE of the already existing KEY and then send back an ACK message to the client containing the SET message's TRANSACTION ID. SET messages are one shot actions and they will never trigger more than one response message from the server.


### SUBSCRIBE

A SUBSCRIBE message is sent by the client to the server in order to establish a SUBSCRIPTION. It contains a TRANSACTION ID and a KEY. The server will acknowledge the subscription by sending an ACK message containing the SUBSCRIBE message's TRANSACTION ID. It will then look up the contained KEY's VALUE and send a STATE message to the client, containing the SUBSCRIPTION's REQUEST PATTERN, the KEY, the VALUE and the SUBSCRIBE message's TRANSACTION ID. Then it will register an internal listener that watches for any SET messages whose KEY matches the SUBSCRIPTION's REQUEST PATTERN and for each send an additional EVENT message containing the SUBSCRIPTION's REQUEST PATTERN, the SET message's KEY and VALUE and the SUBSCRIBE message's TRANSACTION ID.

A SUBSCRIPTION is a long term contract between the client and the server causing the server to send any number of EVENT messages to the client and the SUBSCRIBE message's TRANSACTION ID will be included in every EVENT message resulting from it.

Currently there is no other way to stop an ongoing SUBSCRIPTION other than closing the connection. This may be added at a later time if use cases arise.

### PSUBSCRIBE

A SUBSCRIBE message is sent by the client to the server in order to establish a SUBSCRIPTION. It contains a TRANSACTION ID and a REQUEST PATTERN. The server will acknowledge the subscription by sending an ACK message containing the SUBSCRIBE message's TRANSACTION ID. It will then collect any KEY/VALUE pairs from its store whose KEY matches the REQUEST PATTERN and for each of them send an EVENT message to the client, containing theSUBSCRIPTION's REQUEST PATTERN, the KEY, the VALUE and the SUBSCRIBE message's TRANSACTION ID. Then it will register an internal listener that watches for any SET messages whose KEY matches the SUBSCRIPTION's REQUEST PATTERN and for each send an additional EVENT message containing the SUBSCRIPTION's REQUEST PATTERN, the SET message's KEY and VALUE and the SUBSCRIBE message's TRANSACTION ID.

A SUBSCRIPTION is a long term contract between the client and the server causing the server to send any number of EVENT messages to the client and the SUBSCRIBE message's TRANSACTION ID will be included in every EVENT message resulting from it.

Currently there is no other way to stop an ongoing SUBSCRIPTION other than closing the connection. This may be added at a later time if use cases arise.
  
## Message Format

Wörterbuch uses bidirectional TCP and/or WebSocket streams for server/client communication. Messages are byte blobs sent across those streams. While TCP itself has no concept of messages, WebSockets do, so in order to save bandwidth the WebSocket protocol uses a different message format with smaller headers.

A Wörterbuch message is composed of a header and a payload. The header's purpose is to define how long the message is and which part of the payload is to be found where in the message. Its layout differs between message types.

#### Byte 0

The first byte of a message encodes its message type:
```
0000 0000 = GET
0000 0001 = SET
0000 0010 = SUBSCRIBE
0000 0011 = PGET
0000 0100 = PSUBSCRIBE

1000 0000 = PSTATE
1000 0001 = ACK
1000 0010 = STATE
1000 0011 = ERR
```
The remaining variants are left free for potential future extension of the protocol.

#### Bytes 1 - 8

The next eight bytes represent a 64 Bit unsigned integer which is the TRANSACTION ID.

The rest of the header differs between message types:

### GET

#### Bytes 9 & 10

These are a 16 Bit unsigned integer designating the length of the GET message's REQUEST PATTERN, which is also the entire payload.

This means that a request pattern cannot be larger than 64 KiB. GET messages with a larger REQUEST PATTERN must be rejected by the client, e.g. by throwing an error.

#### Bytes 11 - n

These are the payload of the message, containing one UTF-8 encoded string, being the REQUEST PATTERN, of the previously specified length.

### SET

#### Bytes 9 & 10

These are a 16 Bit unsigned integer designating the length of the SET message's KEY.

This means that a KEY cannot be larger than 64 KiB. SET messages with a larger KEY must be rejected by the client, e.g. by throwing an error.

#### Bytes 11 to 14

These are a 32 Bit unsigned integer designating the length of the SET message's VALUE.

This means that a VALUE cannot be larger than 4 GiB. SET messages with a larger VALUE must be rejected by the client, e.g. by throwing an error.

#### Bytes 15 to N

These are the payload of the message, which is two UTF-8 encoded Strings, the first being the KEY, the second the VALUE, of the previously specified lengths.

### SUBSCRIBE

#### Bytes 9 to 10

These are a 16 Bit unsigned integer designating the length of the GET message's REQUEST PATTERN, which is also the entire payload.

This means that a request pattern cannot be larger than 64 KiB. GET messages with a larger REQUEST PATTERN must be rejected by the client, e.g. by throwing an error.

#### Bytes 11 to N

These are the payload of the message, containing one UTF-8 encoded string, being the REQUEST PATTERN, of the previously specified length.

### STATE

#### Bytes 9 & 10

These are a 16 Bit unsigned integer designating the length of the REQUEST PATTERN that triggered this message.

For later calculation let's assume the length of the REQUEST PATTERN is X.

#### Bytes 11 to 14

These are a 32 Bit unsigned integer specifying the number of KEY/VALUE pairs contained in the message.

If a GET request would result in more KEY/VALUE pairs being returned than can be encoded with 32 Bits an ERR message must be sent instead containing the appropriate ERROR CODE to inform the client that it must make a more specific request.

The number of contained KEY/VALUE pairs has influence on the contents of the remaining header, so in order to be able to address the bytes accordingly, let's assume there are N KEY/VALUE pairs in the message.

#### Bytes 15 to 6×N

For each KEY/VALUE pair there is a six byte block where the first two bytes are a 16 Bit unsigned integer designating the length of the KEY and the remaining four bytes are a 32 Bit unsigned integer designating the length of the VALUE.

#### Bytes 6×N + 1 to 6×N + 1 + X

This is the first part of the payload, a UTF-8 encoded string being the REQUEST PATTERN.

#### The Rest

The remaining part of the payload are the KEY/VALUE pairs, UTF-8 encoded strings in the same sequence as their lengths were specified in the header.

### ACK

ACK messages only have a type and a TRANSACTION ID, there is nothing more in the header and they don't have a payload.

### EVENT

#### Bytes 9 & 10

These are a 16 Bit unsigned integer designating the length of the REQUEST PATTERN that triggered this message.

#### Bytes 11 & 12

These are a 16 Bit unsigned integer designating the length of the KEY contained in this message.

#### Bytes 13 to 16

These are a 32 Bit unsigned integer designating the length of the VALUE contained in this message.

#### Bytes 17 to N

The payload, which is three UTF-8 encoded string, the first being the REQUEST PATTERN, the second being the KEY and the third the VALUE, all according to the lengths defined in the header.

### ERR

#### Byte 9

The ERROR CODE, see [Error](#error)

#### Bytes 10 to 13

These are a 32 Bit unsigned integer designating the length of the metadata contained in this message.

#### Bytes 14 to N

The payload, being an UTF-8 encoded string of the length specified in the header.

## Errors

// TODO