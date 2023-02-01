# Wörterbuch Protocol

Wörterbuch follows a client/server model. All data is stored on the server, clients can manipulate or query that data, very similar to how a database works. The server exposes a WebSocket endpoint clients can connect to. Optionally the server can also provide an http endpoint to which clients can send GET and POST requests in order to query and manipulate data.

Within a SESSION a client can make an arbitrary number of GET, PGET, SET, SUBSCRIBE and PSUBSCRIBE requests to query or update data on the server.

Upon connecting to the server the client sends a HANDSHAKE REQUEST. It contains the following information:
 - a list of the protocol verions the client supports
 - optionally a LAST WILL (KEY and VALUE) that the server will set when the client disconnects
 - optionally a list of GRAVE GOODS (KEYs or REQUEST PATTERNs) that the server will clear when the client disconnects
The Server must respond either with a HANDSHAKE ERROR if the server supports none of the protocol versions the client requested or with a HANDSHAKE RESPONSE containing the following:
 - the protocol version that will be used in this session. This must be the highest version that both server and client suppport
 - the character the server uses as WILDCARD
 - the character the server uses as MULTI LEVEL WILDCARD
 - the character the server uses as SEPARATOR

## Term definitions

- MESSAGE: a JSON string transferred between the client and the server. Every message must follow the specifications defined in [Message Format](#message-format)
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
- TRANSACTION ID: transaction IDs are used to match server messages to the client message that triggered them. Transaction ids used by the client must be unique within the current SESSION, i.e. for every message the client sends it must use a new transaction id. Uniqueness across SESSIONs or across multiple clients is not necessary. Transaction ids must always be an unsigned 64 Bit integer. A reasonable strategy for client implementations is to start with 1 in a new SESSION and simply count up for every message sent. Handshake messages (both client and server) must always have the transaction ID 0.
- SUBSCRIPTION: clients can receive asynchronous notifications from the server whenever certain VALUEs change on the server. This is done by sending a SUBSCRIBE message containing a REQUEST PATTERN. For every change of a VALUE whose KEY matches the subscription's REQUEST PATTERN the server will send an EVENT message containing the REQUEST PATTERN, the concrete KEY and the new VALUE to the client.
- LAST WILL: clients can specify a last will message that the server will publish when the client disconnects. This can be useful to detect vital services being offline 
- GRAVE GOODS: KEYs or REQUEST PATTERNs that will be 'buried' along with the client, i.e. if the client disconnects, matching keys will be cleared.

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

Wörterbuch uses bidirectional WebSocket streams for server/client communication. Messages are JSON strings sent across those streams.

// TODO document JSON message formats