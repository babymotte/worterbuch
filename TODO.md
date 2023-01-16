- clear(key)/pclear(pattern)
  clear removes a single value for the provided key, pclear removes all values of keys matching the provided pattern

- publish(key,value)
  subscribers to key or a pattern matching key will be notified of the value but it won't be put into the store

- ls(key)
  get a list of all immediate child key segments, e.g.
  ```
  wbset rootkey/child1=1
  wbset rootkey/child2=2
  wbset rootkey/child2/subchild1=3
  wbls rootkey
  ```
  should output
  ```
  child1 child2
    ```
  similar to how `ls` lists all immediate children of the file system tree

- get rid of nonblocking decoder, messages should have known length
  length of the message must either be the first or second fixed size number in the packet or messages must be base64 encoded and delimited by a line break

- Protocol versions
  Servers should support multiple protocol versions. The list of supported versions is sent to the client with the handshake message. The client should respond, telling the server which protocol version it wishes to use

- keepalive messages to make sure client is still responding