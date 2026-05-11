# Worterbuch Proxy Mode

## Purpose

Proxy mode will be an additional cluster operting mode along leader and follower modes where:

- a proxy instance is not officially part of a cluster configuration, i.e. there can be an unspecified number of proxy instances and instances can come and go dynamically
- proxy instances do not take part in the cluster leader election
- proxy instances connect to and sync with the leader similarly to how a follower does it, but other than a follower they accept client connections and do not write a persistence file
- any WRITE operation (e.g. set, publish, cset, delete, lock, acquirelock etc.) that is sent by a client to a proxy instance is forwarded to the leader and the leader processes it as if it had come from a client
- any READ operation (get, pget, cget, ls, subscribe, psubscribe, etc.) that is sent by a client to a proxy instance is processed on the proxy directly and an according response is sent to the client
- if the connection to the leader breaks, the proxy will re-connect to the new leader once it is elected, receive its entire new state, generate a diff to its cached state of the previous leader and send any differences to subscribed keys to its connected clients as state/pstate messages
- clients can just stay connected to their proxy instance and are guaranteed to retain a state that is consistent with the new leader

## Implications that need consideration

### Leader change

It is probably a good idea to add a message to the protocol that informs clients of the leader change in case they need to react to it in some way

### Locks

Proxy instances that just re-connected to a new leader must try to re-acquire any locks any of their clients previously held. However there can be no guarantee that the lock is given to the same client as before if there are multiple clients contending for the same lock. As such a new message needs to be added that informs a client that a previously held lock has been lost.

### Grave Goods/Last Wills

Normally grave goods and last wills are stored in the persistence file so that in case of a server restart (which implies that all client connections break) they can all be triggered before the server accepts any new connections. In a scenario where there are proxy instances a server restart does not necessarily mean that any client connections break, so triggering grave goods and las wills on the new leader is probably not desired. It would probably make sense to add a config entry that allows skipping the trigger of grave goods and last wills on server start iff the server is starting in leader mode.

I'm also considering making grave goods and last wills a first class citizen in the client protocol since a breaking change in the client protocol will be introduced by the locking API anyway
