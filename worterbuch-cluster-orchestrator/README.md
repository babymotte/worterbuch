# Wörterbuch Cluster Orchestrator

This is the orchestration layer of a Wörterbuch server cluster. It takes care of leader election and putting the node it manages in the correct mode as required.

The orchestrator offers the following endpoints (in addition to worterbuch's usual client end points):

- a UDP port through which it receives and sends the leader election and heartbeat messages to the other nodes
- a REST endpoint from which clients can request the current leader configuration

Wörterbuch can be started in two modes, leader or follower mode.

In leader mode it loads its state from disk on startup and offers its regular client API end points, plus an additional TCP socket to which followers connect to receive the leader's current state + any subsequent state changes for state replication.

In follower mode worterbuch does not load state from disk, instead it connects to the leader's TCP port and receives the leader's state through it. It also does not accept any client connection, but instead receives any state changes directly from the leader directly through the same TCP connection. If the TCP connection fails, the follower must immediately shut down.