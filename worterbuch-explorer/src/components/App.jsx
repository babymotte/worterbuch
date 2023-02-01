import * as React from "react";
import TopicTree from "./TopicTree";
import SortedMap from "collections/sorted-map";

export default function App() {
  const [wbAddress, setWbAddress] = React.useState(
    window.localStorage?.getItem("worterbuch.address")
  );

  let url = wbAddress;

  if (!url || url.trim() === "") {
    const loc = window.location;
    let proto;
    if (loc.protocol === "https:") {
      proto = "wss";
    } else {
      proto = "ws";
    }
    url = `${proto}://${loc.hostname}:${loc.port}/ws`;
    setWbAddress(url);
  }

  React.useEffect(() => {
    if (window.localStorage) {
      window.localStorage.setItem("worterbuch.address", url);
    }
  }, [url]);

  const dataRef = React.useRef(new SortedMap());
  const [data, setData] = React.useState(new SortedMap());
  const [socket, setSocket] = React.useState();
  const multiWildcardRef = React.useRef();
  const separatorRef = React.useRef();

  React.useEffect(() => {
    const topic = multiWildcardRef.current;
    if (topic && socket) {
      dataRef.current = new SortedMap();
      const subscrMsg = {
        pSubscribe: { transactionId: 1, requestPattern: topic, unique: true },
      };
      socket.send(JSON.stringify(subscrMsg));
    }
  }, [socket]);

  React.useEffect(() => {
    console.log("Connecting to server.");
    const socket = new WebSocket(url);
    socket.onclose = (e) => {
      setSocket(undefined);
    };
    socket.onmessage = async (e) => {
      const msg = JSON.parse(e.data);
      if (msg.pState) {
        mergeKeyValuePairs(
          msg.pState.keyValuePairs,
          dataRef.current,
          separatorRef.current
        );
        setData(new SortedMap(dataRef.current));
      }
      if (msg.handshake) {
        console.log("Handshake complete.");
        setSocket(socket);
        separatorRef.current = msg.handshake.separator;
        multiWildcardRef.current = msg.handshake.multiWildcard;
      }
      if (msg.err) {
        const meta = JSON.parse(msg.err.metadata);
        window.alert(meta);
      }
    };
    socket.onopen = () => {
      console.log("Connected to server.");
      const handshake = {
        handshakeRequest: {
          supportedProtocolVersions: [{ major: 0, minor: 3 }],
          lastWill: [],
          graveGoods: [],
        },
      };
      socket.send(JSON.stringify(handshake));
    };
    return () => {
      console.log("Disconnecting from server.");
      socket.close();
    };
  }, [url]);

  function handleUrlInput(e) {
    if (e.key === "Enter") {
      setWbAddress(e.target.value);
    }
  }

  return (
    <div className="App">
      <nav className="AddressBar">
        <input type="text" defaultValue={url} onKeyDown={handleUrlInput} />
      </nav>
      <div className="spacer" style={{ padding: "4px" }} />
      <TopicTree data={data} separator={separatorRef.current} />
    </div>
  );
}

function mergeKeyValuePairs(kvps, data, separator) {
  for (const { key, value } of kvps) {
    const segments = key.split(separator);
    mergeIn(data, segments.shift(), segments, value);
  }
}

function mergeIn(data, headSegment, segmentsTail, value) {
  let child = data.get(headSegment);
  if (!child) {
    child = {};
    data.set(headSegment, child);
  } else {
  }

  if (segmentsTail.length === 0) {
    child.value = value;
  } else {
    if (!child.children) {
      child.children = new SortedMap();
    }
    mergeIn(child.children, segmentsTail.shift(), segmentsTail, value);
  }
}
