import * as React from "react";
import { decode_server_message, encode_client_message } from "worterbuch-js";
import TopicTree from "./TopicTree";
import SortedMap from "collections/sorted-map";

export default function App() {
  const loc = window.location;
  let proto;
  if (loc.protocol === "https:") {
    proto = "wss";
  } else {
    proto = "ws";
  }
  const url = `${proto}://${loc.hostname}:${loc.port}/ws`;
  const topic = "#";
  const separator = "/";

  const dataRef = React.useRef(new SortedMap());
  const [data, setData] = React.useState(new SortedMap());

  React.useEffect(() => {
    const socket = new WebSocket(url);
    socket.onopen = (e) => {
      const subscrMsg = {
        pSubscribe: { transactionId: 1, requestPattern: topic },
      };
      const buf = encode_client_message(subscrMsg);
      socket.send(buf);
    };
    socket.onclose = (e) => {};
    socket.onmessage = async (e) => {
      const buf = await e.data.arrayBuffer();
      const uint8View = new Uint8Array(buf);
      const msg = decode_server_message(uint8View);
      if (msg.pState) {
        mergeKeyValuePairs(
          msg.pState.keyValuePairs,
          dataRef.current,
          separator
        );
        setData(new SortedMap(dataRef.current));
      }
    };
  }, [url]);

  return <div className="App">{<TopicTree data={data} />}</div>;
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
