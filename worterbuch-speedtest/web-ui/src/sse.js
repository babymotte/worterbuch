import React from "react";

export function useSse(path, callback) {
  React.useEffect(() => {
    console.log("Subscribing to event source", path);
    const evtSource = new EventSource(path);
    // const evtSource = new EventSource("http://localhost:4000/throughput/stats");
    evtSource.onmessage = (event) => {
      callback(JSON.parse(event.data));
    };
    evtSource.onerror = (err) => {
      console.error("EventSource failed:", err);
    };

    return () => {
      console.log("Unsubscribing from event source", path);
      evtSource.close();
    };
  }, [callback, path]);
}
