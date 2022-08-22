import React from "react";
import ReactDOM from "react-dom/client";
import { useEffect, useState } from "react";
import init, {
  encode_client_message,
  decode_server_message,
} from "worterbuch-wasm";

async function start() {
  const res = await init();
  console.log("init", res);
  const root = ReactDOM.createRoot(document.getElementById("root"));
  root.render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
  );
}

start();

const url = "ws://localhost:80/ws";
const topic = "hello/world";

function App() {
  const [connected, setConnected] = useState(false);
  const [value, setValue] = useState();

  useEffect(() => {
    const socket = new WebSocket(url);
    socket.onopen = (e) => {
      setConnected(true);
      const subscrMsg = { subscribe: { transactionId: 1, key: topic } };
      const buf = encode_client_message(subscrMsg);
      socket.send(buf);
    };
    socket.onclose = (e) => {
      setConnected(false);
    };
    socket.onmessage = async (e) => {
      const buf = await e.data.arrayBuffer();
      const uint8View = new Uint8Array(buf);
      const msg = decode_server_message(uint8View);
      if (msg.state) {
        setValue(msg.state.keyValue.value);
      }
    };
  }, []);

  const nconContent = (
    <>
      Could not connect to {url}
      <br />
      Make sure Wörterbuch is running at localhost:80 (which is the default port
      for websocket if not configured otherwise), e.g. by running 'worterbuch'.
      <p />
      If you haven't installed Wörterbuch yet, do so by running 'cargo install
      --path worterbuch &amp;&amp; cargo install --path worterbuch-cli' from the
      repository root.
      <p />
      Reload this page once Wörterbuch is running.
    </>
  );

  const conContent = (
    <>
      <div>
        Connected to {url}
        <br />
        Publish messages to topic {topic} (e.g. using 'wbset hello/world="Hello,
        there!"') to see live updates here:
        <p />
        {topic} = {value}
      </div>
    </>
  );

  const content = connected ? conContent : nconContent;

  return <div>{content}</div>;
}
