import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.jsx";
import Theme from "./Theme.jsx";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <Theme>
      <App />
    </Theme>
  </React.StrictMode>
);
