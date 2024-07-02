import React from "react";
import {
  RouterProvider,
  createBrowserRouter,
  useNavigate,
} from "react-router-dom";
import { usePersistedState } from "./utils";
import ThroughputTest from "./ThroughputTest";
import LatencyTest from "./LatencyTest";
import TabBar from "./TabBar";
import { Stack } from "@mui/material";

export const views = [
  ["/throughputTest", "Throughput Test", <ThroughputTest key="throughput" />],
  ["/latencyTest", "Latency Test", <LatencyTest key="latency" />],
];

function Redirect({ lastVisited }) {
  const navigate = useNavigate();
  React.useEffect(() => {
    navigate(lastVisited);
  }, [lastVisited, navigate]);
}

function TestView({ setLastVisited, view, tab }) {
  React.useEffect(() => {
    setLastVisited(view[0]);
  }, [setLastVisited, view]);
  return (
    <Stack padding={1}>
      <TabBar tab={tab} />
      {view[2]}
    </Stack>
  );
}

function App() {
  const [lastVisited, setLastVisited] = usePersistedState(
    "worterbuch.speedtest.visited.last",
    views[0][0]
  );

  const paths = views.map((v, i) => ({
    path: v[0],
    element: <TestView setLastVisited={setLastVisited} view={v} tab={i} />,
  }));

  const router = createBrowserRouter([
    {
      path: "*",
      element: <Redirect lastVisited={lastVisited} />,
    },
    ...paths,
  ]);

  return <RouterProvider router={router} />;
}

export default App;
