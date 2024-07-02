import {
  Button,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import React from "react";
import { usePersistedState } from "./utils";
import axios from "axios";
import { useSse } from "./sse";

export default function LatencyTest() {
  const [publishers, setPublishers] = usePersistedState(
    "wortebruch.speedtest.latency.publishers",
    10
  );
  const [nAryKeys, setNArayKeys] = usePersistedState(
    "wortebruch.speedtest.latency.nAryKeys",
    5
  );
  const [keyLength, setKeyLength] = usePersistedState(
    "wortebruch.speedtest.latency.keyLength",
    5
  );
  const [messagesPerKey, setMessagesPerKey] = usePersistedState(
    "wortebruch.speedtest.latency.messagesPerKey",
    1
  );
  const [subscribers, setSubscribers] = usePersistedState(
    "wortebruch.speedtest.latency.subscribers",
    10
  );
  const [totalMessages, setTotalMessages] = usePersistedState(
    "wortebruch.speedtest.latency.totalMessages",
    0
  );
  const [running, setRunning] = React.useState(false);

  React.useEffect(() => {
    setTotalMessages(
      publishers * Math.pow(nAryKeys, keyLength) * messagesPerKey * subscribers
    );
  }, [
    keyLength,
    messagesPerKey,
    nAryKeys,
    publishers,
    setTotalMessages,
    subscribers,
  ]);

  const runTest = () => {
    setRunning(true);
    axios
      .post(
        "/latency/start",
        {
          publishers,
          nAryKeys,
          keyLength,
          messagesPerKey,
          subscribers,
          totalMessages,
        },
        {
          headers: {
            "Content-Type": "application/json",
          },
        }
      )
      .catch((err) => {
        console.error(err.message);
        setRunning(false);
      });
  };

  const stopTest = () => {
    axios
      .post(
        "/latency/stop",
        {},
        {
          headers: {
            "Content-Type": "application/json",
          },
        }
      )
      .catch((err) => {
        console.error(err.message);
      });
  };

  const onEvent = React.useCallback((event) => {
    console.log(event);
    // TODO
    if (event === "Running") {
      setRunning(true);
    } else if (event === "Stopped") {
      setRunning(false);
    }
  }, []);

  useSse("/latency/events", onEvent);

  return (
    <TableContainer component={Paper}>
      <Table>
        <TableBody>
          <TableRow>
            <TableCell>
              <Typography>Publishers:</Typography>
            </TableCell>
            <TableCell>
              <TextField
                size="small"
                value={String(publishers)}
                disabled={running}
                onChange={(e) => setPublishers(parseInt(e.target.value) || 0)}
              />
            </TableCell>
          </TableRow>
          <TableRow>
            <TableCell>
              <Typography>Key tree:</Typography>
            </TableCell>
            <TableCell>
              <Stack direction="row" alignItems="center">
                <TextField
                  size="small"
                  value={String(nAryKeys)}
                  disabled={running}
                  onChange={(e) => setNArayKeys(parseInt(e.target.value) || 0)}
                />
                <Typography>&nbsp;-&nbsp;ary</Typography>
              </Stack>
            </TableCell>
          </TableRow>
          <TableRow>
            <TableCell>
              <Typography>Key length:</Typography>
            </TableCell>
            <TableCell>
              <TextField
                size="small"
                value={String(keyLength)}
                disabled={running}
                onChange={(e) => setKeyLength(parseInt(e.target.value) || 0)}
              />
            </TableCell>
          </TableRow>
          <TableRow>
            <TableCell>
              <Typography>Messages per Key:</Typography>
            </TableCell>
            <TableCell>
              <TextField
                size="small"
                value={String(messagesPerKey)}
                disabled={running}
                onChange={(e) =>
                  setMessagesPerKey(parseInt(e.target.value) || 0)
                }
              />
            </TableCell>
          </TableRow>
          <TableRow>
            <TableCell>
              <Typography>Subscribers:</Typography>
            </TableCell>
            <TableCell>
              <TextField
                size="small"
                value={String(subscribers)}
                disabled={running}
                onChange={(e) => setSubscribers(parseInt(e.target.value) || 0)}
              />
            </TableCell>
          </TableRow>

          <TableRow>
            <TableCell>
              <Typography>Total Messages:</Typography>
            </TableCell>
            <TableCell>
              <Typography>{totalMessages}</Typography>
            </TableCell>
          </TableRow>
        </TableBody>

        <TableRow>
          <TableCell>
            <Button variant="contained" disabled={running} onClick={runTest}>
              Start
            </Button>
          </TableCell>
          <TableCell>
            <Button variant="contained" disabled={!running} onClick={stopTest}>
              Stop
            </Button>
          </TableCell>
        </TableRow>
      </Table>
    </TableContainer>
  );
}
