import {
  Button,
  Paper,
  Slider,
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
import axios from "axios";
import { useSse } from "./sse";

export default function ThroughputTest() {
  const applyAgents = React.useCallback((agents) => {
    axios.post(
      "/throughput/settings",
      {
        agents,
      },
      {
        headers: {
          "Content-Type": "application/json",
        },
      }
    );
  }, []);

  const applyTargetRate = React.useCallback((targetRate) => {
    axios.post(
      "/throughput/settings",
      {
        targetRate,
      },
      {
        headers: {
          "Content-Type": "application/json",
        },
      }
    );
  }, []);

  const [agents, setAgents] = React.useState(10);
  const [agentsPending, setAgentsPending] = React.useState(false);
  const [agentsError, setAgentsError] = React.useState();

  const [logTargetRate, setLogTargetRate] = React.useState(0);
  const [targetRate, setTargetRate] = React.useState(1000);

  const applyLogTargetRate = (value) => {
    const exp = 2 + Math.max(0, Math.floor(Math.log10(value * 5)));
    const factor = Math.pow(10, exp);
    const targetRate =
      factor *
      Math.round(
        Math.pow(
          10.0,
          Math.log10(1000) +
            (Math.log10(1000000) - Math.log10(1000)) * ((value * 5) / 100.0)
        ) / factor
      );
    applyTargetRate(targetRate);
  };

  const updateTargetRate = (value) => {
    setTargetRate(value);
    const logValue = Math.log10(value);
    const logMin = Math.log10(1000);
    const logMax = Math.log10(1000000);
    const ratio = (logValue - logMin) / (logMax - logMin);
    setLogTargetRate(ratio * 20);
  };

  const [running, setRunning] = React.useState(false);

  const start = () => {
    axios.post(
      "/throughput/start",
      {
        targetRate,
        agents,
      },
      {
        headers: {
          "Content-Type": "application/json",
        },
      }
    );
  };

  const stop = () => {
    axios.post(
      "/throughput/stop",
      {},
      {
        headers: {
          "Content-Type": "application/json",
        },
      }
    );
  };

  const [stats, setStats] = React.useState();
  const onEvent = React.useCallback((event) => {
    if (event === "Running") {
      setRunning(true);
    } else if (event === "Stopped") {
      setRunning(false);
    } else if (event?.Settings) {
      if (event.Settings.agents !== undefined) {
        setAgents(event.Settings.agents);
      }
      if (event.Settings.targetRate !== undefined) {
        updateTargetRate(event.Settings.targetRate);
      }
    } else if (event?.Stats) {
      setStats(event.Stats);
    } else if (event?.CreatingAgents) {
      let [connected, total] = event.CreatingAgents;
      setAgentsPending(connected < total);
      setAgents(connected);
    }
  }, []);
  useSse("/throughput/stats", onEvent);

  const [unparsedAgents, setUnparsedAgents] = React.useState(undefined);

  const commitAgents = () => {
    if (unparsedAgents === undefined) {
      return;
    }
    const parsed = Number.parseInt(unparsedAgents);
    if (Number.isNaN(parsed)) {
      setAgentsError(`'${unparsedAgents}' is not a number`);
    } else if (parsed < 1) {
      setAgentsError(`must be >= 1`);
    } else {
      applyAgents(parsed);
      setAgentsError(null);
    }
  };

  return (
    <TableContainer component={Paper}>
      <Table>
        <TableBody>
          <TableRow>
            <TableCell>
              <Typography>Agents:</Typography>
            </TableCell>
            <TableCell>
              <Typography>{agents}</Typography>
            </TableCell>
            <TableCell>
              <Stack direction="row" spacing={1}>
                <TextField
                  size="small"
                  defaultValue={agents || 1}
                  onChange={(e) => {
                    setUnparsedAgents(e.target.value);
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      commitAgents();
                    }
                  }}
                  inputProps={{ inputMode: "numeric" }}
                  error={Boolean(agentsError)}
                  helperText={agentsError}
                  disabled={agentsPending}
                />
                <Button
                  variant="contained"
                  onClick={(e) => commitAgents()}
                  disabled={agentsPending}
                >
                  Apply
                </Button>
              </Stack>
            </TableCell>
          </TableRow>

          <TableRow>
            <TableCell>
              <Typography>Target Rate:</Typography>
            </TableCell>
            <TableCell>
              <Typography>{targetRate}</Typography>
            </TableCell>
            <TableCell>
              <Slider
                value={logTargetRate}
                min={0}
                max={20}
                onChange={(e) => applyLogTargetRate(e.target.value)}
                disabled={agentsPending}
              />
            </TableCell>
          </TableRow>

          <TableRow>
            <TableCell>
              <Button
                variant="contained"
                disabled={running || agentsPending}
                onClick={start}
              >
                Start
              </Button>
            </TableCell>
            <TableCell>
              <Button
                variant="contained"
                disabled={!running || agentsPending}
                onClick={stop}
              >
                Stop
              </Button>
            </TableCell>
            <TableCell></TableCell>
          </TableRow>

          {stats ? (
            <TableRow>
              <TableCell>
                <Typography>Send rate: {stats.sendRate}</Typography>
              </TableCell>
              <TableCell>
                <Typography>Receive rate: {stats.receiveRate}</Typography>
              </TableCell>
              <TableCell>
                <Typography>Lag: {stats.lag}</Typography>
              </TableCell>
            </TableRow>
          ) : null}
        </TableBody>
      </Table>
    </TableContainer>
  );
}
