import {
  AppBar,
  Box,
  CssBaseline,
  Tab,
  Tabs,
  Toolbar,
  Typography,
} from "@mui/material";
import { views } from "./App";
import { useNavigate } from "react-router-dom";
import React from "react";

export default function TabBar({ tab }) {
  const navigate = useNavigate();
  const [value, setValue] = React.useState(tab);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  const tabs = views.map(([path, name], i) => {
    return (
      <Tab
        key={path}
        label={name}
        onClick={(e) => {
          handleChange(e, i);
          navigate(path);
        }}
      />
    );
  });

  return (
    <Box>
      <CssBaseline />
      <AppBar>
        <Toolbar>
          <Tabs value={value} onChange={handleChange}>
            {tabs}
          </Tabs>
        </Toolbar>
      </AppBar>
      <Toolbar />
    </Box>
  );
}
