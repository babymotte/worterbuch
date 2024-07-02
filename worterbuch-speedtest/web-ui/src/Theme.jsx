import { CssBaseline, ThemeProvider } from "@mui/material";
import { grey, teal } from "@mui/material/colors";
import { createTheme } from "@mui/material/styles";
import React from "react";
import { usePersistedState } from "./utils";

const DarkTheme = createTheme({
  palette: {
    mode: "dark",
    primary: {
      main: teal["800"],
    },
    text: {
      primary: grey["500"],
    },
  },
});

const LightTheme = createTheme({
  palette: {
    mode: "light",
  },
});

const DarkModeContext = React.createContext({
  darkMode: true,
  setDarkMode: () => {},
});

export const useDarkMode = () => React.useContext(DarkModeContext);

const Theme = ({ children }) => {
  const [darkMode, setDarkMode] = usePersistedState(
    "worterbuch.speedtest.mode.dark",
    true
  );

  const theme = darkMode ? DarkTheme : LightTheme;

  return (
    <DarkModeContext.Provider value={{ darkMode, setDarkMode }}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        {children}
      </ThemeProvider>
    </DarkModeContext.Provider>
  );
};

export default Theme;
