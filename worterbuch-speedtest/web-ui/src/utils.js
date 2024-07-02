import React from "react";

export function usePersistedState(key, initialState) {
  const [state, updateState] = React.useState(load(key, initialState));
  const setState = React.useCallback(
    (value) => {
      persist(key, value);
      updateState(value);
    },
    [key]
  );
  return [state, setState];
}

function load(key, initialState) {
  if (window.localStorage) {
    const persisted = window.localStorage.getItem(key);
    try {
      return persisted !== null ? JSON.parse(persisted) : initialState;
    } catch (err) {
      console.error(err.message);
      return initialState;
    }
  } else {
    return initialState;
  }
}

function persist(key, state) {
  if (window.localStorage) {
    window.localStorage.setItem(key, JSON.stringify(state));
  }
}
