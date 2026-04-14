import { useCallback, useEffect, useState } from "react";

import {
  DEFAULT_LOCAL_SETTINGS,
  getLocalSettings,
  saveLocalSettings,
  type LocalSettings,
} from "./local";

export function useLocalSettings(): [
  LocalSettings,
  <K extends keyof LocalSettings>(
    key: K,
    value: Partial<LocalSettings[K]>,
  ) => void,
] {
  const [mounted, setMounted] = useState(false);
  const [state, setState] = useState<LocalSettings>(DEFAULT_LOCAL_SETTINGS);
  useEffect(() => {
    if (!mounted) {
      setState(getLocalSettings());
    }
    setMounted(true);
  }, [mounted]);
  const setter = useCallback(
    <K extends keyof LocalSettings>(key: K, value: Partial<LocalSettings[K]>) => {
      if (!mounted) return;
      setState((prev) => {
        const newState = {
          ...prev,
          [key]: {
            ...prev[key],
            ...value,
          },
        };
        saveLocalSettings(newState);
        return newState;
      });
    },
    [mounted],
  );
  return [state, setter];
}
