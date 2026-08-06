const DATA_CENTER_CHAT_SELECTION_KEY = "data-center.chat-selection";

function isBrowser() {
  return typeof window !== "undefined";
}

export function readSelectedDataSourceIds(): string[] {
  if (!isBrowser()) {
    return [];
  }

  try {
    const raw = window.localStorage.getItem(DATA_CENTER_CHAT_SELECTION_KEY);
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed.filter((item): item is string => typeof item === "string");
  } catch {
    return [];
  }
}

export function writeSelectedDataSourceIds(ids: string[]): void {
  if (!isBrowser()) {
    return;
  }

  const normalized = Array.from(
    new Set(ids.filter((id): id is string => typeof id === "string" && id.length > 0)),
  );

  window.localStorage.setItem(
    DATA_CENTER_CHAT_SELECTION_KEY,
    JSON.stringify(normalized),
  );
}

