export const AUTHORIZATION_STORAGE_KEY = 'Authorization';
const AUTHORIZATION_CHANGED_EVENT = 'onecityagent:authorization-changed';

// Never put credentials in events: listeners only need to discard old state.
export const notifyAuthorizationChanged = () => {
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new Event(AUTHORIZATION_CHANGED_EVENT));
  }
};

export const subscribeAuthorizationChange = (listener: () => void) => {
  if (typeof window === 'undefined') return () => {};
  const onStorage = (event: StorageEvent) => {
    if (
      (event.key === AUTHORIZATION_STORAGE_KEY || event.key === null) &&
      event.oldValue !== event.newValue
    ) {
      listener();
    }
  };
  window.addEventListener(AUTHORIZATION_CHANGED_EVENT, listener);
  window.addEventListener('storage', onStorage);
  return () => {
    window.removeEventListener(AUTHORIZATION_CHANGED_EVENT, listener);
    window.removeEventListener('storage', onStorage);
  };
};

export const buildDeerFlowDownloadUrl = (
  fileUrl: string,
  conversationId: string,
  origin: string,
) => {
  const parsed = new URL(fileUrl, origin);
  const artifactUrl =
    parsed.origin === origin &&
    parsed.pathname === '/query/deerflow/file/download'
      ? parsed.searchParams.get('url') || ''
      : fileUrl;
  if (!artifactUrl || !conversationId) {
    throw new Error('文件下载缺少会话或文件地址');
  }
  // Only this same-origin endpoint receives the platform login token. The
  // server validates the upstream artifact origin and the dialog ownership.
  return `/query/deerflow/file/download?${new URLSearchParams({
    url: artifactUrl,
    conversation_id: conversationId,
  }).toString()}`;
};

export const isDeerFlowArtifactUrl = (fileUrl: string, origin: string) => {
  try {
    const parsed = new URL(fileUrl, origin);
    return parsed.pathname.startsWith('/api/threads/') ||
      (parsed.origin === origin && parsed.pathname === '/query/deerflow/file/download');
  } catch {
    return false;
  }
};
