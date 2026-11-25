import { subscribeAuthorizationChange } from './session';

interface ActiveRun {
  thread_id: string;
  run_id: string;
}

class StreamRequestError extends Error {
  constructor(message: string, readonly status?: number) {
    super(message);
  }
}

/** Receive a task stream, resuming only an already accepted DeerFlow run. */
export class SseClient {
  private abortController: AbortController | null = null;
  private isConnected = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private unsubscribeAuthorizationChange: (() => void) | null = null;
  private run: ActiveRun | null = null;
  private terminal = false;
  private completed = false;
  private retries = 0;
  private seenSegments = new Set<string>();
  private resumeConnection: (() => void) | null = null;
  private readonly maxRetries = 3;

  constructor(
    private url: string,
    private headers?: Record<string, string>,
  ) {}

  get connected() {
    return this.isConnected;
  }

  get activeRun(): ActiveRun | null {
    return this.run ? { ...this.run } : null;
  }

  connect(
    onMessage: (data: any) => void,
    onError?: (error: any) => void,
    onOpen?: () => void,
  ) {
    this.close();
    this.run = null;
    this.terminal = false;
    this.completed = false;
    this.retries = 0;
    this.seenSegments.clear();
    const controller = new AbortController();
    this.abortController = controller;
    this.unsubscribeAuthorizationChange = subscribeAuthorizationChange(() => this.close());
    this.resumeConnection = () => {
      const resume = this.resumeUrl();
      if (!resume || this.terminal) return;
      this.abortController?.abort();
      const resumedController = new AbortController();
      this.abortController = resumedController;
      this.retries = 0;
      this.readStream(resume, resumedController, onMessage, onError, onOpen);
    };
    this.readStream(this.url, controller, onMessage, onError, onOpen);
  }

  resume() {
    this.resumeConnection?.();
  }

  private resumeUrl(): string | null {
    const original = new URL(this.url, window.location?.origin || 'http://localhost');
    if (!['/query/deerflow', '/query/deerflow/resume'].includes(original.pathname) || !this.run) {
      return null;
    }
    const conversationId = original.searchParams.get('conversation_id');
    if (!conversationId) return null;
    const params = new URLSearchParams({
      conversation_id: conversationId,
      thread_id: this.run.thread_id,
      run_id: this.run.run_id,
      query: original.searchParams.get('query') || '',
    });
    return `/query/deerflow/resume?${params.toString()}`;
  }

  private readStream(
    url: string,
    controller: AbortController,
    onMessage: (data: any) => void,
    onError?: (error: any) => void,
    onOpen?: () => void,
  ) {
    const { signal } = controller;
    void (async () => {
      const response = await fetch(url, { headers: this.headers, signal });
      if (!response.ok) {
        const messages: Record<number, string> = {
          401: '登录已失效，请重新登录',
          403: '当前账号没有访问该会话的权限',
          404: '会话不存在或当前账号无权访问',
          409: '当前任务已变更，无法恢复该任务的连接',
          429: '当前并发名额已满，请等待已有任务完成后重试',
        };
        throw new StreamRequestError(messages[response.status] || `请求失败（HTTP ${response.status}）`, response.status);
      }
      if (!response.body) throw new Error('服务未返回任务数据流');
      if (signal.aborted) return;
      this.isConnected = true;
      onOpen?.();

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      const dispatch = (block: string) => {
        const data = block.split(/\r?\n/)
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice(5).replace(/^ /, ''))
          .join('\n');
        if (!data || signal.aborted) return;
        try {
          const event = JSON.parse(data);
          const threadId = event.thread_id || event.metadata?.thread_id || event.data?.thread_id;
          const runId = event.run_id || event.metadata?.run_id || event.data?.run_id;
          if (threadId && runId && (!this.run || this.run.run_id === runId)) {
            this.run = { thread_id: String(threadId), run_id: String(runId) };
          }
          if (event.type === '[DONE]') this.completed = true;
          if (this.completed || event.type === 'error') this.terminal = true;
          if (event.type === 'agent' && event.metadata?.source_event) {
            const key = JSON.stringify([event.role || event.type, event.metadata.source_event, event.content]);
            if (this.seenSegments.has(key)) return;
            this.seenSegments.add(key);
          }
        } catch {
          // Preserve non-JSON frames for callers using the general query API.
        }
        onMessage(data);
      };

      try {
        while (!signal.aborted && !this.completed) {
          const { value, done } = await reader.read();
          buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
          const blocks = buffer.split(/\r?\n\r?\n/);
          buffer = blocks.pop() || '';
          blocks.forEach(dispatch);
          if (done) break;
        }
        if (buffer.trim() && !this.completed) dispatch(buffer);
      } finally {
        reader.releaseLock();
      }
      if (!signal.aborted && !this.completed) {
        throw new Error(this.run ? '任务连接中断，正在尝试恢复' : '连接在任务确认前中断，请核对会话状态后重试');
      }
    })().catch((error) => {
      if (signal.aborted || this.abortController !== controller) return;
      this.isConnected = false;
      const resume = this.resumeUrl();
      const stopStatus = error instanceof StreamRequestError && [401, 403, 404, 409, 429].includes(error.status || 0);
      if (!this.terminal && !stopStatus && resume && this.retries < this.maxRetries) {
        const delay = 1000 * 2 ** this.retries;
        this.retries += 1;
        this.reconnectTimer = setTimeout(() => {
          this.reconnectTimer = null;
          if (!signal.aborted) this.readStream(resume, controller, onMessage, onError, onOpen);
        }, delay);
        return;
      }
      onError?.(error);
    }).finally(() => {
      if (this.abortController === controller) this.isConnected = false;
    });
  }

  close() {
    this.abortController?.abort();
    this.abortController = null;
    this.isConnected = false;
    this.unsubscribeAuthorizationChange?.();
    this.unsubscribeAuthorizationChange = null;
    this.resumeConnection = null;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}
