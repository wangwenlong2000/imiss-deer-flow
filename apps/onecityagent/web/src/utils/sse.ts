import { subscribeAuthorizationChange } from './session';

/**
 * SSE 客户端类：用于接收后端推送的数据
 */
export class SseClient {
  private eventSource: EventSource | null = null;
  private abortController: AbortController | null = null;
  private url: string;
  private headers?: Record<string, string>;
  private isConnected: boolean = false;
  private reconnectInterval: number = 5000;
  private reconnectTimer: any = null;
  private unsubscribeAuthorizationChange: (() => void) | null = null;

  constructor(url: string, headers?: Record<string, string>) {
    this.url = url;
    this.headers = headers;
  }

  get connected() {
    return this.isConnected;
  }

  connect(
    onMessage: (data: any) => void,
    onError?: (error: any) => void,
    onOpen?: () => void,
  ) {
    this.close();
    this.unsubscribeAuthorizationChange = subscribeAuthorizationChange(() => this.close());
    if (this.headers && Object.keys(this.headers).length > 0) {
      this.connectWithFetch(onMessage, onError, onOpen);
      return;
    }

    this.eventSource = new EventSource(this.url);
    this.eventSource.onopen = () => {
      this.isConnected = true;
      if (onOpen) onOpen();
    };
    this.eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        onMessage(data);
      } catch (e) {
        onMessage(event.data);
      }
    };
    this.eventSource.onerror = (err) => {
      this.isConnected = false;
      if (onError) onError(err);
      // // 自动重连
      // this.reconnectTimer = setTimeout(() => {
      //   this.connect(onMessage, onError, onOpen);
      // }, this.reconnectInterval);
    };
  }

  private connectWithFetch(
    onMessage: (data: any) => void,
    onError?: (error: any) => void,
    onOpen?: () => void,
  ) {
    this.abortController = new AbortController();
    const signal = this.abortController.signal;

    void (async () => {
      const response = await fetch(this.url, {
        headers: this.headers,
        signal,
      });
      if (!response.ok) {
        const messages: Record<number, string> = {
          401: '登录已失效，请重新登录',
          403: '当前账号没有访问该会话的权限',
          404: '会话不存在或当前账号无权访问',
          429: '当前并发名额已满，请等待已有任务完成后重试',
        };
        throw new Error(messages[response.status] || `请求失败（HTTP ${response.status}）`);
      }
      if (!response.body) {
        throw new Error('SSE response has no body');
      }

      this.isConnected = true;
      if (onOpen) onOpen();

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      const dispatch = (block: string) => {
        const data = block
          .split(/\r?\n/)
          .filter((line) => line.startsWith('data:'))
          .map((line) => line.slice(5).replace(/^ /, ''))
          .join('\n');
        if (data && !signal.aborted) onMessage(data);
      };

      while (!signal.aborted) {
        const { value, done } = await reader.read();
        buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
        const blocks = buffer.split(/\r?\n\r?\n/);
        buffer = blocks.pop() || '';
        blocks.forEach(dispatch);
        if (done) break;
      }
      if (buffer.trim()) dispatch(buffer);
      if (this.abortController?.signal === signal) this.isConnected = false;
    })().catch((error) => {
      if (this.abortController?.signal === signal) this.isConnected = false;
      if (!signal.aborted && onError) onError(error);
    });
  }

  close() {
    this.abortController?.abort();
    this.abortController = null;
    this.isConnected = false;
    this.unsubscribeAuthorizationChange?.();
    this.unsubscribeAuthorizationChange = null;
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
      this.isConnected = false;
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
}
