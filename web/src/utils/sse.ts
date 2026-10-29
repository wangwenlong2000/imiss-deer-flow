/**
 * SSE 客户端类：用于接收后端推送的数据
 */
export class SseClient {
  private eventSource: EventSource | null = null;
  private url: string;
  private isConnected: boolean = false;
  private reconnectInterval: number = 5000;
  private reconnectTimer: any = null;

  constructor(url: string) {
    this.url = url;
  }

  get connected() {
    return this.isConnected;
  }

  connect(
    onMessage: (data: any) => void,
    onError?: (error: any) => void,
    onOpen?: () => void,
  ) {
    if (this.eventSource) {
      this.close();
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

  close() {
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
