export type DataSourceType =
  | "local_dataset"
  | "uploaded_file"
  | "database"
  | "vector_store";

export type DataSourceStatus = "ready" | "syncing" | "error" | "disabled";

export interface DataSourceRecord {
  id: string;
  name: string;
  type: DataSourceType;
  status: DataSourceStatus;
  description?: string | null;
  path?: string | null;
  virtual_path?: string | null;
  updated_at?: string | null;
  owner_scope: "thread" | "workspace" | "global";
  selectable_in_chat: boolean;
  thread_id?: string | null;
  metadata?: Record<string, unknown>;
}

export interface DataSourceListResponse {
  sources: DataSourceRecord[];
  count: number;
}
