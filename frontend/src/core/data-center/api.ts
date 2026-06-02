import { getBackendBaseURL } from "../config";

import type { DataSourceListResponse, DataSourceRecord } from "./types";

export interface UploadDataSourcesResponse {
  success: boolean;
  sources: DataSourceRecord[];
  message: string;
}

export interface DeleteDataSourceResponse {
  success: boolean;
  source_id: string;
  file_deleted: boolean;
  message: string;
}

async function readErrorDetail(
  response: Response,
  fallback: string,
): Promise<string> {
  const error = await response.json().catch(() => ({ detail: fallback }));
  return error.detail ?? fallback;
}

export async function listDataSources(): Promise<DataSourceListResponse> {
  const response = await fetch(`${getBackendBaseURL()}/api/data-center/sources`);
  if (!response.ok) {
    throw new Error(
      await readErrorDetail(response, "Failed to list data sources"),
    );
  }
  return response.json();
}


export function getDataSourceDownloadUrl(sourceId: string) {
  return `${getBackendBaseURL()}/api/data-center/sources/${sourceId}/download`;
}


export async function deleteDataSource(
  sourceId: string,
): Promise<DeleteDataSourceResponse> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/data-center/sources/${sourceId}`,
    {
      method: "DELETE",
    },
  );

  if (!response.ok) {
    throw new Error(
      await readErrorDetail(response, "Failed to delete data source"),
    );
  }

  return response.json();
}

export async function getDataSourceDetail(
  sourceId: string,
): Promise<DataSourceRecord> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/data-center/sources/${sourceId}`,
  );
  if (!response.ok) {
    throw new Error(
      await readErrorDetail(response, "Failed to load data source detail"),
    );
  }
  return response.json();
}

export async function registerUploadedFileAsDataSource(payload: {
  thread_id: string;
  filename: string;
  name?: string;
  description?: string;
}): Promise<DataSourceRecord> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/data-center/sources/register-upload`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    },
  );
  if (!response.ok) {
    throw new Error(
      await readErrorDetail(response, "Failed to register uploaded file"),
    );
  }
  return response.json();
}

export async function uploadDataSourceFiles(
  files: File[],
): Promise<UploadDataSourcesResponse> {
  const formData = new FormData();
  files.forEach((file) => {
    formData.append("files", file);
  });

  const response = await fetch(`${getBackendBaseURL()}/api/data-center/sources/upload`, {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    throw new Error(
      await readErrorDetail(response, "Failed to upload data sources"),
    );
  }

  return response.json();
}


export interface EsIndexItem {
  name: string;
  display_name: string;
  type: "elasticsearch";
  health: string | null;
  status: string | null;
  uuid?: string | null;
  pri: number;
  rep: number;
  docs_count: number;
  docs_deleted: number;
  store_size_bytes: number;
  pri_store_size_bytes: number;
}

export interface EsIndexListResponse {
  total: number;
  items: EsIndexItem[];
}

export interface EsFieldItem {
  name: string;
  type: string;
}

export interface EsIndexDetailResponse {
  name: string;
  docs_count: number;
  docs_deleted: number;
  store_size_bytes: number;
  field_count: number;
  fields: EsFieldItem[];
}

export interface EsSampleItem {
  id: string | null;
  score: number | null;
  source: Record<string, unknown>;
}

export interface EsSamplesResponse {
  index: string;
  total: unknown;
  items: EsSampleItem[];
}

export async function listEsIndices(): Promise<EsIndexListResponse> {
  const response = await fetch(`${getBackendBaseURL()}/api/data-center/es/indices`);

  if (!response.ok) {
    throw new Error(
      await readErrorDetail(response, "Failed to list Elasticsearch indices"),
    );
  }

  return response.json();
}

export async function getEsIndexDetail(
  indexName: string,
): Promise<EsIndexDetailResponse> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/data-center/es/indices/${encodeURIComponent(indexName)}`,
  );

  if (!response.ok) {
    throw new Error(
      await readErrorDetail(response, "Failed to load Elasticsearch index detail"),
    );
  }

  return response.json();
}

export async function getEsIndexSamples(
  indexName: string,
  size = 5,
): Promise<EsSamplesResponse> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/data-center/es/indices/${encodeURIComponent(indexName)}/samples?size=${size}`,
  );

  if (!response.ok) {
    throw new Error(
      await readErrorDetail(response, "Failed to load Elasticsearch samples"),
    );
  }

  return response.json();
}