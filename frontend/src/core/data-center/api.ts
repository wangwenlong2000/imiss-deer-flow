import { getBackendBaseURL } from "../config";

import type { DataSourceListResponse, DataSourceRecord } from "./types";

export interface UploadDataSourcesResponse {
  success: boolean;
  sources: DataSourceRecord[];
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
