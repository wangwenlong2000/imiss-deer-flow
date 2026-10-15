import { getBackendBaseURL } from "@/core/config";

export interface WorkspaceFileItem {
  name: string;
  relative_path: string;
  virtual_path: string;
  size_bytes: number;
  modified_at: string;
  mime_type: string | null;
}

export interface WorkspaceFileListResponse {
  root: string;
  files: WorkspaceFileItem[];
}

export async function listWorkspaceFiles(
  threadId: string,
): Promise<WorkspaceFileListResponse> {
  const response = await fetch(
    `${getBackendBaseURL()}/api/threads/${encodeURIComponent(
      threadId,
    )}/workspace-files`,
    {
      method: "GET",
      cache: "no-store",
    },
  );

  if (!response.ok) {
    const detail = await response.text();

    throw new Error(
      `Failed to list workspace files: ${response.status} ${detail}`,
    );
  }

  return (await response.json()) as WorkspaceFileListResponse;
}