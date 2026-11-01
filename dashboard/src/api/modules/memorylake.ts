/** Same-origin, explicit file submission. No automatic mutation retries. */
import { getApiUrl } from "../config";
import { request, requestUpload } from "../request";
export interface MemoryLakeStatus {
  enabled: boolean;
  available: boolean;
  reason?: string;
  max_file_bytes: number;
  upload_ttl_seconds: number;
}
export interface MemoryLakeBinding {
  binding_id: string;
  connector_id: string;
  workspace_id: string;
  project_id: string;
  library_parent_id: string;
  writes_enabled: boolean;
  policy_revision: number;
}
export interface MemoryLakeConnection {
  writes_enabled: boolean;
  api_key: string;
  base_url: string;
  workspace_id: string;
  project_id: string;
  library_parent_id: string;
  display_name: string;
}
export interface MemoryLakeReview {
  submission_id: string;
  review_id: string;
  filename: string;
  size: number;
  sha256: string;
  workspace_id: string;
  project_id: string;
  library_parent_id: string;
  expires_at: number;
}
export interface MemoryLakeSubmission {
  submission_id: string;
  state: string;
  requires_reconciliation: boolean;
}
function local(path: string) {
  if (
    new URL(getApiUrl(path), window.location.href).origin !==
    window.location.origin
  )
    throw new Error("memorylake_same_origin_required");
  return path;
}
const agentPath = (agent: string) =>
  `/memorylake/agents/${encodeURIComponent(agent)}`;
const bindingPath = (agent: string, binding: string) =>
  `${agentPath(agent)}/bindings/${encodeURIComponent(binding)}`;
export const memorylakeApi = {
  search: (
    agent: string,
    binding: string,
    query: string,
    top_k = 5,
    signal?: AbortSignal,
  ) =>
    request<MemoryLakeSearchResult>(
      local(`${bindingPath(agent, binding)}/search`),
      { method: "POST", body: JSON.stringify({ query, top_k }), signal },
    ),
  document: (
    agent: string,
    binding: string,
    id: string,
    signal?: AbortSignal,
  ) =>
    request<MemoryLakeDocumentResult>(
      local(
        `${bindingPath(agent, binding)}/documents/${encodeURIComponent(id)}`,
      ),
      { signal },
    ),
  status: (signal?: AbortSignal) =>
    request<MemoryLakeStatus>(local("/memorylake/status"), { signal }),
  bindings: (agent: string, signal?: AbortSignal) =>
    request<{ bindings: MemoryLakeBinding[] }>(
      local(`${agentPath(agent)}/bindings`),
      { signal },
    ),
  createBinding: (
    agent: string,
    body: MemoryLakeConnection,
    signal?: AbortSignal,
  ) =>
    request<MemoryLakeBinding>(local(`${agentPath(agent)}/bindings`), {
      method: "POST",
      body: JSON.stringify(body),
      signal,
    }),
  writePolicy: (
    agent: string,
    binding: string,
    enabled: boolean,
    revision: number,
    signal?: AbortSignal,
  ) =>
    request<MemoryLakeBinding>(
      local(`${bindingPath(agent, binding)}/write-policy`),
      {
        method: "PATCH",
        body: JSON.stringify({ enabled, expected_revision: revision }),
        signal,
      },
    ),
  upload: (
    agent: string,
    binding: string,
    file: File,
    signal?: AbortSignal,
  ) => {
    const body = new FormData();
    body.append("file", file);
    return requestUpload<{ upload_id: string }>(
      local(`${bindingPath(agent, binding)}/uploads`),
      body,
      { signal },
    );
  },
  discardUpload: (
    agent: string,
    binding: string,
    upload: string,
    signal?: AbortSignal,
  ) =>
    request<{ discarded: boolean }>(
      local(
        `${bindingPath(agent, binding)}/uploads/${encodeURIComponent(upload)}`,
      ),
      { method: "DELETE", signal },
    ),
  review: (
    agent: string,
    binding: string,
    upload: string,
    signal?: AbortSignal,
  ) =>
    request<MemoryLakeReview>(
      local(
        `${bindingPath(agent, binding)}/uploads/${encodeURIComponent(
          upload,
        )}/review`,
      ),
      { method: "POST", signal },
    ),
  submit: (
    agent: string,
    binding: string,
    upload: string,
    review: string,
    signal?: AbortSignal,
  ) =>
    request<MemoryLakeSubmission>(
      local(
        `${bindingPath(agent, binding)}/uploads/${encodeURIComponent(
          upload,
        )}/submit`,
      ),
      {
        method: "POST",
        body: JSON.stringify({ review_id: review, confirm: true }),
        signal,
      },
    ),
  submission: (
    agent: string,
    binding: string,
    id: string,
    signal?: AbortSignal,
  ) =>
    request<MemoryLakeSubmission>(
      local(
        `${bindingPath(agent, binding)}/submissions/${encodeURIComponent(id)}`,
      ),
      { signal },
    ),
};

export interface MemoryLakeReadScope {
  request_scope: { project_ids: string[] };
  untrusted: true;
  connector_id: string;
  workspace_id: string;
}
export interface MemoryLakeDocumentHit {
  document_id: string;
  project_id?: string | null;
  document_name?: string | null;
  file_name?: string | null;
  document_summary?: string | null;
  items?: {
    type?: string | null;
    text?: string | null;
    range?: string | null;
    highlight?: {
      chunks: {
        id?: string | null;
        text?: string | null;
        range?: string | null;
      }[];
    };
  }[];
}
export interface MemoryLakeSearchResult extends MemoryLakeReadScope {
  operation: "search_documents";
  data: { documents: MemoryLakeDocumentHit[] };
}
export interface MemoryLakeDocumentResult extends MemoryLakeReadScope {
  operation: "get_document";
  data: {
    id: string;
    name?: string | null;
    status?: string | null;
    document_type?: string | null;
    created_at?: string | null;
  };
}
