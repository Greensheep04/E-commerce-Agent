export interface DataCenterResult<T = unknown> {
  status: string;
  run_id?: string | null;
  data: T;
  resources?: Array<{
    module_id: string;
    resource_type: string;
    resource_id: string;
    version?: string;
    sha256?: string;
  }>;
}

export interface DataCenterSource {
  source_id: string;
  kind: string;
  title: string;
  revision: string;
  nature: string;
  license_id: string;
  sha256: string;
  properties: Record<string, unknown>;
  resource?: Record<string, unknown> | null;
  relative_path?: string | null;
  active?: boolean;
  asset_id?: string;
  record_id?: string;
}

export interface DataCenterTask {
  task_id: string;
  run_id: string;
  operation: string;
  status: string;
  cancel_requested: boolean;
  input_sha256: string;
  created_at: string;
  updated_at: string;
  checkpoints: Record<string, unknown>;
  result?: DataCenterResult | null;
  error?: string | null;
}

export class DataCenterApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "DataCenterApiError";
    this.status = status;
  }
}

async function errorMessage(response: Response): Promise<string> {
  const body = await response.text();
  if (!body) {
    return `请求失败，HTTP ${response.status}`;
  }
  try {
    const parsed: unknown = JSON.parse(body);
    if (typeof parsed === "object" && parsed !== null && "detail" in parsed) {
      const detail = parsed.detail;
      if (typeof detail === "string" && detail.trim()) {
        return detail;
      }
      if (typeof detail === "object" && detail !== null) {
        return JSON.stringify(detail);
      }
    }
    if (typeof parsed === "object" && parsed !== null && "message" in parsed) {
      const message = parsed.message;
      if (typeof message === "string" && message.trim()) {
        return message;
      }
    }
  } catch {
    return body;
  }
  return body;
}

async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: { ...(init.headers ?? {}) },
  });
  if (!response.ok) {
    throw new DataCenterApiError(response.status, await errorMessage(response));
  }
  return (await response.json()) as T;
}

function jsonBody(body: unknown): RequestInit {
  return {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

function unwrap<T>(result: DataCenterResult<T>): T {
  return result.data;
}

export const dataCenterApi = {
  login(username: string, password: string): Promise<{ id: number | string; username: string; role: string }> {
    return requestJson("/api/login", jsonBody({ username, password }));
  },

  logout(): Promise<{ status: string }> {
    return requestJson("/api/logout", { method: "POST" });
  },

  async catalog(): Promise<DataCenterSource[]> {
    const result = await requestJson<DataCenterResult<{ items: DataCenterSource[] }>>(
      "/api/modules/data_center/operations/catalog",
      jsonBody({ request_id: crypto.randomUUID(), version: "1", payload: {}, upstream: [] }),
    );
    return unwrap(result).items;
  },

  async metrics(): Promise<Array<Record<string, unknown>>> {
    const result = await requestJson<DataCenterResult<{ items: Array<Record<string, unknown>> }>>(
      "/api/modules/data_center/operations/metrics",
      jsonBody({ request_id: crypto.randomUUID(), version: "1", payload: {}, upstream: [] }),
    );
    return unwrap(result).items;
  },

  async tasks(): Promise<DataCenterTask[]> {
    const result = await requestJson<DataCenterResult<{ items: DataCenterTask[] }>>(
      "/api/modules/data_center/operations/tasks",
      jsonBody({ request_id: crypto.randomUUID(), version: "1", payload: {}, upstream: [] }),
    );
    return unwrap(result).items;
  },

  operation<T = unknown>(operation: string, payload: Record<string, unknown> = {}): Promise<DataCenterResult<T>> {
    return requestJson(`/api/modules/data_center/operations/${encodeURIComponent(operation)}`,
      jsonBody({ request_id: crypto.randomUUID(), version: "1", payload, upstream: [] }));
  },

  async download(taskId: string, format: "markdown" | "csv"): Promise<{ blob: Blob; filename: string }> {
    const exported = await this.operation<{ relative_path: string; mime_type: string }>("export", {
      task_id: taskId,
      format,
    });
    const reference = exported.resources?.find((item) => item.resource_type === `export_${format}`);
    if (!reference) {
      throw new Error("导出结果缺少已登记的文件引用。");
    }
    const path = `/api/modules/data_center/resources/${encodeURIComponent(reference.resource_id)}/files/${encodeURIComponent(exported.data.relative_path)}`;
    const parameters = new URLSearchParams({ resource_type: reference.resource_type });
    if (reference.version) {
      parameters.set("version", reference.version);
    }
    if (reference.sha256) {
      parameters.set("sha256", reference.sha256);
    }
    const response = await fetch(`${path}?${parameters.toString()}`, { credentials: "same-origin" });
    if (!response.ok) {
      throw new DataCenterApiError(response.status, await errorMessage(response));
    }
    const filename = format === "csv" ? "data-center.csv" : "data-center.md";
    return { blob: await response.blob(), filename };
  },
};
