import type { Task, UserIdentity } from "./types";

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function responseError(response: Response): Promise<ApiError> {
  const body: unknown = await response.json().catch(() => null);
  if (typeof body === "object" && body !== null) {
    const detail = "detail" in body ? body.detail : undefined;
    const message = "message" in body ? body.message : undefined;
    if (typeof detail === "string" && detail.trim()) {
      return new ApiError(response.status, detail);
    }
    if (typeof message === "string" && message.trim()) {
      return new ApiError(response.status, message);
    }
  }
  return new ApiError(response.status, "请求未能完成，请稍后重试。");
}

async function requestJson<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: {
      ...(init.headers ?? {}),
    },
  });
  if (!response.ok) {
    throw await responseError(response);
  }
  return (await response.json()) as T;
}

async function requestVoid(path: string, init: RequestInit = {}): Promise<void> {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: {
      ...(init.headers ?? {}),
    },
  });
  if (!response.ok) {
    throw await responseError(response);
  }
}

function taskPath(taskId: string): string {
  return "/api/harness/tasks/" + encodeURIComponent(taskId);
}

export const api = {
  login(username: string, password: string): Promise<UserIdentity> {
    return requestJson<UserIdentity>("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
  },

  logout(): Promise<void> {
    return requestVoid("/api/logout", { method: "POST" });
  },

  listTasks(): Promise<Task[]> {
    return requestJson<Task[]>("/api/harness/tasks");
  },

  createTask(prompt: string): Promise<Task> {
    return requestJson<Task>("/api/harness/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt }),
    });
  },

  getTask(taskId: string): Promise<Task> {
    return requestJson<Task>(taskPath(taskId));
  },

  runTask(taskId: string): Promise<Task> {
    return requestJson<Task>(taskPath(taskId) + "/run", { method: "POST" });
  },

  resumeTask(taskId: string): Promise<Task> {
    return requestJson<Task>(taskPath(taskId) + "/resume", {
      method: "POST",
    });
  },

  cancelTask(taskId: string): Promise<Task> {
    return requestJson<Task>(taskPath(taskId) + "/cancel", {
      method: "POST",
    });
  },

  async previewTask(taskId: string): Promise<string> {
    const response = await fetch(taskPath(taskId) + "/preview", {
      credentials: "same-origin",
    });
    if (!response.ok) {
      throw await responseError(response);
    }
    return response.text();
  },

  exportUrl(taskId: string): string {
    return taskPath(taskId) + "/export";
  },
};

export function assetUrl(assetId: string, version: string | number): string {
  return (
    "/api/assets/" +
    encodeURIComponent(assetId) +
    "/versions/" +
    encodeURIComponent(String(version)) +
    "/file"
  );
}
