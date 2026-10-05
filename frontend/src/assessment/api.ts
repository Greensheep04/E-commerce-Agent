import type {
  AssessmentAccessEntry,
  AssessmentAccessUpdate,
  AssessmentEmployee,
  AssessmentEvidence,
  AssessmentIdentity,
  AssessmentMetricsResult,
  AssessmentRun,
  AssessmentScope,
  AssessmentStatus,
} from "./types";

export class AssessmentApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "AssessmentApiError";
    this.status = status;
  }
}

async function errorMessage(response: Response): Promise<string> {
  const body = await response.text();
  if (body) {
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
  return `请求失败，HTTP ${response.status}`;
}

async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: {
      ...(init.headers ?? {}),
    },
  });
  if (!response.ok) {
    throw new AssessmentApiError(response.status, await errorMessage(response));
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
    throw new AssessmentApiError(response.status, await errorMessage(response));
  }
}

function jsonBody(body: unknown): RequestInit {
  return {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

function runPath(runId: string): string {
  return "/api/assessment/runs/" + encodeURIComponent(runId);
}

export const assessmentApi = {
  async login(username: string, password: string): Promise<void> {
    await requestVoid("/api/login", jsonBody({ username, password }));
  },

  logout(): Promise<void> {
    return requestVoid("/api/logout", { method: "POST" });
  },

  getIdentity(): Promise<AssessmentIdentity> {
    return requestJson("/api/assessment/me");
  },

  getEmployees(): Promise<AssessmentEmployee[]> {
    return requestJson("/api/assessment/employees");
  },

  getStatus(): Promise<AssessmentStatus> {
    return requestJson("/api/assessment/status");
  },

  getMetrics(
    scope: AssessmentScope,
    employeeIds: number[] = [],
  ): Promise<AssessmentMetricsResult> {
    const parameters = new URLSearchParams({ scope });
    employeeIds.forEach((employeeId) => {
      parameters.append("employee_ids", String(employeeId));
    });
    return requestJson("/api/assessment/metrics?" + parameters.toString());
  },

  getEvidence(employeeId: number): Promise<AssessmentEvidence> {
    return requestJson(
      "/api/assessment/evidence/" + encodeURIComponent(String(employeeId)),
    );
  },

  listRuns(): Promise<AssessmentRun[]> {
    return requestJson("/api/assessment/runs");
  },

  createRun(prompt: string, parentId?: string): Promise<AssessmentRun> {
    return requestJson(
      "/api/assessment/runs",
      jsonBody({ prompt, ...(parentId ? { parent_id: parentId } : {}) }),
    );
  },

  getRun(runId: string): Promise<AssessmentRun> {
    return requestJson(runPath(runId));
  },

  runAction(runId: string, action: "run" | "resume" | "cancel"): Promise<AssessmentRun> {
    return requestJson(runPath(runId) + "/" + action, { method: "POST" });
  },

  listAccess(): Promise<AssessmentAccessEntry[]> {
    return requestJson("/api/assessment/access");
  },

  updateAccess(update: AssessmentAccessUpdate): Promise<AssessmentAccessEntry> {
    return requestJson("/api/assessment/access", jsonBody(update));
  },

  exportUrl(runId: string, format: "html" | "csv"): string {
    const parameters = new URLSearchParams({ format });
    return runPath(runId) + "/export?" + parameters.toString();
  },
};
