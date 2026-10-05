export type TaskStatus =
  | "created"
  | "running"
  | "paused"
  | "completed"
  | "stopped"
  | "cancelled";

export type TaskResultStatus = "materials_found" | "insufficient";

export interface UserIdentity {
  id: string | number;
  username: string;
  role: string;
}

export interface Recommendation {
  asset_id: string;
  title: string;
  caption: string;
  source_id: string;
  source_revision: string;
  record_id: string;
  version: string | number;
  file_sha256: string;
}

export interface TaskResult {
  status: TaskResultStatus;
  headline: string;
  summary: string;
  recommendations: Recommendation[];
  missing_info: string[];
}

export interface Task {
  task_id: string;
  run_id: string | null;
  prompt: string;
  status: TaskStatus;
  model_calls: number;
  tool_calls: number;
  elapsed_seconds: number;
  cancel_requested: boolean;
  result: TaskResult | null;
  error: string | null;
  stop_reason: string | null;
  requests?: unknown[];
  tools?: unknown[];
}
