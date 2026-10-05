export type AssessmentLevel = "employee" | "manager" | "admin";

export type AssessmentRunStatus =
  | "created"
  | "running"
  | "paused"
  | "completed"
  | "stopped"
  | "cancelled";

export type AssessmentScope = "self" | "team" | "employees" | "organization";

export interface AssessmentIdentity {
  user_id: string | number;
  level: AssessmentLevel;
  employee_id: number | null;
  allowed_employee_ids: number[];
  managed_employee_ids: number[];
  notice: string;
}

export interface AssessmentEmployee {
  employee_id: number;
  job_title: string;
  current_flag: boolean;
}

export interface AssessmentStatus {
  status: "ready" | "DATA_BLOCKED";
  reason: string | null;
  notice: string;
  source_id: string;
  revision: string;
}

export interface AssessmentMetric {
  metric_id: string;
  label: string;
  value: string | null;
  display: string;
  status: string;
  evidence_id: string;
}

export interface AssessmentEmployeeMetrics {
  employee_id: number;
  job_title: string;
  metrics: AssessmentMetric[];
  quota: AssessmentQuotaEvidence | null;
}

export interface AssessmentMetricsResult {
  period_start: string;
  period_end: string;
  rule_version: string;
  notice: string;
  source: Record<string, unknown>;
  employees: AssessmentEmployeeMetrics[];
  summary: { employee_ids: number[]; label: string; metrics: AssessmentMetric[] };
  unassigned: Record<string, unknown> | null;
}

export interface AssessmentOrderEvidence {
  order_id: number | string;
  order_date: string;
  subtotal: string;
  source_line: number;
  status: number | string;
}

export interface AssessmentQuotaEvidence {
  [key: string]: unknown;
}

export interface AssessmentEvidence {
  employee_id: number;
  orders: AssessmentOrderEvidence[];
  quota: AssessmentQuotaEvidence | null;
  source: Record<string, unknown>;
}

export interface AssessmentObservation {
  employee_id: number | null;
  metric_id: string;
  evidence_id: string;
  text: string;
}

export interface AssessmentComparisonEntry {
  employee_id: number;
  value: string | null;
  display: string;
  status: string;
  evidence_id: string;
  rank: number | null;
}

export interface AssessmentComparison {
  metric_id: string;
  label: string;
  employee_ids: number[];
  entries: AssessmentComparisonEntry[];
  text: string;
}

export interface AssessmentResult {
  headline: string;
  observations: AssessmentObservation[];
  comparisons?: AssessmentComparison[];
  suggestions: string[];
  notice?: string;
  evidence?: { employees: AssessmentEvidence[] } | null;
}

export interface AssessmentRequestUsage {
  step_key: string;
  status: string;
  charged_cny: string | null;
  reservation_cny: string;
  model: string;
  usage: Record<string, unknown> | null;
}

export interface AssessmentRun {
  id: string;
  run_id: string;
  prompt: string;
  status: AssessmentRunStatus;
  model_calls: number;
  tool_calls: number;
  elapsed_seconds: number;
  cancel_requested: boolean;
  stop_reason: string | null;
  error: string | null;
  metrics: AssessmentMetricsResult | null;
  result: AssessmentResult | null;
  requests?: AssessmentRequestUsage[];
}

export interface AssessmentAccessEntry {
  user_id: string | number;
  username: string;
  level: AssessmentLevel | null;
  employee_id: number | null;
  managed_employee_ids: number[];
}

export interface AssessmentAccessUpdate {
  user_id: string | number;
  level: AssessmentLevel;
  employee_id: number | null;
  managed_employee_ids: number[];
}
