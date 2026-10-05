import { useEffect, useMemo, useState } from "react";
import { AssessmentApiError, assessmentApi } from "./api";
import type {
  AssessmentAccessEntry,
  AssessmentAccessUpdate,
  AssessmentEmployee,
  AssessmentEvidence,
  AssessmentIdentity,
  AssessmentLevel,
  AssessmentMetricsResult,
  AssessmentRun,
  AssessmentRunStatus,
  AssessmentScope,
  AssessmentStatus,
} from "./types";
import "./styles.css";

type AuthState = "checking" | "anonymous" | "authenticated" | "forbidden" | "unavailable";
type RunAction = "run" | "resume" | "cancel";

const levelLabels: Record<AssessmentLevel, string> = {
  employee: "员工",
  manager: "主管",
  admin: "管理员",
};

const runStatusLabels: Record<AssessmentRunStatus, string> = {
  created: "待启动",
  running: "运行中",
  paused: "已暂停",
  completed: "已完成",
  stopped: "已停止",
  cancelled: "已取消",
};

function errorMessage(error: unknown): string {
  if (error instanceof AssessmentApiError) {
    if (error.status === 401) {
      return "登录信息无效或会话已过期，请重新登录。";
    }
    if (error.status === 403) {
      return "当前账号没有考核权限，或无权读取所选员工。";
    }
    if (error.status === 409 && error.message.startsWith("DATA_BLOCKED")) {
      return `数据状态：${error.message}`;
    }
    return `服务返回 HTTP ${error.status}：${error.message}`;
  }
  if (error instanceof Error && error.message) {
    return `服务连接失败：${error.message}`;
  }
  return "服务请求失败，未能取得考核数据。";
}

function durationText(seconds: number): string {
  const total = Math.max(0, Math.floor(seconds));
  const minutes = Math.floor(total / 60);
  const remainingSeconds = total % 60;
  return minutes > 0
    ? `${minutes} 分 ${remainingSeconds} 秒`
    : `${remainingSeconds} 秒`;
}

function jsonText(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

async function getAssessmentStatus(): Promise<AssessmentStatus> {
  try {
    return await assessmentApi.getStatus();
  } catch (error) {
    if (
      error instanceof AssessmentApiError &&
      error.status === 409 &&
      error.message.startsWith("DATA_BLOCKED")
    ) {
      return {
        status: "DATA_BLOCKED",
        reason: error.message,
        notice: error.message,
        source_id: "",
        revision: "",
      };
    }
    throw error;
  }
}

function StatusBadge({ status }: { status: AssessmentRunStatus }) {
  return (
    <span className={`assessment-status assessment-status-${status}`}>
      <span className="assessment-status-dot" />
      {runStatusLabels[status]}
    </span>
  );
}

function LoginScreen({
  authState,
  message,
  onLogin,
  onRetry,
}: {
  authState: AuthState;
  message: string;
  onLogin: (username: string, password: string) => Promise<void>;
  onRetry: () => void;
}) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [formMessage, setFormMessage] = useState("");

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setFormMessage("");
    try {
      await onLogin(username, password);
    } catch (error) {
      setFormMessage(errorMessage(error));
    } finally {
      setSubmitting(false);
    }
  }

  if (authState === "checking") {
    return (
      <main className="assessment-auth-shell">
        <section className="assessment-auth-card">
          <span className="assessment-brand-mark">A</span>
          <p className="assessment-kicker">销售考核工作台</p>
          <h1>正在读取账号状态</h1>
          <p className="assessment-muted">正在连接考核服务。</p>
          <span className="assessment-loading-line" />
        </section>
      </main>
    );
  }

  if (authState === "forbidden") {
    return (
      <main className="assessment-auth-shell">
        <section className="assessment-auth-card">
          <span className="assessment-brand-mark">A</span>
          <p className="assessment-kicker">销售考核工作台</p>
          <h1>当前账号没有考核权限</h1>
          <p className="assessment-error-text" role="alert">
            {message || "请联系管理员配置考核身份。"}
          </p>
        </section>
      </main>
    );
  }

  if (authState === "unavailable") {
    return (
      <main className="assessment-auth-shell">
        <section className="assessment-auth-card">
          <span className="assessment-brand-mark">A</span>
          <p className="assessment-kicker">销售考核工作台</p>
          <h1>暂时无法连接服务</h1>
          <p className="assessment-error-text" role="alert">{message}</p>
          <button className="assessment-button assessment-button-primary assessment-button-wide" onClick={onRetry}>
            重新连接
          </button>
        </section>
      </main>
    );
  }

  return (
    <main className="assessment-auth-shell">
      <section className="assessment-auth-card">
        <span className="assessment-brand-mark">A</span>
        <p className="assessment-kicker">销售考核工作台</p>
        <h1>登录考核工作台</h1>
        <p className="assessment-muted">
          查看服务端授权范围内的员工指标、来源依据与考核任务。
        </p>
        <form className="assessment-login-form" onSubmit={submit}>
          <label>
            用户名
            <input
              autoComplete="username"
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              required
            />
          </label>
          <label>
            密码
            <input
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              required
            />
          </label>
          {(formMessage || message) && (
            <p className="assessment-error-text" role="alert">
              {formMessage || message}
            </p>
          )}
          <button
            className="assessment-button assessment-button-primary assessment-button-wide"
            type="submit"
            disabled={submitting}
          >
            {submitting ? "正在登录…" : "登录"}
          </button>
        </form>
      </section>
    </main>
  );
}

function employeeName(employee: AssessmentEmployee): string {
  return `${employee.employee_id} · ${employee.job_title}`;
}

function AssessmentWorkspace() {
  const [authState, setAuthState] = useState<AuthState>("checking");
  const [authMessage, setAuthMessage] = useState("");
  const [identity, setIdentity] = useState<AssessmentIdentity | null>(null);
  const [employees, setEmployees] = useState<AssessmentEmployee[]>([]);
  const [dataStatus, setDataStatus] = useState<AssessmentStatus | null>(null);
  const [runs, setRuns] = useState<AssessmentRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [prompt, setPrompt] = useState("");
  const [scope, setScope] = useState<AssessmentScope>("self");
  const [selectedEmployeeIds, setSelectedEmployeeIds] = useState<number[]>([]);
  const [selectedEmployeeId, setSelectedEmployeeId] = useState<number | null>(null);
  const [metrics, setMetrics] = useState<AssessmentMetricsResult | null>(null);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [metricsError, setMetricsError] = useState("");
  const [evidence, setEvidence] = useState<AssessmentEvidence | null>(null);
  const [evidenceLoading, setEvidenceLoading] = useState(false);
  const [evidenceError, setEvidenceError] = useState("");
  const [globalError, setGlobalError] = useState("");
  const [busy, setBusy] = useState(false);
  const [runAction, setRunAction] = useState<Record<string, RunAction>>({});
  const [runErrors, setRunErrors] = useState<Record<string, string>>({});
  const [accessEntries, setAccessEntries] = useState<AssessmentAccessEntry[]>([]);
  const [accessError, setAccessError] = useState("");
  const [accessSaving, setAccessSaving] = useState(false);
  const [accessUserId, setAccessUserId] = useState("");
  const [accessLevel, setAccessLevel] = useState<AssessmentLevel>("employee");
  const [accessEmployeeId, setAccessEmployeeId] = useState("");
  const [managedEmployeeIds, setManagedEmployeeIds] = useState<number[]>([]);

  const selectedRun = runs.find((run) => run.id === selectedRunId) ?? null;
  const dataReady = dataStatus?.status === "ready";
  const authorizedEmployees = useMemo(() => {
    if (!identity || identity.level === "admin") {
      return employees;
    }
    const permitted = new Set(identity.allowed_employee_ids);
    if (identity.employee_id !== null) {
      permitted.add(identity.employee_id);
    }
    return employees.filter((employee) => permitted.has(employee.employee_id));
  }, [employees, identity]);

  function replaceRun(updatedRun: AssessmentRun) {
    setRuns((current) => {
      const exists = current.some((run) => run.id === updatedRun.id);
      if (!exists) {
        return [updatedRun, ...current];
      }
      return current.map((run) => run.id === updatedRun.id ? updatedRun : run);
    });
  }

  function clearSession() {
    setIdentity(null);
    setEmployees([]);
    setDataStatus(null);
    setRuns([]);
    setSelectedRunId(null);
    setMetrics(null);
    setEvidence(null);
    setAccessEntries([]);
  }

  function showRequestError(error: unknown) {
    if (error instanceof AssessmentApiError && error.status === 401) {
      clearSession();
      setAuthMessage(errorMessage(error));
      setAuthState("anonymous");
      return;
    }
    if (error instanceof AssessmentApiError && error.status === 403) {
      setGlobalError(errorMessage(error));
      return;
    }
    if (error instanceof AssessmentApiError && error.status === 409 && error.message.startsWith("DATA_BLOCKED")) {
      setDataStatus((current) => ({
        status: "DATA_BLOCKED",
        reason: error.message,
        notice: current?.notice ?? "数据来源暂不可用。",
        source_id: current?.source_id ?? "",
        revision: current?.revision ?? "",
      }));
      setMetrics(null);
      setMetricsError(errorMessage(error));
      return;
    }
    setGlobalError(errorMessage(error));
  }

  async function refreshSession() {
    setAuthState("checking");
    setAuthMessage("");
    try {
      const currentIdentity = await assessmentApi.getIdentity();
      setIdentity(currentIdentity);
      setAuthState("authenticated");
      setScope(
        currentIdentity.level === "employee"
          ? "self"
          : currentIdentity.level === "manager"
            ? "team"
            : "organization",
      );
      setSelectedEmployeeIds(currentIdentity.managed_employee_ids);
      const results = await Promise.allSettled([
        assessmentApi.getEmployees(),
        getAssessmentStatus(),
        assessmentApi.listRuns(),
        currentIdentity.level === "admin" ? assessmentApi.listAccess() : Promise.resolve([]),
      ]);
      setGlobalError("");
      if (results[1].status === "fulfilled") {
        setDataStatus(results[1].value);
      } else {
        showRequestError(results[1].reason);
      }
      if (results[0].status === "fulfilled") {
        const employeeRows = results[0].value;
        setEmployees(employeeRows);
        setSelectedEmployeeId(
          currentIdentity.employee_id ??
          currentIdentity.allowed_employee_ids[0] ??
          employeeRows[0]?.employee_id ??
          null,
        );
      } else {
        showRequestError(results[0].reason);
      }
      if (results[2].status === "fulfilled") {
        const currentRuns = results[2].value;
        setRuns(currentRuns);
        setSelectedRunId(currentRuns[0]?.id ?? null);
      } else {
        showRequestError(results[2].reason);
      }
      if (results[3].status === "fulfilled") {
        setAccessEntries(results[3].value);
      } else {
        showRequestError(results[3].reason);
      }
    } catch (error) {
      setAuthMessage(errorMessage(error));
      if (error instanceof AssessmentApiError && error.status === 401) {
        setAuthState("anonymous");
      } else if (error instanceof AssessmentApiError && error.status === 403) {
        setAuthState("forbidden");
      } else {
        setAuthState("unavailable");
      }
    }
  }

  async function login(username: string, password: string): Promise<void> {
    await assessmentApi.login(username, password);
    await refreshSession();
  }

  async function logout() {
    try {
      await assessmentApi.logout();
      clearSession();
      setAuthState("anonymous");
      setAuthMessage("");
    } catch (error) {
      showRequestError(error);
    }
  }

  async function refreshMetrics() {
    if (!dataReady) {
      return;
    }
    if (scope === "employees" && selectedEmployeeIds.length === 0) {
      setMetrics(null);
      setMetricsError("请至少选择一名服务端允许查看的员工。");
      return;
    }
    setMetricsLoading(true);
    setMetricsError("");
    try {
      const employeeIds = scope === "employees" ? selectedEmployeeIds : [];
      const result = await assessmentApi.getMetrics(scope, employeeIds);
      setMetrics(result);
    } catch (error) {
      setMetrics(null);
      if (error instanceof AssessmentApiError && error.status === 409 && error.message.startsWith("DATA_BLOCKED")) {
        setDataStatus((current) => ({
          status: "DATA_BLOCKED",
          reason: error.message,
          notice: current?.notice ?? "数据来源暂不可用。",
          source_id: current?.source_id ?? "",
          revision: current?.revision ?? "",
        }));
        setMetricsError(errorMessage(error));
      } else {
        const message = errorMessage(error);
        setMetricsError(message);
        showRequestError(error);
      }
    } finally {
      setMetricsLoading(false);
    }
  }

  async function refreshEvidence(employeeId: number) {
    setEvidenceLoading(true);
    setEvidenceError("");
    try {
      setEvidence(await assessmentApi.getEvidence(employeeId));
    } catch (error) {
      setEvidence(null);
      setEvidenceError(errorMessage(error));
      showRequestError(error);
    } finally {
      setEvidenceLoading(false);
    }
  }

  async function refreshRun(runId: string) {
    try {
      replaceRun(await assessmentApi.getRun(runId));
    } catch (error) {
      showRequestError(error);
    }
  }

  useEffect(() => {
    void refreshSession();
  }, []);

  useEffect(() => {
    if (authState === "authenticated" && dataReady) {
      void refreshMetrics();
    }
  }, [authState, dataReady, scope, selectedEmployeeIds]);

  useEffect(() => {
    if (authState !== "authenticated" || selectedEmployeeId === null) {
      return;
    }
    void refreshEvidence(selectedEmployeeId);
  }, [authState, selectedEmployeeId]);

  useEffect(() => {
    if (authState !== "authenticated" || !runs.some((run) => run.status === "running" ||
        runAction[run.id] === "run" || runAction[run.id] === "resume")) {
      return;
    }
    const interval = window.setInterval(() => {
      runs
        .filter((run) => run.status === "running" || runAction[run.id] === "run" || runAction[run.id] === "resume")
        .forEach((run) => void refreshRun(run.id));
    }, 2000);
    return () => window.clearInterval(interval);
  }, [authState, runs, runAction]);

  async function createRun(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const value = prompt.trim();
    if (!value) {
      setGlobalError("请填写考核问题。未创建任务。");
      return;
    }
    setBusy(true);
    setGlobalError("");
    try {
      const run = await assessmentApi.createRun(value, selectedRun?.status === "completed" ? selectedRun.id : undefined);
      replaceRun(run);
      setSelectedRunId(run.id);
      setPrompt("");
    } catch (error) {
      showRequestError(error);
    } finally {
      setBusy(false);
    }
  }

  async function performRunAction(run: AssessmentRun, action: RunAction) {
    setRunErrors((current) => {
      const next = { ...current };
      delete next[run.id];
      return next;
    });
    setRunAction((current) => ({ ...current, [run.id]: action }));
    try {
      replaceRun(await assessmentApi.runAction(run.id, action));
    } catch (error) {
      const message = errorMessage(error);
      setRunErrors((current) => ({ ...current, [run.id]: message }));
      showRequestError(error);
      void refreshRun(run.id);
    } finally {
      setRunAction((current) => {
        if (current[run.id] !== action) return current;
        const next = { ...current };
        delete next[run.id];
        return next;
      });
    }
  }

  function changeScope(nextScope: AssessmentScope) {
    setMetrics(null);
    setMetricsError("");
    setScope(nextScope);
  }

  function toggleEmployee(employeeId: number) {
    setSelectedEmployeeIds((current) => current.includes(employeeId)
      ? current.filter((id) => id !== employeeId)
      : [...current, employeeId]);
  }

  async function saveAccess(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setAccessSaving(true);
    setAccessError("");
    const rawUserId = accessUserId.trim();
    const userId: string | number = /^\d+$/.test(rawUserId) ? Number(rawUserId) : rawUserId;
    const update: AssessmentAccessUpdate = {
      user_id: userId,
      level: accessLevel,
      employee_id: accessEmployeeId ? Number(accessEmployeeId) : null,
      managed_employee_ids: accessLevel === "manager" ? managedEmployeeIds : [],
    };
    try {
      const entry = await assessmentApi.updateAccess(update);
      setAccessEntries((current) => [
        entry,
        ...current.filter((item) => String(item.user_id) !== String(entry.user_id)),
      ]);
    } catch (error) {
      setAccessError(errorMessage(error));
      showRequestError(error);
    } finally {
      setAccessSaving(false);
    }
  }

  function loadAccessEntry(userId: string) {
    const entry = accessEntries.find((item) => String(item.user_id) === userId);
    if (!entry) {
      setAccessUserId(userId);
      setAccessLevel("employee");
      setAccessEmployeeId("");
      setManagedEmployeeIds([]);
      return;
    }
    setAccessUserId(String(entry.user_id));
    setAccessLevel(entry.level ?? "employee");
    setAccessEmployeeId(entry.employee_id === null ? "" : String(entry.employee_id));
    setManagedEmployeeIds(entry.managed_employee_ids);
  }

  if (authState !== "authenticated" || !identity) {
    return (
      <LoginScreen
        authState={authState}
        message={authMessage}
        onLogin={login}
        onRetry={() => void refreshSession()}
      />
    );
  }

  const selectedMetricsEmployee = metrics?.employees.find(
    (employee) => employee.employee_id === selectedEmployeeId,
  ) ?? null;
  const visibleMetricsEmployees = metrics?.employees ?? [];
  const canSelectEmployee = identity.level !== "employee";
  const scopeOptions: { value: AssessmentScope; label: string }[] =
    identity.level === "employee"
      ? [{ value: "self", label: "本人" }]
      : identity.level === "manager"
        ? [
            { value: "self", label: "本人" },
            { value: "team", label: "管理团队" },
            { value: "employees", label: "指定员工" },
          ]
        : [{ value: "organization", label: "全体员工" }];

  return (
    <div className="assessment-workspace">
      <header className="assessment-topbar">
        <div className="assessment-brand-lockup">
          <span className="assessment-brand-mark">A</span>
          <span>
            <strong>销售考核</strong>
            <small>Assessment workspace</small>
          </span>
        </div>
        <div className="assessment-topbar-actions">
          <span className="assessment-sample-chip">官方模拟样例</span>
          <span className="assessment-identity-chip">
            {levelLabels[identity.level]} · {identity.employee_id ?? `账号 ${identity.user_id}`}
          </span>
          <button className="assessment-button assessment-button-quiet" onClick={() => void logout()}>
            退出登录
          </button>
        </div>
      </header>

      <div className="assessment-page-heading">
        <div>
          <p className="assessment-kicker">销售数据与问答</p>
          <h1>考核工作台</h1>
        </div>
        <div className="assessment-period-card">
          <span>统计期间</span>
          <strong>2023-07-01 — 2023-09-30</strong>
          <small>样例计算，币种未核实</small>
        </div>
      </div>

      {identity.notice && <div className="assessment-notice assessment-notice-info">{identity.notice}</div>}
      {globalError && (
        <div className="assessment-notice assessment-notice-error" role="alert">
          <span>{globalError}</span>
          <button aria-label="关闭提示" onClick={() => setGlobalError("")}>×</button>
        </div>
      )}

      <section className={`assessment-source-banner ${dataStatus?.status === "DATA_BLOCKED" ? "assessment-source-blocked" : ""}`}>
        <div className="assessment-source-indicator" />
        <div className="assessment-source-copy">
          <strong>{dataStatus?.status === "DATA_BLOCKED" ? "数据暂不可用" : dataStatus ? "来源与计算规则" : "正在读取来源状态"}</strong>
          <span>
            {dataStatus?.status === "DATA_BLOCKED"
              ? dataStatus.reason || "DATA_BLOCKED：服务未提供可验证的完整数据。"
              : dataStatus
                ? `${dataStatus.source_id} · ${dataStatus.revision}`
                : "正在检查来源版本与完整性。"}
          </span>
        </div>
        <span className="assessment-source-period">[2023-07-01, 2023-10-01)</span>
      </section>
      {dataStatus?.notice && <p className="assessment-source-notice">{dataStatus.notice}</p>}

      {dataStatus?.status === "DATA_BLOCKED" && (
        <section className="assessment-blocked-panel" role="status">
          <span className="assessment-blocked-icon">!</span>
          <div>
            <strong>当前无法提供考核指标</strong>
            <p>{dataStatus.reason || "DATA_BLOCKED：来源完整性尚未确认。"}</p>
            <small>系统不会用默认数值或生成内容替代缺失数据。</small>
          </div>
        </section>
      )}

      <div className="assessment-main-grid">
        <aside className="assessment-sidebar">
          <section className="assessment-panel assessment-scope-panel">
            <div className="assessment-panel-heading">
              <div>
                <p className="assessment-kicker">数据范围</p>
                <h2>查看指标</h2>
              </div>
              <span className="assessment-scope-level">{levelLabels[identity.level]}</span>
            </div>
            <label className="assessment-field-label" htmlFor="assessment-scope">当前范围</label>
            <select
              id="assessment-scope"
              value={scope}
              onChange={(event) => changeScope(event.target.value as AssessmentScope)}
              disabled={identity.level === "employee" || !dataReady}
            >
              {scopeOptions.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>

            {scope === "employees" && (
              <fieldset className="assessment-employee-picker">
                <legend>选择允许查看的员工</legend>
                {authorizedEmployees.map((employee) => (
                  <label key={employee.employee_id}>
                    <input
                      type="checkbox"
                      checked={selectedEmployeeIds.includes(employee.employee_id)}
                      onChange={() => toggleEmployee(employee.employee_id)}
                    />
                    <span>{employeeName(employee)}</span>
                  </label>
                ))}
                {authorizedEmployees.length === 0 && <p>服务端没有返回可选择的员工。</p>}
              </fieldset>
            )}

            <div className="assessment-scope-summary">
              <span>本人员工编号</span>
              <strong>{identity.employee_id ?? "未绑定"}</strong>
              <span>管理范围编号</span>
              <strong>{identity.managed_employee_ids.length ? identity.managed_employee_ids.join("、") : "服务端未返回"}</strong>
            </div>
            <button
              className="assessment-button assessment-button-secondary assessment-button-wide"
              onClick={() => void refreshMetrics()}
              disabled={!dataReady || metricsLoading}
            >
              {metricsLoading ? "正在读取…" : "刷新指标"}
            </button>
          </section>

          <section className="assessment-panel assessment-employee-panel">
            <div className="assessment-panel-heading">
              <div>
                <p className="assessment-kicker">依据查询</p>
                <h2>员工来源记录</h2>
              </div>
              <span className="assessment-count">{authorizedEmployees.length}</span>
            </div>
            <label className="assessment-field-label" htmlFor="assessment-employee">员工</label>
            {canSelectEmployee ? (
              <select
                id="assessment-employee"
                value={selectedEmployeeId ?? ""}
                onChange={(event) => setSelectedEmployeeId(event.target.value ? Number(event.target.value) : null)}
              >
                <option value="">选择员工</option>
                {authorizedEmployees.map((employee) => (
                  <option key={employee.employee_id} value={employee.employee_id}>{employeeName(employee)}</option>
                ))}
              </select>
            ) : (
              <div className="assessment-locked-employee">
                {identity.employee_id ?? "账号尚未绑定员工"}
              </div>
            )}
            {!canSelectEmployee && identity.employee_id === null && (
              <p className="assessment-inline-error">此账号尚未绑定员工编号。</p>
            )}
          </section>

          <section className="assessment-panel assessment-run-list-panel">
            <div className="assessment-panel-heading">
              <div>
                <p className="assessment-kicker">任务记录</p>
                <h2>历史任务</h2>
              </div>
              <span className="assessment-count">{runs.length}</span>
            </div>
            {runs.length === 0 ? (
              <div className="assessment-empty-list">
                <span>＋</span>
                <p>还没有考核任务</p>
                <small>提交一个问题后，任务会显示在这里。</small>
              </div>
            ) : (
              <nav className="assessment-run-list" aria-label="考核任务历史">
                {runs.map((run) => (
                  <button
                    key={run.id}
                    className={`assessment-run-item ${run.id === selectedRunId ? "assessment-run-item-selected" : ""}`}
                    onClick={() => setSelectedRunId(run.id)}
                  >
                    <span className="assessment-run-item-top">
                      <StatusBadge status={run.status} />
                      <span>{durationText(run.elapsed_seconds)}</span>
                    </span>
                    <strong>{run.prompt}</strong>
                    <small>{run.model_calls} 次模型请求 · {run.tool_calls} 次工具调用</small>
                  </button>
                ))}
              </nav>
            )}
          </section>
        </aside>

        <main className="assessment-content">
          <section className="assessment-panel assessment-metrics-panel">
            <div className="assessment-panel-heading assessment-metrics-heading">
              <div>
                <p className="assessment-kicker">固定计算指标</p>
                <h2>{scopeOptions.find((item) => item.value === scope)?.label ?? "考核范围"}表现</h2>
              </div>
              {metrics && <span className="assessment-rule-tag">规则 {metrics.rule_version}</span>}
            </div>
            {metricsError && <div className="assessment-inline-error" role="alert">{metricsError}</div>}
            {!metrics && metricsLoading && <div className="assessment-data-placeholder">正在向服务端读取指标。</div>}
            {!metrics && !metricsLoading && !metricsError && dataReady && (
              <div className="assessment-data-placeholder">尚未取得指标数据。</div>
            )}
            {metrics && (
              <>
                <p className="assessment-data-notice">{metrics.notice}</p>
                <div className="assessment-metrics-meta">
                  <span>期间 {metrics.period_start} 至 {metrics.period_end}（上限不包含）</span>
                  <span>来源 {String(metrics.source.source_id ?? dataStatus?.source_id ?? "服务端记录")}</span>
                </div>
                {scope !== "self" && (
                  <article className="assessment-employee-metrics">
                    <h3>{metrics.summary.label}</h3>
                    <div className="assessment-metric-grid">
                      {metrics.summary.metrics.map((metric) => (
                        <div className="assessment-metric-card" key={metric.metric_id}>
                          <span>{metric.label}</span><strong>{metric.display}</strong>
                          <small>{metric.status}</small><code>{metric.evidence_id}</code>
                        </div>
                      ))}
                    </div>
                  </article>
                )}
                {visibleMetricsEmployees.length === 0 ? (
                  <div className="assessment-data-placeholder">服务端未返回此范围的员工指标。</div>
                ) : (
                  <div className="assessment-employee-metrics-list">
                    {visibleMetricsEmployees.map((employee) => (
                      <article className="assessment-employee-metrics" key={employee.employee_id}>
                        <div className="assessment-employee-metrics-heading">
                          <div>
                            <span>员工 {employee.employee_id}</span>
                            <strong>{employee.job_title}</strong>
                          </div>
                          <button
                            className="assessment-text-button"
                            onClick={() => {
                              setSelectedEmployeeId(employee.employee_id);
                            }}
                          >
                            查看依据
                          </button>
                        </div>
                        <div className="assessment-metric-grid">
                          {employee.metrics.map((metric) => (
                            <div className="assessment-metric-card" key={metric.metric_id}>
                              <span>{metric.label}</span>
                              <strong>{metric.display}</strong>
                              <small>{metric.status}</small>
                              {metric.evidence_id && <code>{metric.evidence_id}</code>}
                            </div>
                          ))}
                        </div>
                        {employee.quota && (
                          <details className="assessment-source-details">
                            <summary>季度目标记录</summary><pre>{jsonText(employee.quota)}</pre>
                          </details>
                        )}
                      </article>
                    ))}
                  </div>
                )}
                {metrics.unassigned && (
                  <details className="assessment-source-details">
                    <summary>未分配订单统计</summary>
                    <pre>{jsonText(metrics.unassigned)}</pre>
                  </details>
                )}
                <details className="assessment-source-details">
                  <summary>来源与规则信息</summary>
                  <pre>{jsonText(metrics.source)}</pre>
                </details>
              </>
            )}
          </section>

          <section className="assessment-panel assessment-evidence-panel">
            <div className="assessment-panel-heading">
              <div>
                <p className="assessment-kicker">原始依据</p>
                <h2>{selectedEmployeeId === null ? "选择员工后查看订单与目标" : `员工 ${selectedEmployeeId} 的依据`}</h2>
              </div>
              {evidence && <span className="assessment-count">{evidence.orders.length} 笔订单</span>}
            </div>
            {evidenceError && <div className="assessment-inline-error" role="alert">{evidenceError}</div>}
            {evidenceLoading && <div className="assessment-data-placeholder">正在读取来源依据。</div>}
            {evidence && !evidenceLoading && (
              <>
                <p className="assessment-data-notice">订单和目标均来自服务端返回的来源记录。</p>
                {evidence.orders.length === 0 ? (
                  <div className="assessment-data-placeholder">来源没有返回此员工的期间订单。</div>
                ) : (
                  <div className="assessment-table-scroll">
                    <table className="assessment-evidence-table">
                      <thead>
                        <tr>
                          <th>订单编号</th>
                          <th>订单日期</th>
                          <th>小计</th>
                          <th>状态</th>
                          <th>来源行</th>
                        </tr>
                      </thead>
                      <tbody>
                        {evidence.orders.map((order) => (
                          <tr key={`${order.order_id}-${order.source_line}`}>
                            <td>{order.order_id}</td>
                            <td>{order.order_date}</td>
                            <td>{order.subtotal}</td>
                            <td>{order.status}</td>
                            <td>{order.source_line}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                <div className="assessment-evidence-lower-grid">
                  <section>
                    <h3>季度目标</h3>
                    {evidence.quota
                      ? <pre>{jsonText(evidence.quota)}</pre>
                      : <p>服务端没有返回此员工的季度目标。</p>}
                  </section>
                  <section>
                    <h3>来源信息</h3>
                    <pre>{jsonText(evidence.source)}</pre>
                  </section>
                </div>
              </>
            )}
          </section>

          <section className="assessment-panel assessment-composer-panel">
            <div className="assessment-panel-heading">
              <div>
                <p className="assessment-kicker">中文考核问答</p>
                <h2>{selectedRun?.status === "completed" ? "继续追问" : "创建考核任务"}</h2>
              </div>
              {selectedRun?.status === "completed" && <span className="assessment-parent-tag">将关联当前任务</span>}
            </div>
            <form className="assessment-composer" onSubmit={createRun}>
              <label className="assessment-field-label" htmlFor="assessment-prompt">
                {selectedRun?.status === "completed" ? "补充问题" : "考核问题"}
              </label>
              <textarea
                id="assessment-prompt"
                value={prompt}
                onChange={(event) => setPrompt(event.target.value)}
                placeholder="例如：请说明本季度个人销售额与目标完成情况，并引用对应来源依据。"
                rows={4}
                required
              />
              <div className="assessment-composer-footer">
                <span>{selectedRun?.status === "completed" ? `新任务会引用报告 ${selectedRun.run_id}` : "任务结果由服务端按当前权限和来源计算"}</span>
                <button
                  className="assessment-button assessment-button-primary"
                  type="submit"
                  disabled={busy || !dataReady}
                >
                  {busy ? "正在创建…" : selectedRun?.status === "completed" ? "创建追问" : "创建任务"}
                </button>
              </div>
              {!dataReady && <p className="assessment-inline-error">数据状态可用前，暂不能创建考核任务。</p>}
            </form>
          </section>

          {selectedRun && (
            <section className="assessment-panel assessment-run-detail-panel">
              <div className="assessment-panel-heading assessment-run-detail-heading">
                <div>
                  <p className="assessment-kicker">任务详情</p>
                  <h2>{selectedRun.prompt}</h2>
                  <span className="assessment-run-id">运行编号 {selectedRun.run_id}</span>
                </div>
                <div className="assessment-run-controls">
                  <StatusBadge status={selectedRun.status} />
                  {selectedRun.status === "created" && (
                    <button className="assessment-button assessment-button-primary" onClick={() => void performRunAction(selectedRun, "run")} disabled={Boolean(runAction[selectedRun.id]) || !dataReady}>
                      {runAction[selectedRun.id] === "run" ? "启动中…" : "启动任务"}
                    </button>
                  )}
                  {selectedRun.status === "running" && (
                    <button className="assessment-button assessment-button-danger" onClick={() => void performRunAction(selectedRun, "cancel")} disabled={runAction[selectedRun.id] === "cancel" || selectedRun.cancel_requested}>
                      {runAction[selectedRun.id] === "cancel" ? "正在取消…" : "取消任务"}
                    </button>
                  )}
                  {(selectedRun.status === "paused" || selectedRun.status === "stopped") && (
                    <button className="assessment-button assessment-button-secondary" onClick={() => void performRunAction(selectedRun, "resume")} disabled={Boolean(runAction[selectedRun.id]) || !dataReady}>
                      {runAction[selectedRun.id] === "resume" ? "正在恢复…" : "恢复任务"}
                    </button>
                  )}
                  {selectedRun.status === "completed" && selectedRun.result && (
                    <>
                      <a className="assessment-button assessment-button-secondary" href={assessmentApi.exportUrl(selectedRun.id, "html")} download>
                        下载 HTML
                      </a>
                      <a className="assessment-button assessment-button-secondary" href={assessmentApi.exportUrl(selectedRun.id, "csv")} download>
                        下载 CSV
                      </a>
                    </>
                  )}
                </div>
              </div>

              {runErrors[selectedRun.id] && <div className="assessment-inline-error" role="alert">{runErrors[selectedRun.id]}</div>}
              {selectedRun.error && <div className="assessment-run-error" role="alert">{selectedRun.error}</div>}
              {selectedRun.stop_reason && <p className="assessment-run-note">停止原因：{selectedRun.stop_reason}</p>}

              <div className="assessment-run-stats">
                <div><span>状态</span><strong>{runStatusLabels[selectedRun.status]}</strong></div>
                <div><span>累计时间</span><strong>{durationText(selectedRun.elapsed_seconds)}</strong></div>
                <div><span>模型请求</span><strong>{selectedRun.model_calls}</strong></div>
                <div><span>工具调用</span><strong>{selectedRun.tool_calls}</strong></div>
              </div>
              {selectedRun.cancel_requested && <p className="assessment-run-note">服务端已记录取消请求。</p>}

              {selectedRun.result && (
                <div className="assessment-result-block">
                  <p className="assessment-kicker">服务端结果</p>
                  <h3>{selectedRun.result.headline}</h3>
                  {selectedRun.result.notice && <p className="assessment-data-notice">{selectedRun.result.notice}</p>}
                  {selectedRun.result.observations.length > 0 && (
                    <div className="assessment-observations">
                      {selectedRun.result.observations.map((observation, index) => (
                        <article key={`${observation.evidence_id}-${index}`}>
                          <p>{observation.text}</p>
                          <button
                            className="assessment-text-button"
                            onClick={() => {
                              if (observation.employee_id !== null) {
                                setSelectedEmployeeId(observation.employee_id);
                              }
                            }}
                            disabled={observation.employee_id === null}
                          >
                            员工 {observation.employee_id ?? "未指定"} · 依据 {observation.evidence_id}
                          </button>
                        </article>
                      ))}
                    </div>
                  )}
                  {!!selectedRun.result.comparisons?.length && (
                    <div className="assessment-observations">
                      {selectedRun.result.comparisons.map((comparison, comparisonIndex) => (
                        <article key={`${comparison.metric_id}-${comparisonIndex}`}>
                          <h4>{comparison.label}比较 · 员工 {comparison.employee_ids.join("、")}</h4>
                          <p>{comparison.text}</p>
                          {comparison.entries.length > 0 && (
                            <div className="assessment-table-scroll">
                              <table className="assessment-request-table">
                                <thead>
                                  <tr>
                                    <th>排名</th>
                                    <th>员工编号</th>
                                    <th>指标</th>
                                    <th>状态</th>
                                    <th>依据</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {comparison.entries.map((entry, entryIndex) => (
                                    <tr key={`${entry.employee_id}-${entry.evidence_id}-${entryIndex}`}>
                                      <td>{entry.rank ?? "未排名"}</td>
                                      <td>{entry.employee_id}</td>
                                      <td>{entry.display}</td>
                                      <td>{entry.status}</td>
                                      <td>{entry.evidence_id}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          )}
                        </article>
                      ))}
                    </div>
                  )}
                  {selectedRun.result.suggestions.length > 0 && (
                    <div className="assessment-suggestions">
                      <h4>建议</h4>
                      <ul>{selectedRun.result.suggestions.map((suggestion, index) => <li key={`${index}-${suggestion}`}>{suggestion}</li>)}</ul>
                    </div>
                  )}
                  {selectedRun.result.evidence && (
                    <details className="assessment-source-details">
                      <summary>本报告保存的订单与目标依据</summary>
                      <pre>{jsonText(selectedRun.result.evidence)}</pre>
                    </details>
                  )}
                </div>
              )}

              {selectedRun.metrics && (
                <details className="assessment-source-details">
                  <summary>本次任务使用的指标快照</summary>
                  <pre>{jsonText(selectedRun.metrics)}</pre>
                </details>
              )}

              <section className="assessment-billing-section">
                <div className="assessment-section-subheading">
                  <div>
                    <p className="assessment-kicker">请求记录</p>
                    <h3>费用与调用</h3>
                  </div>
                  <span>已发生费用与预留费用分别显示</span>
                </div>
                {!selectedRun.requests?.length ? (
                  <p className="assessment-data-placeholder">服务端尚未返回请求和费用记录。</p>
                ) : (
                  <div className="assessment-table-scroll">
                    <table className="assessment-request-table">
                      <thead><tr><th>步骤</th><th>模型</th><th>状态</th><th>已发生费用</th><th>预留费用</th></tr></thead>
                      <tbody>
                        {selectedRun.requests.map((request, index) => (
                          <tr key={`${request.step_key}-${index}`}>
                            <td>{request.step_key}</td>
                            <td>{request.model}</td>
                            <td>{request.status}</td>
                            <td>{request.charged_cny ?? "服务端未返回"}</td>
                            <td>{request.reservation_cny}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                {selectedRun.requests?.map((request, index) => request.usage && (
                  <details className="assessment-source-details" key={`usage-${request.step_key}-${index}`}>
                    <summary>{request.step_key} 用量字段</summary>
                    <pre>{jsonText(request.usage)}</pre>
                  </details>
                ))}
              </section>
            </section>
          )}
        </main>
      </div>

      {identity.level === "admin" && (
        <section className="assessment-admin-section">
          <details>
            <summary>管理员：考核身份与团队范围</summary>
            <div className="assessment-admin-content">
              <p className="assessment-admin-notice">权限由服务端保存并在每次请求时校验。此表单只提交账号、级别、员工绑定和管理范围。</p>
              {accessError && <div className="assessment-inline-error" role="alert">{accessError}</div>}
              <div className="assessment-access-existing">
                <h3>已配置账号</h3>
                {accessEntries.length === 0 ? <p>服务端没有返回已配置账号。</p> : (
                  <div className="assessment-table-scroll">
                    <table className="assessment-request-table">
                      <thead><tr><th>账号</th><th>级别</th><th>员工编号</th><th>管理范围</th><th></th></tr></thead>
                      <tbody>
                        {accessEntries.map((entry) => (
                          <tr key={String(entry.user_id)}>
                            <td>{entry.username} · {entry.user_id}</td>
                            <td>{entry.level ? levelLabels[entry.level] : "未配置"}</td>
                            <td>{entry.employee_id ?? "未绑定"}</td>
                            <td>{entry.managed_employee_ids.join("、") || "无"}</td>
                            <td><button className="assessment-text-button" onClick={() => loadAccessEntry(String(entry.user_id))}>编辑</button></td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
              <form className="assessment-access-form" onSubmit={saveAccess}>
                <label className="assessment-field-label">
                  账号 ID
                  <input value={accessUserId} onChange={(event) => setAccessUserId(event.target.value)} required />
                </label>
                <label className="assessment-field-label">
                  身份级别
                  <select value={accessLevel} onChange={(event) => setAccessLevel(event.target.value as AssessmentLevel)}>
                    <option value="employee">员工</option>
                    <option value="manager">主管</option>
                    <option value="admin">管理员</option>
                  </select>
                </label>
                <label className="assessment-field-label">
                  绑定员工编号
                  <select value={accessEmployeeId} onChange={(event) => setAccessEmployeeId(event.target.value)}>
                    <option value="">不绑定</option>
                    {employees.map((employee) => <option key={employee.employee_id} value={employee.employee_id}>{employeeName(employee)}</option>)}
                  </select>
                </label>
                {accessLevel === "manager" && (
                  <fieldset className="assessment-employee-picker assessment-managed-picker">
                    <legend>明确授权的管理成员</legend>
                    {employees.map((employee) => (
                      <label key={employee.employee_id}>
                        <input
                          type="checkbox"
                          checked={managedEmployeeIds.includes(employee.employee_id)}
                          onChange={() => setManagedEmployeeIds((current) => current.includes(employee.employee_id)
                            ? current.filter((id) => id !== employee.employee_id)
                            : [...current, employee.employee_id])}
                        />
                        <span>{employeeName(employee)}</span>
                      </label>
                    ))}
                  </fieldset>
                )}
                <button className="assessment-button assessment-button-primary" type="submit" disabled={accessSaving}>
                  {accessSaving ? "正在保存…" : "保存权限配置"}
                </button>
              </form>
            </div>
          </details>
        </section>
      )}

      {selectedMetricsEmployee && selectedEmployeeId !== null && (
        <span className="assessment-selection-live" aria-live="polite">
          当前依据员工：{selectedMetricsEmployee.employee_id} · {selectedMetricsEmployee.job_title}
        </span>
      )}
    </div>
  );
}

export default AssessmentWorkspace;
