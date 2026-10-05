import { useEffect, useState } from "react";
import { dataCenterApi, DataCenterApiError } from "./api";
import type { DataCenterResult, DataCenterSource, DataCenterTask } from "./api";
import "./styles.css";

type AuthState = "checking" | "anonymous" | "authenticated" | "forbidden" | "unavailable";
type Section = "catalog" | "ask" | "metrics" | "tasks" | "sources";

const sections: Array<{ id: Section; label: string }> = [
  { id: "catalog", label: "资料目录" },
  { id: "ask", label: "中文问答" },
  { id: "metrics", label: "指标定义" },
  { id: "tasks", label: "任务记录" },
  { id: "sources", label: "来源管理" },
];

function errorText(error: unknown): string {
  if (error instanceof DataCenterApiError) {
    if (error.status === 401) {
      return "登录会话已过期，请重新登录。";
    }
    if (error.status === 403) {
      return "当前账号没有执行此项操作的权限。";
    }
    return `服务返回 HTTP ${error.status}：${error.message}`;
  }
  return error instanceof Error && error.message ? error.message : "请求未能完成。";
}

function pretty(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function isDataCenterTask(value: unknown): value is DataCenterTask {
  return typeof value === "object" && value !== null
    && "task_id" in value && typeof value.task_id === "string"
    && "run_id" in value && typeof value.run_id === "string"
    && "operation" in value && typeof value.operation === "string"
    && "status" in value && typeof value.status === "string"
    && "input_sha256" in value && typeof value.input_sha256 === "string";
}

function taskFromResult(task: DataCenterTask, result: DataCenterResult): DataCenterTask {
  if (isDataCenterTask(result.data)) {
    return result.data;
  }
  return {
    ...task,
    run_id: result.run_id ?? task.run_id,
    status: result.status,
    result,
  };
}

function LoginPanel({
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
  const [formError, setFormError] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setFormError("");
    try {
      await onLogin(username, password);
    } catch (error) {
      setFormError(errorText(error));
    } finally {
      setSubmitting(false);
    }
  }

  if (authState === "checking") {
    return <main className="dc-auth"><section><p className="dc-eyebrow">DATA CENTER</p><h1>正在读取登录状态</h1><p>正在检查当前登录会话。</p></section></main>;
  }
  if (authState === "unavailable" || authState === "forbidden") {
    return <main className="dc-auth"><section><p className="dc-eyebrow">DATA CENTER</p><h1>{authState === "forbidden" ? "当前账号无法访问" : "暂时无法连接服务"}</h1><p role="alert">{message}</p><button type="button" onClick={onRetry}>重新连接</button></section></main>;
  }
  return (
    <main className="dc-auth">
      <section>
        <p className="dc-eyebrow">DATA CENTER</p>
        <h1>登录数据中台</h1>
        <p>使用 Shop 登录账号访问当前授权范围内的资料、指标和任务。</p>
        <form onSubmit={submit} className="dc-login-form">
          <label>用户名<input autoComplete="username" value={username} onChange={(event) => setUsername(event.target.value)} required /></label>
          <label>密码<input type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} required /></label>
          {(formError || message) && <p role="alert" className="dc-error">{formError || message}</p>}
          <button type="submit" disabled={submitting}>{submitting ? "正在登录…" : "登录"}</button>
        </form>
      </section>
    </main>
  );
}

export default function DataCenterWorkspace() {
  const [authState, setAuthState] = useState<AuthState>("checking");
  const [authMessage, setAuthMessage] = useState("");
  const [section, setSection] = useState<Section>("catalog");
  const [sources, setSources] = useState<DataCenterSource[]>([]);
  const [tasks, setTasks] = useState<DataCenterTask[]>([]);
  const [metrics, setMetrics] = useState<Array<Record<string, unknown>>>([]);
  const [answer, setAnswer] = useState<Record<string, unknown> | null>(null);
  const [selectedSources, setSelectedSources] = useState<string[]>([]);
  const [selectedTask, setSelectedTask] = useState<DataCenterTask | null>(null);
  const [referenceData, setReferenceData] = useState<unknown>(null);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [question, setQuestion] = useState("");
  const [retrievalMode, setRetrievalMode] = useState("hybrid");
  const [sourceJson, setSourceJson] = useState("");
  const [grantActor, setGrantActor] = useState("");
  const [grantSource, setGrantSource] = useState("");
  const [grantPermissions, setGrantPermissions] = useState("read");
  const [grantAssets, setGrantAssets] = useState("");
  const [sourceWide, setSourceWide] = useState(false);

  async function loadCatalog() {
    setSources(await dataCenterApi.catalog());
  }

  async function loadTasks() {
    const current = await dataCenterApi.tasks();
    setTasks(current);
    return current;
  }

  async function refreshSession(): Promise<boolean> {
    setAuthState("checking");
    setAuthMessage("");
    try {
      const results = await Promise.all([dataCenterApi.catalog(), dataCenterApi.tasks()]);
      setSources(results[0]);
      setTasks(results[1]);
      setSelectedSources((current) => current.filter((value) => results[0].some((item) => item.source_id === value)));
      setAuthState("authenticated");
      setError("");
      return true;
    } catch (caught) {
      setAuthMessage(errorText(caught));
      setAuthState(caught instanceof DataCenterApiError && caught.status === 401
        ? "anonymous"
        : caught instanceof DataCenterApiError && caught.status === 403 ? "forbidden" : "unavailable");
      return false;
    }
  }

  useEffect(() => {
    void refreshSession();
  }, []);

  async function login(username: string, password: string) {
    await dataCenterApi.login(username, password);
    const connected = await refreshSession();
    if (!connected) {
      throw new Error("登录后无法读取资料目录。");
    }
  }

  async function logout() {
    await dataCenterApi.logout();
    setSources([]);
    setTasks([]);
    setMetrics([]);
    setAnswer(null);
    setSelectedTask(null);
    setAuthState("anonymous");
  }

  async function runAction(action: string, payload: Record<string, unknown> = {}) {
    setBusy(true);
    setNotice("");
    setError("");
    try {
      const result = await dataCenterApi.operation<Record<string, unknown>>(action, payload);
      setNotice(`操作 ${action} 已返回状态：${result.status}`);
      if (action === "ask") {
        setAnswer(result.data as Record<string, unknown>);
        await loadTasks();
      }
      if (action === "run" || action === "cancel") {
        const currentTasks = await loadTasks();
        if (typeof payload.task_id === "string") {
          const task = currentTasks.find((item) => item.task_id === payload.task_id)
            ?? selectedTask;
          if (!task || task.task_id !== payload.task_id) {
            throw new Error("任务列表中没有操作目标。");
          }
          const detail = await dataCenterApi.operation("show", { task_id: payload.task_id });
          setSelectedTask(taskFromResult(task, detail));
        }
      }
      if (action === "manage_sources") {
        const data = result.data as { items?: DataCenterSource[] } | DataCenterSource[];
        const managed = Array.isArray(data) ? data : data.items ?? [];
        setSources(managed);
      }
      if (action === "register_source" || action === "withdraw_source") {
        await runAction("manage_sources");
      }
      return result;
    } catch (caught) {
      if (caught instanceof DataCenterApiError && caught.status === 401) {
        setAuthState("anonymous");
      }
      setError(errorText(caught));
      return null;
    } finally {
      setBusy(false);
    }
  }

  async function showTask(task: DataCenterTask) {
    setReferenceData(null);
    setError("");
    try {
      const result = await dataCenterApi.operation("show", { task_id: task.task_id });
      setSelectedTask(taskFromResult(task, result));
    } catch (caught) {
      setError(errorText(caught));
    }
  }

  async function showReference(reference: unknown) {
    setReferenceData(null);
    setError("");
    try {
      const result = await dataCenterApi.operation("reference", reference as Record<string, unknown>);
      setReferenceData(result.data);
    } catch (caught) {
      setError(errorText(caught));
    }
  }

  async function ask(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const result = await runAction("ask", {
      question: question.trim(),
      source_ids: selectedSources,
      retrieval_mode: retrievalMode,
    });
    if (result) {
      setQuestion("");
    }
  }

  async function openMetrics() {
    setSection("metrics");
    setBusy(true);
    setError("");
    try {
      setMetrics(await dataCenterApi.metrics());
    } catch (caught) {
      setError(errorText(caught));
    } finally {
      setBusy(false);
    }
  }

  async function exportTask(taskId: string, format: "markdown" | "csv") {
    setBusy(true);
    setError("");
    try {
      const { blob, filename } = await dataCenterApi.download(taskId, format);
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      link.click();
      URL.revokeObjectURL(url);
    } catch (caught) {
      setError(errorText(caught));
    } finally {
      setBusy(false);
    }
  }

  async function registerSource(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    let source: Record<string, unknown>;
    try {
      source = JSON.parse(sourceJson) as Record<string, unknown>;
    } catch {
      setError("来源登记内容必须是有效 JSON。");
      return;
    }
    const result = await runAction("register_source", source);
    if (result) {
      setSourceJson("");
    }
  }

  async function setGrant(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await runAction("set_grant", {
      actor_id: grantActor.trim(),
      source_id: grantSource.trim(),
      permissions: grantPermissions.split(",").map((value) => value.trim()).filter(Boolean),
      asset_ids: grantAssets.split(",").map((value) => value.trim()).filter(Boolean),
      source_wide: sourceWide,
    });
  }

  if (authState !== "authenticated") {
    return <LoginPanel authState={authState} message={authMessage} onLogin={login} onRetry={() => void refreshSession()} />;
  }

  const selectedData = selectedTask?.result?.data;
  const selectedEvidence = selectedData && typeof selectedData === "object" && "evidence" in selectedData
    && Array.isArray(selectedData.evidence) ? selectedData.evidence as Array<Record<string, unknown>> : [];
  const selectedRows = selectedData && typeof selectedData === "object" && "rows" in selectedData
    && Array.isArray(selectedData.rows) ? selectedData.rows : [];
  const canExport = selectedTask?.result !== null && selectedTask?.result !== undefined
    && ["completed", "partial"].includes(selectedTask.status);

  return (
    <main className="dc-shell">
      <header className="dc-header">
        <div><p className="dc-eyebrow">DATA CENTER</p><h1>数据中台</h1><p>查看授权资料、经营指标和可追溯答案。</p></div>
        <button type="button" className="dc-button dc-button-light" onClick={() => void logout()}>退出登录</button>
      </header>
      <nav className="dc-tabs" aria-label="数据中台功能">
        {sections.map((item) => <button key={item.id} type="button" aria-current={section === item.id ? "page" : undefined}
          className={section === item.id ? "dc-tab dc-tab-active" : "dc-tab"}
          onClick={() => {
            setSection(item.id);
            setError("");
            if (item.id === "sources") void runAction("manage_sources");
            if (item.id === "metrics") void openMetrics();
            if (item.id === "tasks") void loadTasks().catch((caught: unknown) => setError(errorText(caught)));
          }}>{item.label}</button>)}
      </nav>
      <div className="dc-content">
        {(notice || error) && <p className={error ? "dc-notice dc-error" : "dc-notice"} role={error ? "alert" : "status"}>{error || notice}</p>}
        {section === "catalog" && <section>
          <div className="dc-section-heading"><div><p className="dc-eyebrow">授权范围</p><h2>资料目录</h2></div>
            <button type="button" className="dc-button" disabled={busy} onClick={() => void loadCatalog().catch((caught: unknown) => setError(errorText(caught)))}>刷新目录</button></div>
          {sources.length ? <div className="dc-card-grid">{sources.map((source) => <article className="dc-card" key={source.source_id}>
            <div className="dc-card-top"><span className="dc-chip">{source.kind}</span><span className="dc-source-state">{source.active !== false ? "可用" : "已撤回"}</span></div>
            <h3>{source.title}</h3><p className="dc-source-id">{source.source_id}</p>
            <dl><div><dt>版本</dt><dd>{source.revision}</dd></div><div><dt>来源性质</dt><dd>{source.nature}</dd></div>
              <div><dt>许可</dt><dd>{source.license_id}</dd></div></dl>
            <p className="dc-digest">SHA-256 {source.sha256}</p>
          </article>)}</div> : <p className="dc-empty">当前账号没有可查看的资料来源。</p>}
        </section>}
        {section === "ask" && <section>
          <div className="dc-section-heading"><div><p className="dc-eyebrow">来源受控的中文检索</p><h2>中文问答</h2></div></div>
          <form className="dc-ask-form" onSubmit={(event) => void ask(event)}>
            <label>问题<textarea aria-label="问题" value={question} onChange={(event) => setQuestion(event.target.value)} required rows={4} placeholder="描述要核实的问题" /></label>
            <fieldset><legend>允许使用的来源</legend>{sources.map((source) => <label className="dc-check" key={source.source_id}>
              <input type="checkbox" checked={selectedSources.includes(source.source_id)} onChange={(event) => setSelectedSources((current) => event.target.checked
                ? [...current, source.source_id] : current.filter((value) => value !== source.source_id))} />
              <span>{source.title}<small>{source.source_id}</small></span>
            </label>)}</fieldset>
            <label className="dc-inline-field">检索方式<select value={retrievalMode} onChange={(event) => setRetrievalMode(event.target.value)}>
              <option value="hybrid">混合检索</option><option value="graph">关系检索</option>
            </select></label>
            <button type="submit" className="dc-button dc-button-primary" disabled={busy || !question.trim() || selectedSources.length === 0}>提交问题</button>
          </form>
          {answer && <AnswerView value={answer} onReference={(reference) => void showReference(reference)} referenceData={referenceData} />}
        </section>}
        {section === "metrics" && <section>
          <div className="dc-section-heading"><div><p className="dc-eyebrow">版本化定义</p><h2>指标定义</h2></div>
            <button type="button" className="dc-button" disabled={busy} onClick={() => void openMetrics()}>刷新指标</button></div>
          {metrics.length ? <div className="dc-card-grid">{metrics.map((metric, index) => <article className="dc-card" key={String(metric.metric_id ?? index)}>
            <span className="dc-chip">{String(metric.version ?? "已登记")}</span><h3>{String(metric.title ?? metric.metric_id ?? "指标")}</h3>
            <p>{String(metric.formula ?? "")}</p><dl><div><dt>指标编号</dt><dd>{String(metric.metric_id ?? "")}</dd></div>
              <div><dt>单位</dt><dd>{String(metric.unit ?? "")}</dd></div><div><dt>来源</dt><dd>{Array.isArray(metric.source_ids) ? metric.source_ids.join("、") : ""}</dd></div></dl>
            <p>可用维度：{Array.isArray(metric.dimensions) ? metric.dimensions.join("、") : "未登记"}</p>
          </article>)}</div> : <p className="dc-empty">没有可查看的指标定义。</p>}
        </section>}
        {section === "tasks" && <section>
          <div className="dc-section-heading"><div><p className="dc-eyebrow">可恢复的执行记录</p><h2>任务记录</h2></div>
            <button type="button" className="dc-button" disabled={busy} onClick={() => void loadTasks().catch((caught: unknown) => setError(errorText(caught)))}>刷新任务</button></div>
          <div className="dc-task-layout"><div className="dc-task-list">{tasks.map((task) => <button type="button" key={task.task_id}
            className={selectedTask?.task_id === task.task_id ? "dc-task dc-task-selected" : "dc-task"} onClick={() => void showTask(task)}>
            <span><strong>{task.operation}</strong><small>{task.task_id} · {task.created_at}</small></span><span className={`dc-status dc-status-${task.status}`}>{task.status}</span>
          </button>)}{!tasks.length && <p className="dc-empty">当前账号没有任务记录。</p>}</div>
            {selectedTask && <article className="dc-task-detail"><p className="dc-eyebrow">{selectedTask.run_id}</p><h3>{selectedTask.operation} · {selectedTask.status}</h3>
              <p>任务编号：{selectedTask.task_id}</p><p>输入摘要：{selectedTask.input_sha256}</p>
              {selectedTask.error && <p className="dc-error">任务错误：{selectedTask.error}</p>}
              {selectedTask.result && <AnswerView value={selectedTask.result.data as Record<string, unknown>} onReference={(reference) => void showReference(reference)} referenceData={referenceData} />}
              <div className="dc-actions">
                {!["completed", "partial", "DATA_BLOCKED", "cancelled"].includes(selectedTask.status) && <button type="button" className="dc-button" disabled={busy} onClick={() => void runAction("cancel", { task_id: selectedTask.task_id })}>取消任务</button>}
                {["queued", "waiting", "unknown", "failed", "cancelled"].includes(selectedTask.status) && <button type="button" className="dc-button" disabled={busy} onClick={() => void runAction("run", { task_id: selectedTask.task_id })}>恢复任务</button>}
                {canExport && <button type="button" className="dc-button" disabled={busy} onClick={() => void exportTask(selectedTask.task_id, "markdown")}>下载 Markdown</button>}
                {canExport && selectedRows.length > 0 && <button type="button" className="dc-button" disabled={busy} onClick={() => void exportTask(selectedTask.task_id, "csv")}>下载 CSV</button>}
              </div>
              {selectedEvidence.length > 0 && <div className="dc-evidence-list"><h4>来源引用</h4>{selectedEvidence.map((reference, index) => <button type="button" className="dc-evidence" key={`${String(reference.source_id)}-${index}`} onClick={() => void showReference(reference)}>
                <strong>{String(reference.source_id)} · {String(reference.record_id)}</strong><small>{String(reference.kind)} · SHA-256 {String(reference.content_sha256)}</small>
              </button>)}</div>}
              {referenceData !== null && <section className="dc-raw"><h4>原始来源记录</h4><pre>{pretty(referenceData)}</pre></section>}
            </article>}
          </div>
        </section>}
        {section === "sources" && <section>
          <div className="dc-section-heading"><div><p className="dc-eyebrow">管理员操作</p><h2>来源管理</h2></div>
            <button type="button" className="dc-button" disabled={busy} onClick={() => void runAction("manage_sources")}>读取登记来源</button></div>
          <div className="dc-management-grid">
            <form className="dc-management-card" onSubmit={(event) => void registerSource(event)}>
              <h3>登记或更新来源</h3><p>提交符合 SourceRegistration 格式的 JSON。</p>
              <label>来源登记 JSON<textarea aria-label="来源登记 JSON" value={sourceJson} onChange={(event) => setSourceJson(event.target.value)} rows={10} required placeholder={'{"source_id":"…","kind":"analytics","title":"…","revision":"…","nature":"public_business_sample","license_id":"…","sha256":"…","properties":{}}'} /></label>
              <button className="dc-button dc-button-primary" type="submit" disabled={busy || !sourceJson.trim()}>登记来源</button>
            </form>
            <form className="dc-management-card" onSubmit={(event) => void setGrant(event)}>
              <h3>设置资料授权</h3>
              <label>账号编号<input aria-label="账号编号" value={grantActor} onChange={(event) => setGrantActor(event.target.value)} required /></label>
              <label>来源编号<input aria-label="来源编号" value={grantSource} onChange={(event) => setGrantSource(event.target.value)} required /></label>
              <label>权限（逗号分隔）<input value={grantPermissions} onChange={(event) => setGrantPermissions(event.target.value)} placeholder="read" /></label>
              <label>资料编号（逗号分隔）<input value={grantAssets} onChange={(event) => setGrantAssets(event.target.value)} /></label>
              <label className="dc-check"><input type="checkbox" checked={sourceWide} onChange={(event) => setSourceWide(event.target.checked)} /><span>允许读取整个来源</span></label>
              <button className="dc-button dc-button-primary" type="submit" disabled={busy}>保存授权</button>
            </form>
          </div>
          <div className="dc-managed-list">{sources.map((source) => <article className="dc-managed-source" key={source.source_id}>
            <span><strong>{source.title}</strong><small>{source.source_id} · {source.revision}</small></span>
            <span className="dc-source-state">{source.active ? "已登记" : "已撤回"}</span>
            {source.active && <button type="button" className="dc-button dc-button-danger" disabled={busy} onClick={() => void runAction("withdraw_source", { source_id: source.source_id })}>撤回来源</button>}
          </article>)}</div>
        </section>}
      </div>
    </main>
  );
}

function AnswerView({
  value,
  onReference,
  referenceData,
}: {
  value: Record<string, unknown>;
  onReference: (reference: unknown) => void;
  referenceData: unknown;
}) {
  const evidence = Array.isArray(value.evidence) ? value.evidence as Array<Record<string, unknown>> : [];
  const answer = typeof value.answer === "string" ? value.answer : "";
  return <article className="dc-answer">
    {answer && <><h3>回答</h3><p className="dc-answer-text">{answer}</p></>}
    {Array.isArray(value.unknowns) && value.unknowns.length > 0 && <p className="dc-error">待确认：{value.unknowns.join("；")}</p>}
    {evidence.length > 0 && <div className="dc-evidence-list"><h4>引用来源</h4>{evidence.map((reference, index) => <button type="button" className="dc-evidence" key={`${String(reference.source_id)}-${index}`} onClick={() => onReference(reference)}>
      <strong>{String(reference.source_id)} · {String(reference.record_id)}</strong>
      <small>{String(reference.kind)} · 版本 {String(reference.source_revision)} · 行 {String(reference.start_line ?? "未注明")}-{String(reference.end_line ?? "未注明")}</small>
    </button>)}</div>}
    {referenceData !== null && <section className="dc-raw"><h4>原始来源记录</h4><pre>{pretty(referenceData)}</pre></section>}
    {!answer && evidence.length === 0 && <pre>{pretty(value)}</pre>}
  </article>;
}
