import { useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError, assetUrl } from "./api";
import type { Recommendation, Task, TaskStatus, UserIdentity } from "./types";

type AuthState =
  | "checking"
  | "anonymous"
  | "authenticated"
  | "forbidden"
  | "unavailable";

type TaskAction = "run" | "resume" | "cancel";

const statusLabels: Record<TaskStatus, string> = {
  created: "待运行",
  running: "运行中",
  paused: "已暂停",
  completed: "已完成",
  stopped: "已停止",
  cancelled: "已取消",
};

function errorText(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401) {
      return "登录信息无效或会话已过期，请重新登录。";
    }
    if (error.status === 403) {
      return "当前账号没有权限执行此操作。";
    }
    return error.message;
  }
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return "请求未能完成，请稍后重试。";
}

function elapsedText(seconds: number): string {
  const total = Math.max(0, Math.floor(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const remainingSeconds = total % 60;
  if (hours > 0) {
    return hours + " 小时 " + minutes + " 分钟";
  }
  if (minutes > 0) {
    return minutes + " 分钟 " + remainingSeconds + " 秒";
  }
  return remainingSeconds + " 秒";
}

function StatusPill({ status }: { status: TaskStatus }) {
  return (
    <span className={"status-pill status-" + status}>
      <span className="status-dot" />
      {statusLabels[status]}
    </span>
  );
}

function LoginPage({
  authState,
  authMessage,
  onLogin,
  onRetry,
}: {
  authState: AuthState;
  authMessage: string;
  onLogin: (username: string, password: string) => Promise<void>;
  onRetry: () => void;
}) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState("");

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFormError("");
    setSubmitting(true);
    try {
      await onLogin(username, password);
    } catch (error) {
      setFormError(errorText(error));
    } finally {
      setSubmitting(false);
    }
  }

  if (authState === "checking") {
    return (
      <main className="auth-shell">
        <div className="auth-card">
          <span className="brand-mark">H</span>
          <p className="eyebrow">HARNESS 工作区</p>
          <h1>正在读取登录状态</h1>
          <p className="muted">正在连接任务服务…</p>
          <span className="loading-line" />
        </div>
      </main>
    );
  }

  if (authState === "forbidden") {
    return (
      <main className="auth-shell">
        <div className="auth-card">
          <span className="brand-mark">H</span>
          <p className="eyebrow">HARNESS 工作区</p>
          <h1>当前账号没有访问权限</h1>
          <p className="form-error" role="alert">
            {authMessage || "请联系工作区管理员检查账号权限。"}
          </p>
        </div>
      </main>
    );
  }

  if (authState === "unavailable") {
    return (
      <main className="auth-shell">
        <div className="auth-card">
          <span className="brand-mark">H</span>
          <p className="eyebrow">HARNESS 工作区</p>
          <h1>暂时无法连接</h1>
          <p className="form-error" role="alert">
            {authMessage}
          </p>
          <button className="button button-primary button-full" onClick={onRetry}>
            重新连接
          </button>
        </div>
      </main>
    );
  }

  return (
    <main className="auth-shell">
      <section className="auth-card">
        <span className="brand-mark">H</span>
        <p className="eyebrow">HARNESS 工作区</p>
        <h1>登录后管理宣传任务</h1>
        <p className="auth-intro">
          创建素材需求，查看实际执行进度，并核对每项推荐的来源。
        </p>
        <form className="login-form" onSubmit={submit}>
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
          {(formError || authMessage) && (
            <p className="form-error" role="alert">
              {formError || authMessage}
            </p>
          )}
          <button
            className="button button-primary button-full"
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

function RecommendationCard({
  recommendation,
}: {
  recommendation: Recommendation;
}) {
  return (
    <article className="recommendation-card">
      <div className="recommendation-image">
        <img
          src={assetUrl(recommendation.asset_id, recommendation.version)}
          alt={recommendation.title}
          loading="lazy"
        />
      </div>
      <div className="recommendation-content">
        <div className="recommendation-heading">
          <div>
            <p className="eyebrow">推荐素材</p>
            <h3>{recommendation.title}</h3>
          </div>
          <span className="source-tag">{recommendation.source_id}</span>
        </div>
        <p className="recommendation-caption">{recommendation.caption}</p>
        <dl className="source-details">
          <div>
            <dt>来源版本</dt>
            <dd>{recommendation.source_revision}</dd>
          </div>
          <div>
            <dt>记录编号</dt>
            <dd>{recommendation.record_id}</dd>
          </div>
          <div>
            <dt>素材版本</dt>
            <dd>{recommendation.version}</dd>
          </div>
          <div className="hash-row">
            <dt>文件 SHA-256</dt>
            <dd>
              <code>{recommendation.file_sha256}</code>
            </dd>
          </div>
        </dl>
      </div>
    </article>
  );
}

function App() {
  const [authState, setAuthState] = useState<AuthState>("checking");
  const [authMessage, setAuthMessage] = useState("");
  const [identity, setIdentity] = useState<UserIdentity | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [prompt, setPrompt] = useState("");
  const [creating, setCreating] = useState(false);
  const [pendingActions, setPendingActions] = useState<Record<string, TaskAction>>(
    {},
  );
  const [actionErrors, setActionErrors] = useState<Record<string, string>>({});
  const [globalError, setGlobalError] = useState("");
  const [previewHtml, setPreviewHtml] = useState<string | null>(null);
  const [previewTaskId, setPreviewTaskId] = useState<string | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState("");
  const tasksRef = useRef(tasks);
  const pendingActionsRef = useRef(pendingActions);
  const pollingTasksRef = useRef(new Set<string>());

  tasksRef.current = tasks;
  pendingActionsRef.current = pendingActions;

  const replaceTask = useCallback((updatedTask: Task) => {
    setTasks((current) => {
      const exists = current.some((task) => task.task_id === updatedTask.task_id);
      if (!exists) {
        return [updatedTask, ...current];
      }
      return current.map((task) =>
        task.task_id === updatedTask.task_id ? updatedTask : task,
      );
    });
  }, []);

  const resetSessionData = useCallback(() => {
    setTasks([]);
    setSelectedTaskId(null);
    setIdentity(null);
    setPendingActions({});
    setActionErrors({});
    setGlobalError("");
    setPreviewHtml(null);
    setPreviewTaskId(null);
    setPreviewLoading(false);
    setPreviewError("");
  }, []);

  const expireSession = useCallback(
    (message: string) => {
      resetSessionData();
      setAuthMessage(message);
      setAuthState("anonymous");
    },
    [resetSessionData],
  );

  const refreshTask = useCallback(
    async (taskId: string) => {
      if (pollingTasksRef.current.has(taskId)) {
        return;
      }
      pollingTasksRef.current.add(taskId);
      try {
        replaceTask(await api.getTask(taskId));
      } catch (error) {
        if (error instanceof ApiError && error.status === 401) {
          expireSession(errorText(error));
        } else if (error instanceof ApiError && error.status === 403) {
          setGlobalError(errorText(error));
        } else {
          setGlobalError(errorText(error));
        }
      } finally {
        pollingTasksRef.current.delete(taskId);
      }
    },
    [expireSession, replaceTask],
  );

  const checkSession = useCallback(async () => {
    setAuthState("checking");
    setAuthMessage("");
    try {
      const items = await api.listTasks();
      setTasks(items);
      setSelectedTaskId(items[0]?.task_id ?? null);
      setAuthState("authenticated");
    } catch (error) {
      setAuthMessage(errorText(error));
      if (error instanceof ApiError && error.status === 401) {
        setAuthState("anonymous");
      } else if (error instanceof ApiError && error.status === 403) {
        setAuthState("forbidden");
      } else {
        setAuthState("unavailable");
      }
    }
  }, []);

  useEffect(() => {
    void checkSession();
  }, [checkSession]);

  useEffect(() => {
    if (authState !== "authenticated") {
      return;
    }
    const intervalId = window.setInterval(() => {
      const activeIds = new Set(
        tasksRef.current
          .filter((task) => task.status === "running")
          .map((task) => task.task_id),
      );
      Object.keys(pendingActionsRef.current).forEach((taskId) =>
        activeIds.add(taskId),
      );
      activeIds.forEach((taskId) => void refreshTask(taskId));
    }, 2000);
    return () => window.clearInterval(intervalId);
  }, [authState, refreshTask]);

  const selectedTask =
    tasks.find((task) => task.task_id === selectedTaskId) ?? null;

  async function login(username: string, password: string): Promise<void> {
    setAuthMessage("");
    try {
      const signedIn = await api.login(username, password);
      const items = await api.listTasks();
      setIdentity(signedIn);
      setTasks(items);
      setSelectedTaskId(items[0]?.task_id ?? null);
      setAuthState("authenticated");
    } catch (error) {
      setAuthMessage(errorText(error));
      if (error instanceof ApiError && error.status === 403) {
        setAuthState("forbidden");
      }
      throw error;
    }
  }

  async function logout() {
    setGlobalError("");
    try {
      await api.logout();
      resetSessionData();
      setAuthState("anonymous");
      setAuthMessage("");
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        expireSession(errorText(error));
        return;
      }
      setGlobalError(errorText(error));
    }
  }

  async function createTask(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const content = prompt.trim();
    if (!content) {
      setGlobalError("请填写宣传需求。");
      return;
    }
    setCreating(true);
    setGlobalError("");
    try {
      const task = await api.createTask(content);
      setTasks((current) => [task, ...current]);
      setSelectedTaskId(task.task_id);
      setPrompt("");
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        expireSession(errorText(error));
      } else {
        setGlobalError(errorText(error));
      }
    } finally {
      setCreating(false);
    }
  }

  async function actOnTask(task: Task, action: TaskAction) {
    setActionErrors((current) => {
      const next = { ...current };
      delete next[task.task_id];
      return next;
    });
    setPendingActions((current) => ({ ...current, [task.task_id]: action }));
    void refreshTask(task.task_id);
    try {
      const updated =
        action === "run"
          ? await api.runTask(task.task_id)
          : action === "resume"
            ? await api.resumeTask(task.task_id)
            : await api.cancelTask(task.task_id);
      replaceTask(updated);
    } catch (error) {
      const message = errorText(error);
      if (error instanceof ApiError && error.status === 401) {
        expireSession(message);
      } else if (error instanceof ApiError && error.status === 403) {
        setGlobalError(message);
      } else {
        setActionErrors((current) => ({ ...current, [task.task_id]: message }));
      }
      void refreshTask(task.task_id);
    } finally {
      setPendingActions((current) => {
        const next = { ...current };
        delete next[task.task_id];
        return next;
      });
    }
  }

  async function openPreview(task: Task) {
    if (
      task.status !== "completed" ||
      task.result?.status !== "materials_found"
    ) {
      return;
    }
    setPreviewTaskId(task.task_id);
    setPreviewLoading(true);
    setPreviewError("");
    setPreviewHtml(null);
    try {
      setPreviewHtml(await api.previewTask(task.task_id));
    } catch (error) {
      const message = errorText(error);
      setPreviewError(message);
      if (error instanceof ApiError && error.status === 401) {
        expireSession(message);
      } else if (error instanceof ApiError && error.status === 403) {
        setGlobalError(message);
      }
    } finally {
      setPreviewLoading(false);
    }
  }

  function closePreview() {
    setPreviewTaskId(null);
    setPreviewHtml(null);
    setPreviewError("");
  }

  if (authState !== "authenticated") {
    return (
      <LoginPage
        authState={authState}
        authMessage={authMessage}
        onLogin={login}
        onRetry={() => void checkSession()}
      />
    );
  }

  const actionInProgress = selectedTask
    ? pendingActions[selectedTask.task_id]
    : undefined;
  const canPreview =
    selectedTask?.status === "completed" &&
    selectedTask.result?.status === "materials_found";
  const selectedTaskExportUrl = selectedTask
    ? api.exportUrl(selectedTask.task_id)
    : "";

  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand-lockup" href="/harness/" aria-label="宣传素材任务首页">
          <span className="brand-mark">H</span>
          <span>
            <strong>宣传素材任务</strong>
            <small>HARNESS 工作区</small>
          </span>
        </a>
        <div className="topbar-actions">
          <span className="session-label">
            <span className="session-dot" />
            服务已连接
          </span>
          <span className="user-name">
            {identity?.username ?? "已登录"}
            {identity?.role ? <small>{identity.role}</small> : null}
          </span>
          <button className="button button-quiet" onClick={() => void logout()}>
            退出登录
          </button>
        </div>
      </header>

      {globalError && (
        <div className="notice notice-error" role="alert">
          <span>{globalError}</span>
          <button
            className="notice-close"
            aria-label="关闭提示"
            onClick={() => setGlobalError("")}
          >
            ×
          </button>
        </div>
      )}

      <div className="workspace">
        <aside className="task-sidebar">
          <div className="sidebar-heading">
            <div>
              <p className="eyebrow">工作区</p>
              <h1>宣传任务</h1>
            </div>
            <span className="task-count">{tasks.length}</span>
          </div>
          <p className="sidebar-intro">
            描述需求，系统会从可用素材中查找并整理推荐。
          </p>

          <form className="composer" onSubmit={createTask}>
            <label htmlFor="task-prompt">创建宣传需求</label>
            <textarea
              id="task-prompt"
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
              placeholder="例如：为春季新品发布活动寻找适合社交媒体的图片素材，并说明推荐理由。"
              rows={5}
              required
            />
            <button
              className="button button-primary button-full"
              type="submit"
              disabled={creating}
            >
              {creating ? "正在创建…" : "创建任务"}
            </button>
          </form>

          <div className="task-list-heading">
            <h2>任务列表</h2>
            <span>{tasks.length} 项</span>
          </div>
          {tasks.length === 0 ? (
            <div className="empty-list">
              <span className="empty-mark">＋</span>
              <p>这里还没有任务</p>
              <small>填写上方需求后，任务会显示在这里。</small>
            </div>
          ) : (
            <nav className="task-list" aria-label="任务列表">
              {tasks.map((task) => (
                <button
                  className={
                    "task-list-item" +
                    (task.task_id === selectedTaskId ? " selected" : "")
                  }
                  key={task.task_id}
                  onClick={() => setSelectedTaskId(task.task_id)}
                >
                  <span className="task-item-topline">
                    <StatusPill status={task.status} />
                    <span>{elapsedText(task.elapsed_seconds)}</span>
                  </span>
                  <strong>{task.prompt}</strong>
                  <span className="task-item-metrics">
                    {task.model_calls} 次模型调用
                    <span aria-hidden="true">·</span>
                    {task.tool_calls} 次工具调用
                  </span>
                </button>
              ))}
            </nav>
          )}
          <div className="sidebar-footnote">
            <span className="footnote-icon">i</span>
            任务进度每 2 秒从服务端更新。
          </div>
        </aside>

        <main className="task-detail">
          {selectedTask ? (
            <>
              <section className="detail-header">
                <div className="detail-title">
                  <div className="detail-title-row">
                    <StatusPill status={selectedTask.status} />
                    <span className="run-time">
                      已运行 {elapsedText(selectedTask.elapsed_seconds)}
                    </span>
                  </div>
                  <h2>{selectedTask.prompt}</h2>
                </div>
                <div className="task-actions">
                  {selectedTask.status === "created" && (
                    <button
                      className="button button-primary"
                      onClick={() => void actOnTask(selectedTask, "run")}
                      disabled={Boolean(actionInProgress)}
                    >
                      {actionInProgress === "run" ? "正在启动…" : "运行任务"}
                    </button>
                  )}
                  {(selectedTask.status === "paused" ||
                    (selectedTask.status === "running" && !actionInProgress)) && (
                    <button
                      className="button button-primary"
                      onClick={() => void actOnTask(selectedTask, "resume")}
                      disabled={Boolean(actionInProgress)}
                    >
                      {actionInProgress === "resume" ? "正在恢复…" : "恢复任务"}
                    </button>
                  )}
                  {["created", "running", "paused"].includes(selectedTask.status) && (
                    <button
                      className="button button-danger"
                      onClick={() => void actOnTask(selectedTask, "cancel")}
                      disabled={
                        actionInProgress === "cancel" || selectedTask.cancel_requested
                      }
                    >
                      {selectedTask.cancel_requested
                        ? "正在取消…"
                        : actionInProgress === "cancel"
                          ? "正在发送…"
                          : "取消任务"}
                    </button>
                  )}
                  {canPreview && (
                    <>
                      <button
                        className="button button-secondary"
                        onClick={() => void openPreview(selectedTask)}
                      >
                        预览结果
                      </button>
                      <a
                        className="button button-secondary"
                        href={selectedTaskExportUrl}
                        download
                      >
                        下载 HTML
                      </a>
                    </>
                  )}
                </div>
              </section>

              {actionErrors[selectedTask.task_id] && (
                <div className="notice notice-error" role="alert">
                  {actionErrors[selectedTask.task_id]}
                </div>
              )}

              <section className="metrics-grid" aria-label="实际执行进度">
                <div className="metric-card">
                  <span>模型调用</span>
                  <strong>{selectedTask.model_calls}</strong>
                  <small>服务端累计记录</small>
                </div>
                <div className="metric-card">
                  <span>工具调用</span>
                  <strong>{selectedTask.tool_calls}</strong>
                  <small>服务端累计记录</small>
                </div>
                <div className="metric-card metric-time">
                  <span>已运行时间</span>
                  <strong>{elapsedText(selectedTask.elapsed_seconds)}</strong>
                  <small>服务端当前记录</small>
                </div>
              </section>

              {selectedTask.cancel_requested && (
                <div className="notice notice-info" role="status">
                  已收到取消请求，正在读取服务端的最终状态。
                </div>
              )}
              {selectedTask.error && (
                <section className="message-card message-error">
                  <p className="eyebrow">执行错误</p>
                  <p>{selectedTask.error}</p>
                </section>
              )}
              {selectedTask.stop_reason && (
                <section className="message-card">
                  <p className="eyebrow">停止原因</p>
                  <p>{selectedTask.stop_reason}</p>
                </section>
              )}

              {selectedTask.result ? (
                <section className="result-section">
                  <div className="section-title-row">
                    <div>
                      <p className="eyebrow">任务结果</p>
                      <h2>{selectedTask.result.headline}</h2>
                    </div>
                    <span
                      className={
                        "result-tag result-" + selectedTask.result.status
                      }
                    >
                      {selectedTask.result.status === "materials_found"
                        ? "已找到素材"
                        : "信息不足"}
                    </span>
                  </div>
                  <p className="result-summary">{selectedTask.result.summary}</p>
                  {selectedTask.result.status === "materials_found" &&
                  selectedTask.result.recommendations.length > 0 ? (
                    <div className="recommendation-list">
                      {selectedTask.result.recommendations.map(
                        (recommendation) => (
                          <RecommendationCard
                            key={
                              recommendation.asset_id +
                              ":" +
                              recommendation.version
                            }
                            recommendation={recommendation}
                          />
                        ),
                      )}
                    </div>
                  ) : null}
                  {selectedTask.result.status === "insufficient" &&
                    selectedTask.result.missing_info.length > 0 && (
                      <div className="missing-info">
                        <h3>还需要补充</h3>
                        <ul>
                          {selectedTask.result.missing_info.map((item) => (
                            <li key={item}>{item}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                </section>
              ) : (
                <section className="progress-card">
                  <div className="progress-icon">
                    <span />
                  </div>
                  <div>
                    <p className="eyebrow">执行进度</p>
                    <h3>
                      {selectedTask.status === "running"
                        ? "任务正在处理中"
                        : selectedTask.status === "created"
                          ? "任务已创建，等待运行"
                          : selectedTask.status === "paused"
                            ? "任务已暂停"
                            : selectedTask.status === "stopped"
                              ? "任务已停止"
                              : selectedTask.status === "cancelled"
                                ? "任务已取消"
                                : "正在等待结果"}
                    </h3>
                    <p>
                      页面显示的调用次数与运行时间来自服务端任务记录。
                      {selectedTask.status === "running"
                        ? "进度读取间隔为 2 秒。"
                        : ""}
                    </p>
                  </div>
                </section>
              )}

              {(selectedTask.requests || selectedTask.tools) && (
                <details className="execution-details">
                  <summary>查看请求与工具记录</summary>
                  {selectedTask.requests && (
                    <div>
                      <h3>请求记录</h3>
                      <pre>{JSON.stringify(selectedTask.requests, null, 2)}</pre>
                    </div>
                  )}
                  {selectedTask.tools && (
                    <div>
                      <h3>工具记录</h3>
                      <pre>{JSON.stringify(selectedTask.tools, null, 2)}</pre>
                    </div>
                  )}
                </details>
              )}
            </>
          ) : (
            <section className="detail-empty">
              <div className="empty-artifact">
                <span />
                <span />
                <span />
              </div>
              <p className="eyebrow">任务详情</p>
              <h2>{tasks.length === 0 ? "从一条宣传需求开始" : "选择一个任务"}</h2>
              <p>
                {tasks.length === 0
                  ? "创建任务后，这里会显示服务端进度、来源引用和实际结果。"
                  : "选择左侧任务，查看执行进度与结果。"}
              </p>
            </section>
          )}
        </main>
      </div>

      {previewTaskId && (
        <div
          className="preview-overlay"
          role="presentation"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              closePreview();
            }
          }}
        >
          <section
            className="preview-dialog"
            role="dialog"
            aria-modal="true"
            aria-labelledby="preview-title"
          >
            <header className="preview-header">
              <div>
                <p className="eyebrow">任务结果</p>
                <h2 id="preview-title">宣传内容预览</h2>
              </div>
              <button
                className="button button-quiet"
                onClick={closePreview}
                aria-label="关闭预览"
              >
                关闭
              </button>
            </header>
            {previewLoading ? (
              <div className="preview-state">正在读取受保护的预览内容…</div>
            ) : previewError ? (
              <div className="preview-state form-error" role="alert">
                {previewError}
              </div>
            ) : previewHtml !== null ? (
              <iframe
                className="preview-frame"
                title="宣传内容预览"
                sandbox=""
                referrerPolicy="no-referrer"
                srcDoc={previewHtml}
              />
            ) : null}
          </section>
        </div>
      )}
    </div>
  );
}

export default App;
