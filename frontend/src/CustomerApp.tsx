import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type CustomerIdentity = {
  name: string;
  customer_id: number;
  source_id: string;
  revision: string;
  is_sample: boolean;
};

type OrderItem = {
  line_id: number;
  product_id: number;
  name: string;
  quantity: number;
  unit_price: string;
  discount: string;
  line_total: string;
};

type Order = {
  order_id: number;
  order_number: string;
  order_date: string;
  status: number;
  status_label: string;
  subtotal: string;
  tax: string;
  freight: string;
  total_due: string;
  items: OrderItem[];
  item_count: number;
  remaining_item_count: number;
  source: { source_id: string; revision: string; order_id: number };
  is_sample: boolean;
};

type Product = {
  asset_id: string;
  title: string;
  version: string | number;
  original_fields: Record<string, unknown>;
  source_id: string;
  source_revision: string;
  record_id: string;
  description?: string | { description: string; uncertainties?: string[] };
};

type Ticket = {
  id: string;
  number: string;
  status: string;
  order_number?: string | null;
  reason?: string;
};

type PendingTicket = {
  id: string;
  order_number: string | null;
  reason: string;
};

type TurnResult = {
  text: string;
  orders: Order[];
  products: Product[];
  ticket: Ticket | null;
  pending_ticket: PendingTicket | null;
  sources: Array<Record<string, unknown>>;
};

type TurnStatus = "queued" | "running" | "paused" | "completed" | "cancelled" | "failed";

type Turn = {
  id: string;
  request_id: string;
  message: string;
  status: TurnStatus;
  received_at: string;
  finished_at: string | null;
  model_calls: number;
  tool_calls: number;
  result: TurnResult | null;
  error: string | null;
};

type ConversationSummary = {
  id: string;
  title: string;
  created_at: string;
  selected_order: string | null;
  pending_ticket: PendingTicket | null;
  orders: Order[];
};

type Conversation = ConversationSummary & { turns: Turn[] };

type OrderPage = {
  orders: Order[];
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;
};

type CustomerEvent = {
  id: number;
  type: string;
  turn_id: string | null;
  payload: Record<string, unknown>;
  created_at: string;
};

type EventPage = { events: CustomerEvent[]; last_id: number };

class CustomerRequestError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "CustomerRequestError";
    this.status = status;
  }
}

async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: { ...(init.headers ?? {}) },
  });
  if (!response.ok) {
    let message = "请求未能完成，请稍后重试。";
    try {
      const body: unknown = await response.json();
      if (typeof body === "object" && body !== null && "detail" in body &&
          typeof body.detail === "string") {
        message = body.detail;
      } else if (typeof body === "object" && body !== null && "message" in body &&
                 typeof body.message === "string") {
        message = body.message;
      }
    } catch {
      message = "请求未能完成，请稍后重试。";
    }
    throw new CustomerRequestError(response.status, message);
  }
  return (await response.json()) as T;
}

async function requestVoid(path: string, init: RequestInit = {}): Promise<void> {
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: { ...(init.headers ?? {}) },
  });
  if (!response.ok) {
    let message = "请求未能完成，请稍后重试。";
    try {
      const body: unknown = await response.json();
      if (typeof body === "object" && body !== null && "detail" in body &&
          typeof body.detail === "string") {
        message = body.detail;
      }
    } catch {
      message = "请求未能完成，请稍后重试。";
    }
    throw new CustomerRequestError(response.status, message);
  }
}

function jsonPost(body?: unknown): RequestInit {
  return {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  };
}

const customerApi = {
  login(username: string, password: string) {
    return requestJson<{ id: string; username: string; role: string }>("/api/login", {
      ...jsonPost({ username, password }),
    });
  },
  logout() {
    return requestVoid("/api/logout", { method: "POST" });
  },
  identity() {
    return requestJson<CustomerIdentity>("/api/customer/identity");
  },
  conversations() {
    return requestJson<ConversationSummary[]>("/api/customer/conversations");
  },
  createConversation(title: string) {
    return requestJson<ConversationSummary>("/api/customer/conversations", {
      ...jsonPost({ title }),
    });
  },
  conversation(id: string) {
    return requestJson<Conversation>("/api/customer/conversations/" + encodeURIComponent(id));
  },
  sendMessage(id: string, requestId: string, message: string) {
    return requestJson<Turn>(
      "/api/customer/conversations/" + encodeURIComponent(id) + "/messages",
      { ...jsonPost({ request_id: requestId, message }) },
    );
  },
  cancelTurn(turnId: string) {
    return requestJson<Turn>(
      "/api/customer/turns/" + encodeURIComponent(turnId) + "/cancel",
      { method: "POST" },
    );
  },
  resumeTurn(turnId: string) {
    return requestJson<Turn>(
      "/api/customer/turns/" + encodeURIComponent(turnId) + "/resume",
      { method: "POST" },
    );
  },
  confirmTicket(conversationId: string, pendingId: string, requestId: string) {
    return requestJson<Turn>(
      "/api/customer/conversations/" + encodeURIComponent(conversationId) +
        "/tickets/" + encodeURIComponent(pendingId) + "/confirm",
      { ...jsonPost({ request_id: requestId }) },
    );
  },
  events(conversationId: string, after: number) {
    return requestJson<EventPage>(
      "/api/customer/conversations/" + encodeURIComponent(conversationId) +
        "/events?after=" + encodeURIComponent(String(after)),
    );
  },
  orders(offset: number) {
    return requestJson<OrderPage>("/api/customer/orders?limit=10&offset=" + offset);
  },
  order(orderNumber: string) {
    return requestJson<Order>("/api/customer/orders/" + encodeURIComponent(orderNumber));
  },
  ticket(number: string) {
    return requestJson<Ticket>("/api/customer/tickets/" + encodeURIComponent(number));
  },
};

const statusLabels: Record<TurnStatus, string> = {
  queued: "等待处理",
  running: "正在处理",
  paused: "已暂停",
  completed: "已完成",
  cancelled: "已取消",
  failed: "处理失败",
};

function errorText(error: unknown): string {
  if (error instanceof CustomerRequestError) {
    if (error.status === 401) return "登录状态已过期，请重新登录。";
    if (error.status === 403) return "当前账号没有访问此内容的权限。";
    return error.message;
  }
  if (error instanceof Error && error.message) return error.message;
  return "请求未能完成，请稍后重试。";
}

function makeRequestId(): string {
  return crypto.randomUUID();
}

function isActiveTurn(turn: Turn): boolean {
  return turn.status === "queued" || turn.status === "running" || turn.status === "paused";
}

function formatDate(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium" }).format(parsed);
}

function customerAssetUrl(product: Product): string {
  return "/api/assets/" + encodeURIComponent(product.asset_id) +
    "/versions/" + encodeURIComponent(String(product.version)) + "/file";
}

function sourceField(fields: Record<string, unknown>, name: string): string | null {
  const value = fields[name];
  if (typeof value === "string") return value.trim() ? value : null;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

function OrderCard({
  order,
  onSelect,
}: {
  order: Order;
  onSelect: (orderNumber: string) => void;
}) {
  return (
    <button className="customer-order-card" onClick={() => onSelect(order.order_number)}>
      <span className="customer-order-topline">
        <strong>{order.order_number}</strong>
        <span>{order.status_label}</span>
      </span>
      <span className="customer-order-meta">
        {formatDate(order.order_date)} · {order.item_count} 项明细
      </span>
      <span className="customer-order-total">总额 {order.total_due}</span>
    </button>
  );
}

function ResultContent({
  result,
  onOrderSelect,
  onReadTicket,
}: {
  result: TurnResult;
  onOrderSelect: (orderNumber: string) => void;
  onReadTicket: (number: string) => void;
}) {
  return (
    <div className="customer-result">
      <p className="customer-result-text">{result.text}</p>
      {result.orders.length > 0 && (
        <div className="customer-result-list">
          {result.orders.map((order) => (
            <OrderCard key={order.order_id} order={order} onSelect={onOrderSelect} />
          ))}
        </div>
      )}
      {result.products.length > 0 && (
        <div className="customer-products">
          <h4>服饰素材资料</h4>
          {result.products.map((product) => (
            <article key={product.asset_id + ":" + product.version}>
              <img src={customerAssetUrl(product)} alt={product.title} loading="lazy" />
              <div className="customer-product-info">
                <strong>{product.title}</strong>
                {product.description ? <p>{typeof product.description === "string"
                  ? product.description : product.description.description}</p> : null}
                <dl>
                  {([
                    ["articleType", "服饰类别"],
                    ["baseColour", "颜色"],
                    ["gender", "适用性别"],
                    ["usage", "使用场景"],
                  ] as const).map(([field, label]) => {
                    const value = sourceField(product.original_fields, field);
                    return value ? <div key={field}><dt>{label}</dt><dd>{value}</dd></div> : null;
                  })}
                </dl>
                <small>
                  来源 {product.source_id} · 版本 {product.source_revision} · 记录 {product.record_id}
                </small>
              </div>
            </article>
          ))}
        </div>
      )}
      {result.ticket && (
        <article className="customer-ticket-card">
          <div>
            <strong>人工工单 {result.ticket.number}</strong>
            <span>状态：{result.ticket.status}</span>
            {result.ticket.order_number ? <span>订单：{result.ticket.order_number}</span> : null}
          </div>
          <button
            className="customer-button customer-button-secondary"
            onClick={() => onReadTicket(result.ticket!.number)}
          >
            读取工单
          </button>
        </article>
      )}
      {result.sources.length > 0 && (
        <details className="customer-sources">
          <summary>查看资料来源</summary>
          <ul>
            {result.sources.map((source, index) => (
              <li key={String(source.id ?? source.order_id ?? source.source_id ?? index)}>
                {String(source.name ?? source.title ?? source.source_id ?? "订单资料")}
                {source.revision ? " · " + String(source.revision) : ""}
                {source.order_id ? " · 订单 " + String(source.order_id) : ""}
              </li>
            ))}
          </ul>
        </details>
      )}
    </div>
  );
}

function CustomerApp() {
  const [authState, setAuthState] = useState<"checking" | "anonymous" | "ready" | "forbidden" | "unavailable">("checking");
  const [identity, setIdentity] = useState<CustomerIdentity | null>(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [authError, setAuthError] = useState("");
  const [busy, setBusy] = useState(false);
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [conversation, setConversation] = useState<Conversation | null>(null);
  const [conversationLoading, setConversationLoading] = useState(false);
  const [draft, setDraft] = useState("");
  const [globalError, setGlobalError] = useState("");
  const [connectionLost, setConnectionLost] = useState(false);
  const [orders, setOrders] = useState<Order[]>([]);
  const [ordersTotal, setOrdersTotal] = useState(0);
  const [ordersOffset, setOrdersOffset] = useState(0);
  const [ordersLoading, setOrdersLoading] = useState(false);
  const [ticketDetails, setTicketDetails] = useState<Ticket | null>(null);
  const [confirmingTicket, setConfirmingTicket] = useState(false);
  const eventCursors = useRef<Record<string, number>>({});
  const inFlight = useRef(false);
  const sendRequest = useRef({ message: "", requestId: makeRequestId() });
  const ticketRequestIds = useRef<Record<string, string>>({});
  const ticketInFlight = useRef(false);

  const expireSession = useCallback((message: string) => {
    setAuthState("anonymous");
    setAuthError(message);
    setIdentity(null);
    setConversations([]);
    setConversation(null);
    setActiveId(null);
    setOrders([]);
  }, []);

  const loadWorkspace = useCallback(async () => {
    const [currentIdentity, currentConversations, firstOrders] = await Promise.all([
      customerApi.identity(),
      customerApi.conversations(),
      customerApi.orders(0),
    ]);
    setIdentity(currentIdentity);
    setConversations(currentConversations);
    setOrders(firstOrders.orders);
    setOrdersTotal(firstOrders.total);
    setOrdersOffset(0);
    setActiveId((current) => current ?? currentConversations[0]?.id ?? null);
    setAuthState("ready");
    setAuthError("");
  }, []);

  useEffect(() => {
    let current = true;
    setAuthState("checking");
    void loadWorkspace().catch((error: unknown) => {
      if (!current) return;
      if (error instanceof CustomerRequestError && error.status === 401) {
        setAuthState("anonymous");
      } else if (error instanceof CustomerRequestError && error.status === 403) {
        setAuthState("forbidden");
      } else {
        setAuthState("unavailable");
      }
      setAuthError(errorText(error));
    });
    return () => {
      current = false;
    };
  }, [loadWorkspace]);

  useEffect(() => {
    if (authState !== "ready" || !activeId) {
      setConversation(null);
      return;
    }
    const selectedId = activeId;
    let stopped = false;
    let timer = 0;
    setConversation(null);
    setConversationLoading(true);
    setConnectionLost(false);

    async function refreshConversation() {
      try {
        const detail = await customerApi.conversation(selectedId);
        if (stopped) return;
        setConversation(detail);
        setConversations((current) => current.map((item) =>
          item.id === detail.id ? { ...item, ...detail } : item,
        ));
        setConversationLoading(false);
        const cursor = eventCursors.current[selectedId] ?? 0;
        const eventPage = await customerApi.events(selectedId, cursor);
        if (stopped) return;
        eventCursors.current[selectedId] = Math.max(cursor, eventPage.last_id);
        setConnectionLost(false);
        timer = window.setTimeout(refreshConversation, 1000);
      } catch (error) {
        if (stopped) return;
        setConversationLoading(false);
        if (error instanceof CustomerRequestError && error.status === 401) {
          expireSession(errorText(error));
          return;
        }
        setConnectionLost(true);
        setGlobalError(errorText(error));
        timer = window.setTimeout(refreshConversation, 1000);
      }
    }

    void refreshConversation();
    return () => {
      stopped = true;
      window.clearTimeout(timer);
    };
  }, [activeId, authState, expireSession]);

  const activeTurn = useMemo(
    () => [...(conversation?.turns ?? [])].reverse().find(isActiveTurn) ?? null,
    [conversation],
  );

  async function handleLogin(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setAuthError("");
    try {
      await customerApi.login(username, password);
      await loadWorkspace();
      setPassword("");
    } catch (error) {
      if (error instanceof CustomerRequestError && error.status === 403) {
        setAuthState("forbidden");
      } else {
        setAuthState("anonymous");
      }
      setAuthError(errorText(error));
    } finally {
      setBusy(false);
    }
  }

  async function handleLogout() {
    setBusy(true);
    try {
      await customerApi.logout();
      expireSession("");
      setPassword("");
    } catch (error) {
      if (error instanceof CustomerRequestError && error.status === 401) {
        expireSession("登录状态已过期，请重新登录。");
      } else {
        setGlobalError(errorText(error));
      }
    } finally {
      setBusy(false);
    }
  }

  async function createConversation() {
    setBusy(true);
    setGlobalError("");
    try {
      const created = await customerApi.createConversation("新会话");
      setConversations((current) => [created, ...current]);
      setActiveId(created.id);
      setConversation(null);
      setDraft("");
      sendRequest.current = { message: "", requestId: makeRequestId() };
    } catch (error) {
      setGlobalError(errorText(error));
    } finally {
      setBusy(false);
    }
  }

  async function submitMessage(messageValue = draft) {
    const message = messageValue.trim();
    if (!activeId || !message || busy || inFlight.current || activeTurn) return;
    inFlight.current = true;
    if (sendRequest.current.message !== message) {
      sendRequest.current = { message, requestId: makeRequestId() };
    }
    setBusy(true);
    setGlobalError("");
    try {
      const turn = await customerApi.sendMessage(activeId, sendRequest.current.requestId, message);
      setConversation((current) => current ? {
        ...current,
        turns: current.turns.some((item) => item.id === turn.id)
          ? current.turns.map((item) => item.id === turn.id ? turn : item)
          : [...current.turns, turn],
      } : current);
      setDraft("");
      sendRequest.current = { message: "", requestId: makeRequestId() };
    } catch (error) {
      if (error instanceof CustomerRequestError && error.status === 401) {
        expireSession(errorText(error));
      } else {
        setGlobalError(errorText(error));
      }
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }

  async function updateTurn(action: "cancel" | "resume") {
    if (!activeId || !activeTurn || busy) return;
    setBusy(true);
    setGlobalError("");
    try {
      const updated = action === "cancel"
        ? await customerApi.cancelTurn(activeTurn.id)
        : await customerApi.resumeTurn(activeTurn.id);
      setConversation((current) => current ? {
        ...current,
        turns: current.turns.map((turn) => turn.id === updated.id ? updated : turn),
      } : current);
    } catch (error) {
      setGlobalError(errorText(error));
    } finally {
      setBusy(false);
    }
  }

  async function confirmTicket(pending: PendingTicket) {
    if (!activeId || confirmingTicket || busy || Boolean(activeTurn) || ticketInFlight.current) return;
    ticketInFlight.current = true;
    const requestId = ticketRequestIds.current[pending.id] ?? makeRequestId();
    ticketRequestIds.current[pending.id] = requestId;
    setConfirmingTicket(true);
    setGlobalError("");
    try {
      const turn = await customerApi.confirmTicket(activeId, pending.id, requestId);
      setConversation((current) => current ? {
        ...current,
        turns: current.turns.some((item) => item.id === turn.id)
          ? current.turns.map((item) => item.id === turn.id ? turn : item)
          : [...current.turns, turn],
      } : current);
    } catch (error) {
      setGlobalError(errorText(error));
    } finally {
      ticketInFlight.current = false;
      setConfirmingTicket(false);
    }
  }

  async function loadOrderPage(offset: number) {
    setOrdersLoading(true);
    try {
      const page = await customerApi.orders(offset);
      setOrders(page.orders);
      setOrdersTotal(page.total);
      setOrdersOffset(page.offset);
    } catch (error) {
      setGlobalError(errorText(error));
    } finally {
      setOrdersLoading(false);
    }
  }

  async function readTicket(number: string) {
    setGlobalError("");
    try {
      setTicketDetails(await customerApi.ticket(number));
    } catch (error) {
      setGlobalError(errorText(error));
    }
  }

  function selectOrder(orderNumber: string) {
    void submitMessage("查询订单" + orderNumber + "的完整明细");
  }

  if (authState !== "ready" || !identity) {
    return (
      <main className="customer-root customer-auth-shell">
        <section className="customer-auth-card">
          <span className="customer-brand-mark">客</span>
          <p className="customer-eyebrow">订单文字客服</p>
          {authState === "checking" ? (
            <>
              <h1>正在连接客服</h1>
              <p className="customer-muted">正在读取登录状态…</p>
            </>
          ) : authState === "forbidden" ? (
            <>
              <h1>当前账号无法使用客服</h1>
              <p className="customer-error" role="alert">{authError}</p>
            </>
          ) : authState === "unavailable" ? (
            <>
              <h1>暂时无法连接</h1>
              <p className="customer-error" role="alert">{authError}</p>
              <button className="customer-button customer-button-primary" onClick={() => window.location.reload()}>
                重新连接
              </button>
            </>
          ) : (
            <>
              <h1>登录后查询本人订单</h1>
              <p className="customer-muted">订单信息仅显示已绑定账号对应的客户资料。</p>
              <form className="customer-login-form" onSubmit={(event) => void handleLogin(event)}>
                <label>
                  用户名
                  <input autoComplete="username" value={username} onChange={(event) => setUsername(event.target.value)} required />
                </label>
                <label>
                  密码
                  <input type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} required />
                </label>
                {authError && <p className="customer-error" role="alert">{authError}</p>}
                <button className="customer-button customer-button-primary customer-full" type="submit" disabled={busy}>
                  {busy ? "正在登录…" : "登录"}
                </button>
              </form>
            </>
          )}
        </section>
      </main>
    );
  }

  const pendingTicket = conversation?.pending_ticket ?? null;
  const lastTurn = conversation?.turns.at(-1) ?? null;
  const statusText = connectionLost ? "连接中断，正在重试" : "服务已连接";

  return (
    <main className="customer-root">
      <header className="customer-topbar">
        <a className="customer-brand" href="/harness/?workspace=customer">
          <span className="customer-brand-mark">客</span>
          <span><strong>订单文字客服</strong><small>本人订单与商品资料</small></span>
        </a>
        <div className="customer-top-actions">
          <span className={"customer-connection" + (connectionLost ? " is-lost" : "")}>
            <span />{statusText}
          </span>
          <span className="customer-name">{identity.name} <small>客户 {identity.customer_id}</small></span>
          <button className="customer-button customer-button-quiet" onClick={() => void handleLogout()} disabled={busy}>
            退出登录
          </button>
        </div>
      </header>

      {globalError && (
        <div className="customer-global-error" role="alert">
          <span>{globalError}</span>
          <button aria-label="关闭提示" onClick={() => setGlobalError("")}>×</button>
        </div>
      )}
      {connectionLost && (
        <div className="customer-disconnect" role="status">
          连接暂时中断。页面会继续读取已保存的会话事件，恢复后会显示最新状态。
        </div>
      )}

      <div className="customer-layout">
        <aside className="customer-sidebar">
          <div className="customer-side-heading">
            <div><p className="customer-eyebrow">会话</p><h1>客服对话</h1></div>
            <button className="customer-button customer-button-primary" onClick={() => void createConversation()} disabled={busy}>
              新建
            </button>
          </div>
          <div className="customer-conversation-list" aria-label="客服会话列表">
            {conversations.length === 0 ? (
              <p className="customer-empty-small">尚无会话，点击“新建”开始查询。</p>
            ) : conversations.map((item) => (
              <button
                key={item.id}
                className={"customer-conversation-button" + (item.id === activeId ? " is-selected" : "")}
                onClick={() => setActiveId(item.id)}
              >
                <strong>{item.title}</strong>
                <span>{formatDate(item.created_at)}</span>
              </button>
            ))}
          </div>
          <section className="customer-orders-panel">
            <div className="customer-panel-heading">
              <div><p className="customer-eyebrow">订单</p><h2>我的近期订单</h2></div>
              <span>{ordersTotal} 条</span>
            </div>
            {orders.length === 0 ? (
              <p className="customer-empty-small">当前没有可显示的订单。</p>
            ) : (
              <div className="customer-order-list">
                {orders.map((order) => (
                  <OrderCard key={order.order_id} order={order} onSelect={selectOrder} />
                ))}
              </div>
            )}
            <div className="customer-page-controls">
              <button className="customer-button customer-button-secondary" onClick={() => void loadOrderPage(Math.max(0, ordersOffset - 10))} disabled={ordersLoading || ordersOffset === 0}>
                较新
              </button>
              <span>{ordersTotal ? ordersOffset + 1 : 0}–{Math.min(ordersOffset + orders.length, ordersTotal)}</span>
              <button className="customer-button customer-button-secondary" onClick={() => void loadOrderPage(ordersOffset + 10)} disabled={ordersLoading || ordersOffset + orders.length >= ordersTotal}>
                较早
              </button>
            </div>
          </section>
          <p className="customer-sample-notice">
            {identity.is_sample ? "官方 AdventureWorks 历史样例。" : "订单历史资料。"}
            {" 来源："}{identity.source_id}{" · 版本 "}{identity.revision}
            {"。资料仅反映来源记录，不代表当前价格、库存、售后政策或实时物流。"}
          </p>
        </aside>

        <section className="customer-chat-panel" aria-label="客服对话内容">
          {!activeId ? (
            <div className="customer-chat-empty">
              <span>聊</span>
              <h2>开始查询订单或商品资料</h2>
              <p>可以询问最近订单、某个订单的明细，或查看历史订单。</p>
              <button className="customer-button customer-button-primary" onClick={() => void createConversation()} disabled={busy}>
                新建客服会话
              </button>
            </div>
          ) : conversationLoading && !conversation ? (
            <div className="customer-chat-empty"><p>正在读取会话…</p></div>
          ) : (
            <>
              <header className="customer-chat-heading">
                <div><p className="customer-eyebrow">当前会话</p><h2>{conversation?.title ?? "客服对话"}</h2></div>
                  {activeTurn && (
                    <div className="customer-turn-actions">
                      <span className={"customer-status status-" + activeTurn.status}>{statusLabels[activeTurn.status]}</span>
                      {activeTurn.status === "paused" ? (
                        <>
                          <button className="customer-button customer-button-primary" onClick={() => void updateTurn("resume")} disabled={busy}>继续处理</button>
                          <button className="customer-button customer-button-danger" onClick={() => void updateTurn("cancel")} disabled={busy}>取消本轮</button>
                        </>
                      ) : (
                        <button className="customer-button customer-button-danger" onClick={() => void updateTurn("cancel")} disabled={busy}>取消本轮</button>
                      )}
                  </div>
                )}
              </header>
              <div className="customer-message-list" aria-live="polite">
                {conversation?.turns.length ? conversation.turns.map((turn) => (
                  <article className="customer-turn" key={turn.id} data-turn-id={turn.id}>
                    <div className="customer-message customer-message-user">
                      <span className="customer-message-label">你</span>
                      <p>{turn.message}</p>
                      <small>{formatDate(turn.received_at)}</small>
                    </div>
                    <div className="customer-message customer-message-assistant">
                      <span className="customer-message-label">客服</span>
                      {turn.result ? (
                        <ResultContent result={turn.result} onOrderSelect={selectOrder} onReadTicket={(number) => void readTicket(number)} />
                      ) : turn.status === "failed" ? (
                        <p className="customer-error">{turn.error || "本轮处理失败，请重新描述问题。"}</p>
                      ) : (
                        <p className="customer-muted">{statusLabels[turn.status]}…</p>
                      )}
                      {turn.error && turn.status !== "failed" ? <p className="customer-error">{turn.error}</p> : null}
                      <div className="customer-message-foot">
                        <span className={"customer-status status-" + turn.status}>{statusLabels[turn.status]}</span>
                        {turn.finished_at ? <time>{formatDate(turn.finished_at)}</time> : null}
                      </div>
                    </div>
                  </article>
                )) : (
                  <div className="customer-first-message">
                    <span>你好，{identity.name}</span>
                    <p>我可以协助查询本人订单、订单明细和来源中的商品资料。</p>
                    <small>请勿在此提交银行卡、密码等敏感信息。</small>
                  </div>
                )}
              </div>

              {pendingTicket && (
                <section className="customer-confirm-card">
                  <div>
                    <p className="customer-eyebrow">等待你的确认</p>
                    <h3>提交人工工单</h3>
                    <p>{pendingTicket.reason}</p>
                    {pendingTicket.order_number ? <small>关联订单：{pendingTicket.order_number}</small> : null}
                  </div>
                  <div className="customer-confirm-actions">
                    <button className="customer-button customer-button-primary" onClick={() => void confirmTicket(pendingTicket)} disabled={confirmingTicket || busy}>
                      {confirmingTicket ? "正在提交…" : "确认提交"}
                    </button>
                    <button className="customer-button customer-button-secondary" onClick={() => void submitMessage("取消人工工单")} disabled={confirmingTicket || busy || Boolean(activeTurn)}>
                      暂不提交
                    </button>
                  </div>
                </section>
              )}

              {ticketDetails && (
                <section className="customer-ticket-detail">
                  <button aria-label="关闭工单详情" onClick={() => setTicketDetails(null)}>×</button>
                  <strong>工单 {ticketDetails.number}</strong>
                  <span>状态：{ticketDetails.status}</span>
                  {ticketDetails.order_number ? <span>订单：{ticketDetails.order_number}</span> : null}
                  {ticketDetails.reason ? <p>{ticketDetails.reason}</p> : null}
                </section>
              )}

              <form className="customer-composer" onSubmit={(event) => { event.preventDefault(); void submitMessage(); }}>
                <label className="sr-only" htmlFor="customer-message-input">输入问题</label>
                <textarea
                  id="customer-message-input"
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" && !event.shiftKey) {
                      event.preventDefault();
                      void submitMessage();
                    }
                  }}
                  placeholder="输入订单或商品问题，按 Enter 发送，Shift + Enter 换行"
                  rows={2}
                  disabled={Boolean(activeTurn) || busy}
                />
                <button className="customer-button customer-button-primary" type="submit" disabled={!draft.trim() || Boolean(activeTurn) || busy}>
                  {busy ? "正在发送…" : "发送"}
                </button>
              </form>
              <div className="customer-chat-footnote">
                {lastTurn && activeTurn ? "客服正在处理当前问题，请等待结果或取消本轮。" : "订单内容来自固定历史样例，页面会持续读取已保存的会话更新。"}
              </div>
            </>
          )}
        </section>
      </div>
    </main>
  );
}

export default CustomerApp;
