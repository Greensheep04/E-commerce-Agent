import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createHash, randomUUID } from "node:crypto";

const modulesPath = process.env.SHOP_BROWSER_NODE_MODULES;
const inputsPath = process.argv[2];
const sourceReportPath = process.argv[3];
assert.ok(modulesPath, "需要配置SHOP_BROWSER_NODE_MODULES");
assert.ok(inputsPath && sourceReportPath && process.argv.length === 4,
  "用法: node scripts/verify_integration_browser.mjs <固定输入.json> <已完成业务报告.json>");

const browserPath = path.resolve(".scratch/playwright");
const temporaryPath = path.resolve(".scratch/browser-tmp");
process.env.PLAYWRIGHT_BROWSERS_PATH = browserPath;
process.env.TMPDIR = temporaryPath;
await fs.mkdir(temporaryPath, { recursive: true });
const { chromium } = await import(pathToFileURL(path.join(modulesPath, "playwright", "index.mjs")));

const inputsBytes = await fs.readFile(inputsPath);
const inputDocument = JSON.parse(inputsBytes.toString("utf8"));
const inputs = {
  accounts: {
    harness: inputDocument.accounts?.harness ?? inputDocument.accounts?.admin,
    customer: inputDocument.accounts?.customer,
    assessment: inputDocument.accounts?.assessment,
  },
  harness: {
    ...inputDocument.harness,
    task_id: inputDocument.harness?.task_id ?? inputDocument.harness_task_id,
  },
  customer: {
    ...inputDocument.customer,
    conversation_id: inputDocument.customer?.conversation_id ?? inputDocument.conversation_id,
  },
  assessment: {
    ...inputDocument.assessment,
    run_id: inputDocument.assessment?.run_id ?? inputDocument.assessment_run_id,
  },
};
const configuredBaseUrl = process.env.SHOP_INTEGRATION_URL ?? inputDocument.base_url ?? "http://127.0.0.1:8001";
const parsedBaseUrl = new URL(configuredBaseUrl);
assert.equal(parsedBaseUrl.username, "", "统一服务地址不能包含账号信息");
assert.equal(parsedBaseUrl.password, "", "统一服务地址不能包含账号信息");
assert.equal(parsedBaseUrl.pathname, "/", "统一服务地址只能包含服务origin");
assert.equal(parsedBaseUrl.search, "", "统一服务地址不能包含查询参数");
assert.equal(parsedBaseUrl.hash, "", "统一服务地址不能包含片段");
const baseUrl = parsedBaseUrl.origin;
const sourceReportBytes = await fs.readFile(sourceReportPath);
const sourceReport = JSON.parse(sourceReportBytes.toString("utf8"));
const runId = "if-browser-" + new Date().toISOString().replaceAll(":", "").replaceAll("-", "") + "-" + randomUUID().slice(0, 8);
const directory = path.join(".scratch/evidence", runId);
await fs.mkdir(directory, { recursive: true });

const sourceFiles = [
  "frontend/src/main.tsx",
  "frontend/src/App.tsx",
  "frontend/src/api.ts",
  "frontend/src/CustomerApp.tsx",
  "frontend/src/assessment/AssessmentWorkspace.tsx",
  "frontend/src/assessment/api.ts",
  "frontend/src/assessment/types.ts",
  "frontend/src/types.ts",
  "frontend/index.html",
  "frontend/src/styles.css",
  "frontend/src/customer.css",
  "frontend/src/assessment/styles.css",
  "src/shop/app.py",
  "src/shop/auth.py",
  "src/shop/harness_api.py",
  "src/shop/customer_api.py",
  "src/shop/assessment_api.py",
  "src/shop/integration_api.py",
  "src/shop/harness.py",
  "src/shop/campaign.py",
  "src/shop/templates/campaign.html",
  "src/shop/customer_runtime.py",
  "src/shop/assessment.py",
  "src/shop/assessment_service.py",
  "src/shop/assessment_orders.py",
  "src/shop/assessment_order_adapter.py",
  "src/shop/assessment_types.py",
  "scripts/verify_integration_browser.mjs",
];
const sourceHashes = {};
for (const sourcePath of sourceFiles) {
  sourceHashes[sourcePath] = createHash("sha256").update(await fs.readFile(sourcePath)).digest("hex");
}

const report = {
  run_id: runId,
  scope: "IF-04/IF-06 统一服务真实浏览器与DOM检查",
  base_url: baseUrl,
  inputs_path: path.resolve(inputsPath),
  inputs_sha256: createHash("sha256").update(inputsBytes).digest("hex"),
  source_report_path: path.resolve(sourceReportPath),
  source_report_sha256: createHash("sha256").update(sourceReportBytes).digest("hex"),
  source_report_run_id: sourceReport.run_id ?? null,
  fixed_identity: {
    harness_task_id: inputs.harness?.task_id ?? null,
    customer_conversation_id: inputs.customer?.conversation_id ?? null,
    customer_order_number: inputs.customer?.order_number ?? null,
    customer_ticket_number: inputs.customer?.ticket_number ?? null,
    assessment_run_id: inputs.assessment?.run_id ?? null,
    assessment_employee_id: inputs.assessment?.employee_id ?? null,
    usernames: Object.fromEntries(["harness", "customer", "assessment"].map(key => [key, inputs.accounts?.[key]?.username ?? null])),
    actor_ids: Object.fromEntries(["harness", "customer", "assessment"].map(key => [key, inputs.accounts?.[key]?.actor_id ?? null])),
  },
  source_hashes: sourceHashes,
  checks: [],
  http: [],
  page_errors: [],
  errors: [],
  status: "running",
};

assert.ok(report.fixed_identity.harness_task_id, "固定输入缺少 harness.task_id");
assert.ok(report.fixed_identity.customer_conversation_id, "固定输入缺少 customer.conversation_id");
assert.ok(report.fixed_identity.assessment_run_id, "固定输入缺少 assessment.run_id");
for (const role of ["harness", "customer", "assessment"]) {
  assert.ok(inputs.accounts?.[role]?.username, `固定输入缺少 accounts.${role}.username`);
  assert.ok(inputs.accounts?.[role]?.password_file, `固定输入缺少 accounts.${role}.password_file`);
}

const contexts = [];
const pages = [];

async function save() {
  await fs.writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
}

let browser;
try {
  browser = await chromium.launch({ headless: true });
} catch (error) {
  report.status = "failed";
  report.failure = error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : String(error);
  report.errors.push(report.failure);
  await save();
  console.log(path.join(directory, "report.json"));
  throw error;
}

async function check(name, operation) {
  try {
    const result = await operation();
    report.checks.push({ name, passed: true, result: result ?? null });
    await save();
  } catch (error) {
    report.checks.push({
      name,
      passed: false,
      error: error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : String(error),
    });
    await save();
    throw error;
  }
}

function recordHttp(contextName, response, method) {
  const url = new URL(response.url());
  if (url.origin !== new URL(baseUrl).origin || !url.pathname.startsWith("/api/")) return;
  report.http.push({
    context: contextName,
    method: method ?? response.request().method(),
    path: url.pathname,
    query: url.search,
    status: response.status(),
    ok: response.ok(),
  });
}

async function createWorkspace(role, route) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });
  contexts.push(context);
  const page = await context.newPage();
  pages.push(page);
  page.on("pageerror", error => report.page_errors.push({ workspace: role, message: error.message }));
  page.on("response", response => recordHttp(role, response));
  await page.goto(baseUrl + route);
  await page.getByLabel("用户名", { exact: true }).fill(inputs.accounts[role].username);
  const password = (await fs.readFile(inputs.accounts[role].password_file, "utf8")).trim();
  assert.ok(password, `accounts.${role}.password_file 内容为空`);
  await page.getByLabel("密码", { exact: true }).fill(password);
  const loginResponsePromise = page.waitForResponse(response =>
    new URL(response.url()).pathname === "/api/login" && response.request().method() === "POST");
  await page.getByRole("button", { name: "登录", exact: true }).click();
  const loginResponse = await loginResponsePromise;
  assert.equal(loginResponse.status(), 200, `${role} 登录失败`);
  await page.getByRole("button", { name: "退出登录", exact: true }).waitFor();
  return { context, page };
}

async function getJson(context, endpoint, contextName) {
  const response = await context.request.get(baseUrl + endpoint);
  recordHttp(contextName, response, "GET");
  assert.equal(response.status(), 200, `${contextName} GET ${endpoint} 返回 HTTP ${response.status()}`);
  return response.json();
}

async function verifyAnonymous() {
  const context = await browser.newContext();
  contexts.push(context);
  for (const [endpoint, expected] of [
    ["/api/harness/tasks", 401],
    ["/api/customer/orders", 401],
    ["/api/assessment/me", 401],
  ]) {
    const response = await context.request.get(baseUrl + endpoint);
    recordHttp("anonymous", response, "GET");
    assert.equal(response.status(), expected, `匿名 GET ${endpoint} 未返回 ${expected}`);
  }
}

try {
  await verifyAnonymous();

  const harness = await createWorkspace("harness", "/harness/");
  const taskId = inputs.harness.task_id;
  const listedTasks = await getJson(harness.context, "/api/harness/tasks", "harness");
  const listedTaskIndex = listedTasks.findIndex(item => item.task_id === taskId);
  assert.ok(listedTaskIndex >= 0, "固定 Harness 任务不在当前登录账号的真实任务列表中");
  const task = await getJson(harness.context, "/api/harness/tasks/" + encodeURIComponent(taskId), "harness");
  assert.equal(task.task_id, taskId);
  assert.equal(task.status, "completed");
  assert.equal(task.result?.status, "materials_found", "固定 Harness 任务须为已完成且已审核的素材结果");
  const taskRows = harness.page.getByRole("navigation", { name: "任务列表", exact: true }).getByRole("button");
  await taskRows.nth(listedTaskIndex).click();
  await harness.page.getByRole("heading", { name: task.result.headline, exact: true }).waitFor();
  await check("Harness任务DOM与服务端记录一致", async () => {
    assert.equal(await harness.page.locator(".result-summary").innerText(), task.result.summary);
    assert.equal(await harness.page.locator(".recommendation-card").count(), task.result.recommendations.length);
    for (let index = 0; index < task.result.recommendations.length; index += 1) {
      const card = harness.page.locator(".recommendation-card").nth(index);
      const recommendation = task.result.recommendations[index];
      assert.equal(await card.locator("h3").innerText(), recommendation.title);
      assert.equal(await card.locator(".recommendation-caption").innerText(), recommendation.caption);
      assert.equal(await card.locator(".source-tag").innerText(), recommendation.source_id);
      assert.equal(await card.locator(".source-details dd").nth(0).innerText(), recommendation.source_revision);
      assert.equal(await card.locator(".source-details dd").nth(1).innerText(), recommendation.record_id);
      assert.equal(await card.locator(".source-details dd").nth(2).innerText(), String(recommendation.version));
    }
    return { task_id: taskId, run_id: task.run_id, result_sha256: createHash("sha256").update(JSON.stringify(task.result)).digest("hex") };
  });
  await harness.page.getByRole("button", { name: "预览结果", exact: true }).click();
  const previewFrame = harness.page.frameLocator('iframe[title="宣传内容预览"]');
  await previewFrame.locator("h1").waitFor();
  await check("Harness审核预览与服务端结果一致", async () => {
    assert.equal(await previewFrame.locator("h1").innerText(), task.result.headline);
    assert.equal(await previewFrame.locator(".intro p").innerText(), task.result.summary);
    assert.equal(await previewFrame.locator(".cards article").count(), task.result.recommendations.length);
    for (let index = 0; index < task.result.recommendations.length; index += 1) {
      assert.equal(await previewFrame.locator(".cards article").nth(index).locator("h2").innerText(), task.result.recommendations[index].title);
    }
    return { task_id: taskId, preview_http_status: report.http.findLast(item => item.context === "harness" && item.path.endsWith("/preview"))?.status ?? null };
  });
  await harness.page.getByRole("button", { name: "关闭预览", exact: true }).click();
  await harness.page.getByRole("dialog").waitFor({ state: "hidden" });
  const [harnessExport] = await Promise.all([
    harness.page.waitForEvent("download"),
    harness.page.getByRole("link", { name: "下载 HTML", exact: true }).click(),
  ]);
  const harnessExportPath = path.join(directory, "harness-export.html");
  await harnessExport.saveAs(harnessExportPath);
  const harnessExportContext = await browser.newContext();
  contexts.push(harnessExportContext);
  const harnessExportPage = await harnessExportContext.newPage();
  pages.push(harnessExportPage);
  harnessExportPage.on("pageerror", error => report.page_errors.push({ workspace: "harness-export", message: error.message }));
  await harnessExportPage.goto(pathToFileURL(path.resolve(harnessExportPath)).href);
  await check("Harness独立HTML导出与服务端结果一致", async () => {
    assert.equal(await harnessExportPage.locator("h1").innerText(), task.result.headline);
    assert.equal(await harnessExportPage.locator(".intro p").innerText(), task.result.summary);
    assert.equal(await harnessExportPage.locator(".cards article").count(), task.result.recommendations.length);
    return { task_id: taskId, export_sha256: createHash("sha256").update(await fs.readFile(harnessExportPath)).digest("hex") };
  });
  await harness.page.getByRole("button", { name: "退出登录", exact: true }).click();
  await harness.page.getByLabel("用户名", { exact: true }).waitFor();
  const harnessAnonymous = await harness.context.request.get(baseUrl + "/api/harness/tasks/" + encodeURIComponent(taskId));
  recordHttp("harness-after-logout", harnessAnonymous, "GET");
  assert.equal(harnessAnonymous.status(), 401);
  report.checks.push({ name: "Harness退出后服务端拒绝读取", passed: true, result: { task_id: taskId, status: harnessAnonymous.status() } });

  const customer = await createWorkspace("customer", "/harness/?workspace=customer");
  const conversationId = inputs.customer.conversation_id;
  const conversation = await getJson(customer.context, "/api/customer/conversations/" + encodeURIComponent(conversationId), "customer");
  assert.equal(conversation.id, conversationId);
  const ordersPage = await getJson(customer.context, "/api/customer/orders?limit=10&offset=0", "customer");
  const conversationTicketNumbers = conversation.turns.map(turn => turn.result?.ticket?.number).filter(Boolean);
  const ticketNumber = inputs.customer.ticket_number ?? conversationTicketNumbers.at(-1) ?? null;
  if (inputs.customer.ticket_number) assert.ok(conversationTicketNumbers.includes(inputs.customer.ticket_number), "customer.ticket_number 不属于固定会话的已完成工单");
  const ticket = ticketNumber
    ? await getJson(customer.context, "/api/customer/tickets/" + encodeURIComponent(ticketNumber), "customer")
    : null;
  await customer.page.locator(".customer-conversation-button").filter({ hasText: conversation.title }).click();
  await customer.page.locator(".customer-chat-heading").waitFor();
  await check("客服订单列表与真实客户服务端记录一致", async () => {
    const cards = customer.page.locator(".customer-orders-panel .customer-order-card");
    assert.equal(await cards.count(), ordersPage.orders.length);
    for (const order of ordersPage.orders) {
      const card = cards.filter({ hasText: order.order_number });
      assert.equal(await card.count(), 1);
      assert.ok((await card.innerText()).includes(order.total_due));
    }
    return { conversation_orders: conversation.orders.map(order => order.order_number), order_numbers: ordersPage.orders.map(order => order.order_number), total: ordersPage.total };
  });
  await check("客服已完成会话与服务端记录一致", async () => {
    const completedTurns = conversation.turns.filter(turn => turn.status === "completed");
    assert.ok(completedTurns.length > 0, "固定会话没有已完成客服处理记录");
    const latest = completedTurns.at(-1);
    assert.equal(await customer.page.locator(".customer-result-text").last().innerText(), latest.result.text);
    if (latest.result.ticket) assert.ok((await customer.page.locator(".customer-ticket-card").innerText()).includes(latest.result.ticket.number));
    return { conversation_id: conversationId, completed_turn_ids: completedTurns.map(turn => turn.id), result_sha256: createHash("sha256").update(JSON.stringify(latest.result)).digest("hex") };
  });
  if (ticket) {
    await customer.page.getByRole("button", { name: "读取工单", exact: true }).click();
    await customer.page.locator(".customer-ticket-detail").waitFor();
    await check("客服已完成工单详情与服务端记录一致", async () => {
      const detail = customer.page.locator(".customer-ticket-detail");
      assert.ok((await detail.innerText()).includes(ticket.number));
      assert.ok((await detail.innerText()).includes(ticket.status));
      if (ticket.order_number) assert.ok((await detail.innerText()).includes(ticket.order_number));
      return { ticket_number: ticket.number, ticket_id: ticket.id, status: ticket.status };
    });
  } else {
    report.checks.push({ name: "客服已完成工单详情与服务端记录一致", passed: false, reason: "固定会话没有真实已完成工单编号" });
    throw new Error("固定客服会话缺少真实已完成工单，无法核对工单页面");
  }
  await customer.page.reload();
  await customer.page.locator(".customer-conversation-button").filter({ hasText: conversation.title }).click();
  await customer.page.locator(".customer-result-text").last().waitFor();
  await check("客服会话与订单reload后保持服务端结果", async () => {
    assert.equal(await customer.page.locator(".customer-result-text").last().innerText(), conversation.turns.filter(turn => turn.status === "completed").at(-1).result.text);
    const reloadedOrders = await getJson(customer.context, "/api/customer/orders?limit=10&offset=0", "customer");
    assert.deepEqual(reloadedOrders.orders.map(order => order.order_number), ordersPage.orders.map(order => order.order_number));
    return { conversation_id: conversationId, visible_orders: reloadedOrders.orders.map(order => order.order_number) };
  });
  await customer.page.getByRole("button", { name: "退出登录", exact: true }).click();
  await customer.page.getByLabel("用户名", { exact: true }).waitFor();
  const customerAnonymous = await customer.context.request.get(baseUrl + "/api/customer/conversations/" + encodeURIComponent(conversationId));
  recordHttp("customer-after-logout", customerAnonymous, "GET");
  assert.equal(customerAnonymous.status(), 401);
  report.checks.push({ name: "客服退出后服务端拒绝读取", passed: true, result: { conversation_id: conversationId, status: customerAnonymous.status() } });

  const assessment = await createWorkspace("assessment", "/harness/?workspace=assessment");
  const assessmentRunId = inputs.assessment.run_id;
  const identity = await getJson(assessment.context, "/api/assessment/me", "assessment");
  const status = await getJson(assessment.context, "/api/assessment/status", "assessment");
  const employees = await getJson(assessment.context, "/api/assessment/employees", "assessment");
  const runs = await getJson(assessment.context, "/api/assessment/runs", "assessment");
  const listedRun = runs.find(item => item.id === assessmentRunId || item.run_id === assessmentRunId);
  assert.ok(listedRun, "固定 assessment.run_id 未出现在当前账号的服务端历史任务中");
  const run = await getJson(assessment.context, "/api/assessment/runs/" + encodeURIComponent(listedRun.id), "assessment");
  assert.ok(run.id === listedRun.id && (run.id === assessmentRunId || run.run_id === assessmentRunId));
  assert.equal(run.status, "completed");
  assert.ok(run.result);
  const listedRunIndex = runs.findIndex(item => item.id === run.id);
  assert.ok(listedRunIndex >= 0, "固定考核报告未出现在历史列表中");
  const defaultEmployeeId = identity.employee_id ?? identity.allowed_employee_ids[0] ?? employees[0]?.employee_id;
  const employeeId = inputs.assessment.employee_id ?? defaultEmployeeId;
  assert.ok(employeeId !== null && employeeId !== undefined, "assessment.employee_id 或员工账号绑定编号必填");
  const evidence = await getJson(assessment.context, "/api/assessment/evidence/" + encodeURIComponent(String(employeeId)), "assessment");
  const scope = identity.level === "employee" ? "self" : identity.level === "manager" ? "team" : "organization";
  const metrics = await getJson(assessment.context, "/api/assessment/metrics?scope=" + scope, "assessment");
  assert.ok(employees.some(employee => employee.employee_id === employeeId));
  assert.equal(status.status, "ready");
  await assessment.page.locator(".assessment-run-item").nth(listedRunIndex).click();
  await assessment.page.getByText("运行编号 " + run.run_id, { exact: true }).waitFor();
  const employeeSelect = assessment.page.locator("#assessment-employee");
  if (await employeeSelect.count()) await employeeSelect.selectOption(String(employeeId));
  await assessment.page.locator(".assessment-metrics-meta").waitFor();
  await assessment.page.getByRole("heading", { name: "员工 " + employeeId + " 的依据", exact: true }).waitFor();
  await assessment.page.locator(".assessment-evidence-panel .assessment-data-notice").waitFor();
  const reportEvidence = assessment.page.locator(".assessment-run-detail-panel details").filter({ hasText: "本报告保存的订单与目标依据" });
  if (run.result.evidence) await reportEvidence.locator("summary").click();
  await check("考核员工账号、指标、来源与历史报告DOM匹配服务端", async () => {
    assert.ok((await assessment.page.locator(".assessment-source-banner").innerText()).includes(status.source_id));
    assert.ok((await assessment.page.locator(".assessment-source-banner").innerText()).includes(status.revision));
    assert.ok((await assessment.page.locator(".assessment-run-detail-panel").innerText()).includes(run.result.headline));
    assert.ok((await assessment.page.locator(".assessment-run-detail-panel").innerText()).includes(run.run_id));
    if (run.result.evidence) assert.deepEqual(JSON.parse(await reportEvidence.locator("pre").innerText()), run.result.evidence);
    const metricArticles = assessment.page.locator(".assessment-employee-metrics-list .assessment-employee-metrics");
    assert.equal(await metricArticles.count(), metrics.employees.length);
    for (let index = 0; index < metrics.employees.length; index += 1) {
      const employee = metrics.employees[index];
      const article = metricArticles.nth(index);
      assert.equal((await article.locator(".assessment-employee-metrics-heading span").innerText()).trim(), "员工 " + employee.employee_id);
      assert.ok((await article.innerText()).includes(employee.job_title));
      const shownValues = await article.locator(".assessment-metric-card strong").allInnerTexts();
      assert.deepEqual(shownValues, employee.metrics.map(metric => metric.display));
    }
    const metricsSourceDetails = assessment.page.locator(".assessment-metrics-panel details").filter({ hasText: "来源与规则信息" });
    await metricsSourceDetails.locator("summary").click();
    assert.deepEqual(JSON.parse(await metricsSourceDetails.locator("pre").innerText()), metrics.source);
    assert.equal((await assessment.page.locator(".assessment-evidence-panel h2").innerText()).trim(), "员工 " + employeeId + " 的依据");
    const evidenceRows = await assessment.page.locator(".assessment-evidence-table tbody tr").evaluateAll(rows => rows.map(row => [...row.querySelectorAll("td")].map(cell => cell.innerText.trim())));
    assert.deepEqual(evidenceRows, evidence.orders.map(order => [String(order.order_id), order.order_date, order.subtotal, String(order.status), String(order.source_line)]));
    const evidenceLowerPanels = assessment.page.locator(".assessment-evidence-lower-grid section");
    if (evidence.quota) assert.deepEqual(JSON.parse(await evidenceLowerPanels.nth(0).locator("pre").innerText()), evidence.quota);
    assert.deepEqual(JSON.parse(await evidenceLowerPanels.nth(1).locator("pre").innerText()), evidence.source);
    return {
      user_id: identity.user_id,
      employee_id: employeeId,
      allowed_employee_ids: identity.allowed_employee_ids,
      source_id: status.source_id,
      revision: status.revision,
      employee_count: employees.length,
      report_run_id: run.run_id,
      report_sha256: createHash("sha256").update(JSON.stringify(run.result)).digest("hex"),
      evidence_order_count: evidence.orders.length,
      metrics_employee_ids: metrics.employees.map(item => item.employee_id),
    };
  });
  const [assessmentExport] = await Promise.all([
    assessment.page.waitForEvent("download"),
    assessment.page.getByRole("link", { name: "下载 HTML", exact: true }).click(),
  ]);
  const assessmentExportPath = path.join(directory, "assessment-export.html");
  await assessmentExport.saveAs(assessmentExportPath);
  const assessmentExportContext = await browser.newContext();
  contexts.push(assessmentExportContext);
  const assessmentExportPage = await assessmentExportContext.newPage();
  pages.push(assessmentExportPage);
  assessmentExportPage.on("pageerror", error => report.page_errors.push({ workspace: "assessment-export", message: error.message }));
  await assessmentExportPage.goto(pathToFileURL(path.resolve(assessmentExportPath)).href);
  await check("考核HTML报告导出与服务端报告一致", async () => {
    const exportedText = await assessmentExportPage.locator("body").innerText();
    assert.ok(exportedText.includes(run.result.headline));
    assert.ok(exportedText.includes(run.run_id));
    assert.ok(exportedText.includes(status.source_id));
    return { run_id: run.run_id, export_sha256: createHash("sha256").update(await fs.readFile(assessmentExportPath)).digest("hex") };
  });
  await assessment.page.getByRole("button", { name: "退出登录", exact: true }).click();
  await assessment.page.getByLabel("用户名", { exact: true }).waitFor();
  const assessmentAnonymous = await assessment.context.request.get(baseUrl + "/api/assessment/runs/" + encodeURIComponent(assessmentRunId));
  recordHttp("assessment-after-logout", assessmentAnonymous, "GET");
  assert.equal(assessmentAnonymous.status(), 401);
  report.checks.push({ name: "考核退出后服务端拒绝读取", passed: true, result: { run_id: assessmentRunId, status: assessmentAnonymous.status() } });

  assert.deepEqual(report.page_errors, [], "浏览器出现未处理pageerror");
  report.checks.push({ name: "所有页面pageerror为空", passed: true, result: { page_count: pages.length } });
  report.status = "passed";
} catch (error) {
  report.failure = error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : String(error);
  report.errors.push(report.failure);
  report.status = "failed";
  throw error;
} finally {
  await save();
  for (const context of contexts.reverse()) await context.close();
  await browser.close();
  console.log(path.join(directory, "report.json"));
}
