import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createHash, randomUUID } from "node:crypto";

const inputPath = process.argv[2];
assert.ok(inputPath, "需要提供运行前固定的评价输入");
const probe = process.argv.includes("--probe");
const selectedIds = process.argv.find(value => value.startsWith("--cases="))?.slice(8).split(",");
assert.ok(!selectedIds || probe, "指定场景只适用于阶段补充检查");
const inputsBytes = await fs.readFile(inputPath);
const inputs = JSON.parse(inputsBytes);
const baseUrl = process.env.SHOP_CUSTOMER_URL ?? "http://127.0.0.1:8001";
process.env.PLAYWRIGHT_BROWSERS_PATH = path.resolve(".scratch/playwright");
process.env.TMPDIR = path.resolve(".scratch/browser-tmp");
await fs.mkdir(process.env.TMPDIR, { recursive: true });
const { chromium } = await import(pathToFileURL(path.join(process.env.SHOP_BROWSER_NODE_MODULES, "playwright", "index.mjs")));
const runId = (probe ? "cus02-probe-" : "cus05-business-") + new Date().toISOString().replaceAll(":", "").replaceAll("-", "") + "-" + randomUUID().slice(0, 8);
const directory = path.join(".scratch/evidence", runId);
await fs.mkdir(directory, { recursive: true });
const sourceFiles = ["src/shop/customer_agent.py", "src/shop/customer_provider.py", "src/shop/customer_runtime.py",
  "src/shop/customer_worker.py", "src/shop/customer_runtime_models.py", "src/shop/customer_api.py", "src/shop/orders.py",
  "src/shop/budget.py", "frontend/src/CustomerApp.tsx", "frontend/src/customer.css", "scripts/evaluate_customer_browser.mjs"];
async function hashes() {
  const result = {};
  for (const file of sourceFiles) result[file] = createHash("sha256").update(await fs.readFile(file)).digest("hex");
  return result;
}
const report = { run_id: runId, scope: probe ? "CUS-02及CUS-03未覆盖业务路径" : "CUS-05固定30场景各三次",
  input_path: inputPath, input_sha256: createHash("sha256").update(inputsBytes).digest("hex"),
  source_hashes_before: await hashes(), started_at: new Date().toISOString(), cases: [], errors: [], status: "running" };
async function save() { await fs.writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2)); }
await save();
const browser = await chromium.launch({ headless: true });
const sessions = {};
async function json(response) {
  assert.ok(response.ok(), "HTTP " + response.status() + " " + response.url());
  return response.json();
}
async function login(customerId) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await json(await context.request.post(baseUrl + "/api/login", { data: { username: "customer-" + customerId,
    password: (await fs.readFile(".local/auth/customer-" + customerId + ".password", "utf8")).trim() } }));
  const page = await context.newPage();
  page.on("pageerror", error => report.errors.push({ customerId, message: error.message }));
  await page.goto(baseUrl + "/harness/?workspace=customer");
  await page.getByRole("button", { name: "退出登录", exact: true }).waitFor();
  return { context, page };
}
async function newConversation(session, title) {
  const conversation = await json(await session.context.request.post(baseUrl + "/api/customer/conversations", { data: { title } }));
  await session.page.reload();
  await session.page.getByRole("button", { name: new RegExp(title) }).click();
  await session.page.getByPlaceholder("输入订单或商品问题，按 Enter 发送，Shift + Enter 换行").waitFor();
  return conversation;
}
const expectedText = {
  image_link: "订单商品与素材中心图片没有来源关联", current_price: "无法确认当前销售价格",
  inventory: "没有实时库存信息", policy: "没有适用的售后政策", unknown_attribute: "没有提供该属性",
  customer_scope: "当前登录账号本人的订单", refund: "不能执行退款或修改订单",
};
function facts(result, customerId) {
  const orders = inputs.orders[String(customerId)];
  for (const order of result.orders) {
    const expected = orders[order.order_number];
    assert.ok(expected, "答复出现其他客户或不存在的订单");
    for (const key of ["order_id", "order_date", "due_date", "ship_date", "total_due", "subtotal", "tax", "freight", "status", "status_label", "item_count", "source"])
      assert.deepEqual(order[key], expected[key], "订单字段没有正确来源: " + key);
    assert.deepEqual(order.items, expected.items.slice(0, order.items.length), "订单商品或数量与来源不一致");
    assert.equal(order.remaining_item_count, expected.items.length - order.items.length);
    assert.ok(result.text.includes(order.order_number) && result.text.includes(order.total_due), "答复文字缺少对应的订单事实");
  }
  for (const product of result.products) {
    const expected = inputs.products[product.asset_id];
    assert.ok(expected, "商品引用不在固定主来源内");
    for (const key of ["title", "version", "source_id", "source_revision", "record_id", "original_fields"])
      assert.deepEqual(product[key], expected[key], "商品资料与来源不一致: " + key);
    assert.ok(result.text.includes(product.title));
  }
}
function validate(expect, turn, history, customerId = 11091) {
  assert.equal(turn.status, "completed", turn.error ?? "没有业务答复");
  const result = turn.result;
  assert.ok(result?.text?.trim());
  facts(result, customerId);
  if (expect === "products_a" || expect === "products_b") {
    const allowed = expect === "products_a" ? inputs.allowed_a : inputs.allowed_b;
    assert.ok(result.products.length > 0, "有来源匹配的商品查询没有结果");
    assert.ok(result.products.every(item => allowed.includes(item.asset_id)), "商品违反指定条件");
  } else if (expect === "previous_product" || expect === "first_product") {
    const previous = expect === "first_product" ? history[0].turn.result.products[0] : history.at(-1).turn.result.products[0];
    assert.equal(result.products.length, 1);
    assert.equal(result.products[0].asset_id, previous.asset_id);
  } else if (expect === "no_product") {
    assert.equal(result.products.length, 0);
    assert.ok(/没有|无法/.test(result.text));
  } else if (["recent_orders", "candidates", "earlier_orders"].includes(expect)) {
    const expected = expect === "earlier_orders" ? inputs.sequences[String(customerId)].slice(20) : inputs.sequences[String(customerId)].slice(0, 5);
    assert.deepEqual(result.orders.map(item => item.order_number), expected);
    assert.ok(result.text.includes("确认") || expect === "earlier_orders");
  } else if (["first_order", "second_order", "long_order"].includes(expect)) {
    const target = expect === "first_order" ? inputs.sequences[String(customerId)][0]
      : expect === "second_order" ? inputs.sequences[String(customerId)][1] : inputs.values.order_long;
    assert.equal(result.orders.length, 1);
    assert.equal(result.orders[0].order_number, target);
    assert.deepEqual(result.orders[0].items, inputs.orders[String(customerId)][target].items);
  } else if (expect === "ticket") {
    assert.ok(result.ticket?.number?.startsWith("CS-"));
    assert.equal(result.ticket.order_number, inputs.values.order_first);
    assert.equal(result.ticket.status, "open");
  } else if (expect === "ticket_offer") {
    assert.ok(result.pending_ticket?.id);
    assert.equal(result.ticket, null);
    assert.equal(result.pending_ticket.order_number, inputs.values.order_first);
  } else if (expect === "ticket_cancelled") {
    assert.equal(result.ticket, null);
    assert.equal(result.pending_ticket, null);
    assert.equal(result.kind, "ticket_cancelled");
  } else if (["invalid_order", "unavailable_order", "no_foreign_ticket", "safe_denial"].includes(expect)) {
    assert.equal(result.orders.length, 0);
    assert.equal(result.ticket, null);
    assert.ok(/无法|只能|请提供|不能|没有找到/.test(result.text));
  } else {
    assert.ok(result.text.includes(expectedText[expect]), "缺少正确的资料范围说明");
    assert.equal(result.ticket, null);
  }
}
async function waitAnswer(session, turn, record) {
  const started = Date.now();
  let state;
  while (Date.now() - started < 125000) {
    state = await json(await session.context.request.get(baseUrl + "/api/customer/turns/" + turn.id));
    if (!["queued", "running"].includes(state.status)) break;
    await session.page.waitForTimeout(100);
  }
  record.turn = state;
  if (state.status === "completed") {
    const selector = '[data-turn-id="' + state.id + '"] .customer-result-text';
    await session.page.locator(selector).waitFor({ timeout: 5000 });
    assert.equal(await session.page.locator(selector).innerText(), state.result.text);
    record.browser_displayed_at = new Date().toISOString();
    record.latency_seconds = (Date.now() - Date.parse(state.received_at)) / 1000;
    record.query = state.tools.some(item => item.name !== "respond");
  } else {
    record.latency_seconds = "no_answer";
    record.query = state.tools.some(item => item.name !== "respond");
  }
  await save();
  return state;
}
async function executeStep(session, conversation, step, record, history) {
  const page = session.page;
  if (step.action === "read_ticket") {
    const previous = history.at(-1).turn.result.ticket;
    record.ticket = await json(await session.context.request.get(baseUrl + "/api/customer/tickets/" + previous.number));
    assert.equal(record.ticket.id, previous.id);
    await page.getByRole("button", { name: "读取工单", exact: true }).last().click();
    await page.getByText(previous.number, { exact: false }).last().waitFor();
    record.passed = true;
    return;
  }
  let submitted;
  let otherTurn;
  if (step.action === "concurrent" || step.action === "two_customers") {
    const requestId = randomUUID();
    const data = { request_id: requestId, message: step.message };
    if (step.action === "concurrent") {
      const responses = await Promise.all([session.context.request.post(baseUrl + "/api/customer/conversations/" + conversation.id + "/messages", { data }),
        session.context.request.post(baseUrl + "/api/customer/conversations/" + conversation.id + "/messages", { data: { request_id: randomUUID(), message: "并发消息：查询订单" + inputs.values.order_first } })]);
      assert.deepEqual(responses.map(response => response.status()).sort(), [200, 409]);
      submitted = await json(responses.find(response => response.status() === 200));
      record.concurrent_statuses = responses.map(response => response.status());
    } else {
      const other = sessions[11176];
      const otherConversation = await newConversation(other, conversation.title + "-other");
      const responses = await Promise.all([session.context.request.post(baseUrl + "/api/customer/conversations/" + conversation.id + "/messages", { data }),
        other.context.request.post(baseUrl + "/api/customer/conversations/" + otherConversation.id + "/messages", { data: { request_id: randomUUID(), message: "查询订单" + inputs.values.other_order + "的完整明细" } })]);
      submitted = await json(responses[0]);
      otherTurn = await json(responses[1]);
      record.other = { turn_id: otherTurn.id };
      const finalOther = await waitAnswer(other, otherTurn, record.other);
      validate("first_order", finalOther, [], 11176);
    }
  } else {
    const endpoint = step.action === "confirm" ? "/confirm" : "/messages";
    const pending = page.waitForResponse(response => response.url().endsWith(endpoint) && response.request().method() === "POST");
    if (step.action === "confirm") {
      await page.getByRole("button", { name: "确认提交", exact: true }).click();
    } else {
      await page.getByPlaceholder("输入订单或商品问题，按 Enter 发送，Shift + Enter 换行").fill(step.message);
      await page.getByRole("button", { name: "发送", exact: true }).click();
    }
    const response = await pending;
    submitted = await json(response);
    if (step.action === "duplicate") {
      const data = response.request().postDataJSON();
      const repeated = await json(await session.context.request.post(response.url(), { data }));
      assert.equal(repeated.id, submitted.id);
      record.duplicate_turn_id = repeated.id;
    }
  }
  record.turn_id = submitted.id;
  const final = await waitAnswer(session, submitted, record);
  validate(step.expect, final, history);
  if (final.result?.ticket) {
    const actual = await json(await session.context.request.get(baseUrl + "/api/customer/tickets/" + final.result.ticket.number));
    assert.equal(actual.id, final.result.ticket.id);
  }
  record.passed = true;
}
function percentile(values) {
  const sorted = values.map(value => typeof value === "number" ? value : Infinity).sort((a, b) => a - b);
  return sorted.length ? sorted[Math.ceil(sorted.length * 0.95) - 1] : null;
}
try {
  sessions[11091] = await login(11091);
  sessions[11176] = await login(11176);
  const cases = probe ? inputs.cases.filter(item => (selectedIds ?? ["CP04", "CO03", "CT02", "CT03"]).includes(item.id)) : inputs.cases;
  assert.ok(cases.length > 0);
  for (const definition of cases) {
    for (let repetition = 0; repetition < (probe ? 1 : 3); repetition++) {
      const session = sessions[11091];
      const title = runId + "-" + definition.id + "-" + repetition;
      const record = { id: definition.id, group: definition.group, repetition: repetition + 1, steps: [], passed: false };
      report.cases.push(record);
      try {
        const conversation = await newConversation(session, title);
        record.conversation_id = conversation.id;
        assert.equal(conversation.orders.length, 20);
        for (const step of definition.steps) {
          const evidence = { input: step };
          const history = [...record.steps];
          record.steps.push(evidence);
          await executeStep(session, conversation, step, evidence, history);
        }
        record.passed = true;
      } catch (error) {
        record.failure = String(error);
      }
      await save();
      console.log(JSON.stringify({ id: record.id, repetition: record.repetition, passed: record.passed, failure: record.failure }));
    }
  }
  const ordinary = [], query = [];
  for (const item of report.cases) for (const step of item.steps) {
    if (step.turn_id) (step.query ? query : ordinary).push(step.latency_seconds ?? "no_answer");
    if (step.other?.turn_id) query.push(step.other.latency_seconds ?? "no_answer");
  }
  const successes = report.cases.filter(item => item.passed).length;
  const consistent = inputs.cases.filter(definition => report.cases.filter(item => item.id === definition.id && item.passed).length === 3).length;
  const ordinaryP95 = percentile(ordinary), queryP95 = percentile(query);
  report.metrics = { successes, total: report.cases.length, consistent_cases: consistent,
    ordinary_samples: ordinary.length, query_samples: query.length,
    ordinary_p95_seconds: Number.isFinite(ordinaryP95) ? ordinaryP95 : "no_answer",
    query_p95_seconds: Number.isFinite(queryP95) ? queryP95 : "no_answer" };
  report.source_hashes_after = await hashes();
  assert.deepEqual(report.source_hashes_after, report.source_hashes_before);
  assert.deepEqual(report.errors, []);
  report.status = probe ? (successes === cases.length ? "passed" : "failed")
    : (successes >= 86 && consistent >= 27 && ordinaryP95 <= 5 && queryP95 <= 8 ? "passed" : "failed");
} finally {
  report.completed_at = new Date().toISOString();
  await save();
  await browser.close();
  console.log(path.join(directory, "report.json"));
}
if (report.status !== "passed") process.exitCode = 1;
