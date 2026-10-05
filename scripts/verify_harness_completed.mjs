import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID, createHash } from "node:crypto";

const completedTaskId = process.argv[2];
const verifyEscaping = process.argv[3] === "--escaping";
assert.ok(completedTaskId, "需要提供真实 completed materials_found task_id");
assert.ok(process.argv.length === 3 || (process.argv.length === 4 && verifyEscaping),
  "用法: node scripts/verify_harness_completed.mjs <task_id> [--escaping]");
const escapingHeadline = "<script>window.harnessEscapingProbe = 1</script>";
const escapingSummary = '<img src=x onerror="window.harnessEscapingProbe=2">';

const modulesPath = process.env.SHOP_BROWSER_NODE_MODULES;
assert.ok(modulesPath, "需要配置SHOP_BROWSER_NODE_MODULES");
const browserPath = path.resolve(".scratch/playwright");
const temporaryPath = path.resolve(".scratch/browser-tmp");
process.env.PLAYWRIGHT_BROWSERS_PATH = browserPath;
process.env.TMPDIR = temporaryPath;
await fs.mkdir(temporaryPath, { recursive: true });

const { chromium } = await import(pathToFileURL(path.join(modulesPath, "playwright", "index.mjs")));
const runId = "har-completed-" + new Date().toISOString().replaceAll(":", "").replaceAll("-", "").replace(/\.\d{3}Z$/, "Z") + "-" + randomUUID().slice(0, 8);
const directory = path.join(".scratch/evidence", runId);
await fs.mkdir(directory, { recursive: true });

const sourceHashes = {};
for (const sourcePath of [
  "frontend/src/App.tsx",
  "frontend/src/styles.css",
  "frontend/src/api.ts",
  "src/shop/harness_api.py",
  "src/shop/campaign.py",
  "src/shop/templates/campaign.html",
  "scripts/verify_harness_completed.mjs",
]) {
  const content = await fs.readFile(sourcePath);
  sourceHashes[sourcePath] = createHash("sha256").update(content).digest("hex");
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 1000 },
  acceptDownloads: true,
});
const page = await context.newPage();
const checks = [];
const pageErrors = [];
const requestCounts = { browser: {}, exported_html: { http_requests: [], failed_requests: [] } };
const imageChecks = { preview: [], exported_html: [] };
const taskListResponses = [];
page.on("pageerror", error => pageErrors.push({ page: "workspace", message: error.message }));
page.on("response", response => {
  const url = new URL(response.url());
  if (url.pathname === "/api/harness/tasks" && response.request().method() === "GET" && response.ok()) {
    taskListResponses.push(response);
  }
});
page.on("request", request => {
  const url = new URL(request.url());
  const key = request.method() + " " + url.origin + url.pathname;
  requestCounts.browser[key] = (requestCounts.browser[key] ?? 0) + 1;
});
const layoutResults = [];
let taskBefore = null;
let taskAfter = null;
let browserVersion = null;
let exportPath = null;
let status = "failed";
let failure = null;

function requestSnapshot(task) {
  return {
    run_id: task.run_id,
    status: task.status,
    result_sha256: createHash("sha256").update(JSON.stringify(task.result)).digest("hex"),
    result: {
      status: task.result?.status,
      headline: task.result?.headline,
      summary: task.result?.summary,
      recommendations: task.result?.recommendations?.map(item => ({
        asset_id: item.asset_id,
        title: item.title,
        caption: item.caption,
        file_sha256: item.file_sha256,
        source_id: item.source_id,
        source_revision: item.source_revision,
        record_id: item.record_id,
        version: item.version,
      })),
    },
    model_calls: task.model_calls,
    tool_calls: task.tool_calls,
    requests: task.requests?.map(request => ({
      id: request.id,
      step_key: request.step_key,
      status: request.status,
      usage: request.usage,
      reserved_cny: request.reserved_cny,
      charged_cny: request.charged_cny,
      provider_request_id: request.provider_request_id,
    })) ?? null,
    tools: task.tools?.map(tool => ({
      call_id: tool.call_id,
      name: tool.name,
      status: tool.status,
      error: tool.error,
    })) ?? null,
  };
}

function assertImageFilesMatch(images, recommendations) {
  assert.equal(images.length, recommendations.length);
  const evidence = [];
  for (let index = 0; index < images.length; index += 1) {
    const source = images[index].source;
    const match = /^data:image\/[^;,]+;base64,([A-Za-z0-9+/]+={0,2})$/.exec(source);
    assert.ok(match, "图片没有使用base64 data:image URL");
    const digest = createHash("sha256").update(Buffer.from(match[1], "base64")).digest("hex");
    assert.equal(digest, recommendations[index].file_sha256, "内嵌图片与任务记录中的素材文件摘要不同");
    evidence.push({ asset_id: recommendations[index].asset_id, file_sha256: recommendations[index].file_sha256, embedded_sha256: digest });
  }
  return evidence;
}

try {
  browserVersion = browser.version();
  await page.goto("http://127.0.0.1:8000/harness/");
  await page.getByLabel("用户名", { exact: true }).fill("admin");
  const password = (await fs.readFile(".local/auth/admin.password", "utf8")).trim();
  await page.getByLabel("密码", { exact: true }).fill(password);
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.getByRole("navigation", { name: "任务列表", exact: true }).waitFor();
  checks.push("真实服务登录并读取任务列表");

  const taskResponse = await context.request.get("http://127.0.0.1:8000/api/harness/tasks/" + encodeURIComponent(completedTaskId));
  assert.ok(taskResponse.ok(), "指定任务接口读取失败: " + taskResponse.status());
  taskBefore = await taskResponse.json();
  assert.equal(taskBefore.task_id, completedTaskId);
  assert.equal(taskBefore.status, "completed");
  assert.equal(taskBefore.result?.status, "materials_found");
  assert.ok(Array.isArray(taskBefore.result.recommendations) && taskBefore.result.recommendations.length > 0);
  if (verifyEscaping) {
    assert.equal(taskBefore.result.headline, escapingHeadline, "真实任务标题没有保留转义测试文字");
    assert.ok(taskBefore.result.summary.includes(escapingSummary), "真实任务摘要没有保留转义测试文字");
  }
  checks.push("指定任务来自实际服务且状态为materials_found");

  const listingResponse = taskListResponses.at(-1);
  assert.ok(listingResponse, "登录后没有观察到成功的任务列表响应");
  const listedTasks = await listingResponse.json();
  const targetPosition = listedTasks.findIndex(task => task.task_id === completedTaskId);
  assert.ok(targetPosition >= 0, "指定任务不在当前登录账号的任务列表中");
  const taskRows = page.getByRole("navigation", { name: "任务列表", exact: true }).getByRole("button");
  assert.equal(await taskRows.count(), listedTasks.length, "界面任务列表与真实服务返回数量不一致");
  const taskRow = taskRows.nth(targetPosition);
  assert.ok((await taskRow.textContent()).includes(taskBefore.prompt));
  await taskRow.click();
  await page.getByRole("heading", { name: taskBefore.prompt, exact: true }).waitFor();
  assert.equal(await page.locator(".result-summary").textContent(), taskBefore.result.summary);
  assert.equal(await page.locator(".result-section h2").textContent(), taskBefore.result.headline);
  const cards = page.locator(".recommendation-card");
  assert.equal(await cards.count(), taskBefore.result.recommendations.length);
  for (let index = 0; index < taskBefore.result.recommendations.length; index += 1) {
    const expected = taskBefore.result.recommendations[index];
    const card = cards.nth(index);
    assert.equal(await card.locator("h3").textContent(), expected.title);
    assert.equal(await card.locator(".recommendation-caption").textContent(), expected.caption);
    assert.equal(await card.locator(".source-tag").textContent(), expected.source_id);
    assert.equal(await card.locator(".source-details dd").nth(0).textContent(), expected.source_revision);
    assert.equal(await card.locator(".source-details dd").nth(1).textContent(), expected.record_id);
    assert.equal(await card.locator(".source-details dd").nth(2).textContent(), String(expected.version));
  }
  checks.push("任务列表打开结果并显示真实标题、summary、推荐文案与来源");

  await page.getByRole("button", { name: "预览结果", exact: true }).click();
  const frame = page.frameLocator('iframe[title="宣传内容预览"]');
  await frame.locator("h1").waitFor();
  assert.equal(await frame.locator("h1").textContent(), taskBefore.result.headline);
  assert.equal(await frame.locator(".intro > p").textContent(), taskBefore.result.summary);
  const previewCards = frame.locator(".cards article");
  assert.equal(await previewCards.count(), taskBefore.result.recommendations.length);
  for (let index = 0; index < taskBefore.result.recommendations.length; index += 1) {
    const expected = taskBefore.result.recommendations[index];
    const card = previewCards.nth(index);
    assert.equal(await card.locator("h2").textContent(), expected.title);
    assert.equal(await card.locator(".copy > p").textContent(), expected.caption);
    assert.equal(await card.locator("details dd").nth(0).textContent(), expected.source_id);
    assert.equal(await card.locator("details dd").nth(1).textContent(), expected.source_revision);
    assert.equal(await card.locator("details dd").nth(2).textContent(), expected.record_id);
  }
  const previewImages = frame.locator("img");
  await previewImages.evaluateAll(images => Promise.all(images.map(image => image.decode())));
  const previewImageState = await previewImages.evaluateAll(images => images.map(image => ({
    complete: image.complete,
    naturalWidth: image.naturalWidth,
    source: image.getAttribute("src"),
    alt: image.alt,
  })));
  assert.equal(previewImageState.length, taskBefore.result.recommendations.length);
  for (let index = 0; index < previewImageState.length; index += 1) {
    assert.ok(previewImageState[index].complete && previewImageState[index].naturalWidth > 0);
    assert.ok(previewImageState[index].source.startsWith("data:image/"));
    assert.equal(previewImageState[index].alt, taskBefore.result.recommendations[index].title);
  }
  imageChecks.preview = assertImageFilesMatch(previewImageState, taskBefore.result.recommendations);
  assert.equal(await frame.locator("script").count(), 0, "实际预览包含脚本元素");
  if (verifyEscaping) {
    assert.equal(await frame.locator("[onerror]").count(), 0);
    assert.equal(await frame.locator("html").evaluate(() => window.harnessEscapingProbe), undefined);
    assert.equal(await page.evaluate(() => window.harnessEscapingProbe), undefined);
    checks.push("literal_script_and_img_text_escaped_in_workspace_and_preview");
  }
  checks.push("实际预览iframe包含对应推荐且图片完成加载");

  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    await page.setViewportSize(viewport);
    const layout = await page.evaluate(() => ({
      width: document.documentElement.clientWidth,
      scrollWidth: document.documentElement.scrollWidth,
      iframeWidth: document.querySelector('iframe[title="宣传内容预览"]')?.getBoundingClientRect().width ?? 0,
    }));
    const frameLayout = await frame.locator("html").evaluate(element => ({
      width: element.clientWidth,
      scrollWidth: element.scrollWidth,
    }));
    assert.ok(layout.scrollWidth <= layout.width + 1, JSON.stringify({ viewport, layout }));
    assert.ok(frameLayout.scrollWidth <= frameLayout.width + 1, JSON.stringify({ viewport, frameLayout }));
    assert.ok(layout.iframeWidth > 0);
    layoutResults.push({ viewport, page: layout, preview: frameLayout });
  }
  checks.push("已完成预览在1440和390宽度下无横向溢出");

  await page.getByRole("button", { name: "关闭预览", exact: true }).click();
  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("link", { name: "下载 HTML", exact: true }).click();
  const download = await downloadPromise;
  exportPath = path.join(directory, "campaign.html");
  await download.saveAs(exportPath);

  const exportedContext = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  assert.deepEqual(await exportedContext.cookies(), [], "独立HTML打开前浏览器context应没有登录cookie");
  const exportedPage = await exportedContext.newPage();
  exportedPage.on("pageerror", error => pageErrors.push({ page: "exported_html", message: error.message }));
  exportedPage.on("request", request => {
    const url = new URL(request.url());
    if (url.protocol === "http:" || url.protocol === "https:") {
      requestCounts.exported_html.http_requests.push({ method: request.method(), origin: url.origin, path: url.pathname });
    }
  });
  exportedPage.on("requestfailed", request => {
    const url = new URL(request.url());
    if (url.protocol === "http:" || url.protocol === "https:") {
      requestCounts.exported_html.failed_requests.push({ method: request.method(), origin: url.origin, path: url.pathname });
    }
  });
  await exportedPage.goto(pathToFileURL(path.resolve(exportPath)).href);
  assert.equal(await exportedPage.locator("h1").textContent(), taskBefore.result.headline);
  assert.equal(await exportedPage.locator(".intro > p").textContent(), taskBefore.result.summary);
  assert.ok(await exportedPage.locator("style").count() > 0, "导出HTML缺少内嵌样式");
  assert.equal(await exportedPage.locator("script").count(), 0, "导出HTML包含脚本元素");
  if (verifyEscaping) {
    assert.equal(await exportedPage.locator("[onerror]").count(), 0);
    assert.equal(await exportedPage.evaluate(() => window.harnessEscapingProbe), undefined);
    checks.push("literal_script_and_img_text_escaped_in_independent_html");
  }
  assert.equal(await exportedPage.locator("link[rel=stylesheet]").count(), 0, "导出HTML依赖外部样式");
  const exportedCards = exportedPage.locator(".cards article");
  assert.equal(await exportedCards.count(), taskBefore.result.recommendations.length);
  for (let index = 0; index < taskBefore.result.recommendations.length; index += 1) {
    const expected = taskBefore.result.recommendations[index];
    const card = exportedCards.nth(index);
    assert.equal(await card.locator("h2").textContent(), expected.title);
    assert.equal(await card.locator(".copy > p").textContent(), expected.caption);
    assert.equal(await card.locator("details dd").nth(0).textContent(), expected.source_id);
    assert.equal(await card.locator("details dd").nth(1).textContent(), expected.source_revision);
    assert.equal(await card.locator("details dd").nth(2).textContent(), expected.record_id);
  }
  const exportedImages = exportedPage.locator("img");
  await exportedImages.evaluateAll(images => Promise.all(images.map(image => image.decode())));
  const exportedImageState = await exportedImages.evaluateAll(images => images.map(image => ({
    complete: image.complete,
    naturalWidth: image.naturalWidth,
    source: image.getAttribute("src"),
    alt: image.alt,
  })));
  assert.equal(exportedImageState.length, taskBefore.result.recommendations.length);
  for (let index = 0; index < exportedImageState.length; index += 1) {
    assert.ok(exportedImageState[index].complete && exportedImageState[index].naturalWidth > 0);
    assert.ok(exportedImageState[index].source.startsWith("data:image/"));
    assert.equal(exportedImageState[index].alt, taskBefore.result.recommendations[index].title);
  }
  imageChecks.exported_html = assertImageFilesMatch(exportedImageState, taskBefore.result.recommendations);
  assert.deepEqual(requestCounts.exported_html.http_requests, [], "独立HTML发起了外部HTTP(S)请求");
  assert.deepEqual(requestCounts.exported_html.failed_requests, [], "独立HTML尝试了外部HTTP(S)请求");
  checks.push("界面下载的HTML在无cookie的独立context中打开，文字、内嵌图片和样式完整");
  checks.push("独立HTML未请求外部图片或脚本资源");
  await exportedContext.close();

  const afterResponse = await context.request.get("http://127.0.0.1:8000/api/harness/tasks/" + encodeURIComponent(completedTaskId));
  assert.ok(afterResponse.ok(), "操作后任务接口读取失败: " + afterResponse.status());
  taskAfter = await afterResponse.json();
  assert.deepEqual(requestSnapshot(taskAfter), requestSnapshot(taskBefore), "只读页面检查期间任务结果、调用次数或请求记录发生变化");
  checks.push("操作前后API任务结果、模型/工具次数及请求记录一致");

  assert.deepEqual(pageErrors, [], "浏览器页面发生pageerror");
  status = "passed";
} catch (error) {
  failure = { name: error.name, message: error.message, stack: error.stack };
} finally {
  await browser.close();
  const report = {
    status,
    run_id: runId,
    completed_task_id: completedTaskId,
    escaping_check_requested: verifyEscaping,
    checks,
    failure,
    browser: browserVersion,
    sources_sha256: sourceHashes,
    export_file: exportPath,
    task_api_before: taskBefore ? requestSnapshot(taskBefore) : null,
    task_api_after: taskAfter ? requestSnapshot(taskAfter) : null,
    request_counts: requestCounts,
    image_sha256_checks: imageChecks,
    layout: layoutResults,
    page_errors: pageErrors,
  };
  await fs.writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ report: path.join(directory, "report.json"), status, checks, failure }));
}

if (failure) {
  console.error(failure.stack);
  process.exitCode = 1;
}
