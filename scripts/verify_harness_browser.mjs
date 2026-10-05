import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID, createHash } from "node:crypto";

const modulesPath = process.env.SHOP_BROWSER_NODE_MODULES;
assert.ok(modulesPath, "需要配置SHOP_BROWSER_NODE_MODULES");
process.env.PLAYWRIGHT_BROWSERS_PATH = path.resolve(".scratch/playwright");
const { chromium } = await import(pathToFileURL(path.join(modulesPath, "playwright", "index.mjs")));
const directory = path.join(".scratch/evidence", "har-browser-" + randomUUID());
await fs.mkdir(directory, { recursive: true });
process.env.TMPDIR = path.resolve(".scratch/browser-tmp");
await fs.mkdir(process.env.TMPDIR, { recursive: true });
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });
const page = await context.newPage();
const errors = [];
page.on("pageerror", error => errors.push(error.message));
const checks = [];
try {
  await page.goto("http://127.0.0.1:8000/harness/");
  await page.getByLabel("用户名", { exact: true }).fill("admin");
  const password = (await fs.readFile(".local/auth/admin.password", "utf8")).trim();
  await page.getByLabel("密码", { exact: true }).fill(password);
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.getByLabel("创建宣传需求", { exact: true }).waitFor();
  checks.push("实际浏览器登录与任务列表");
  const prompt = "浏览器取消验证 " + randomUUID();
  await page.getByLabel("创建宣传需求", { exact: true }).fill(prompt);
  await page.getByRole("button", { name: "创建任务", exact: true }).click();
  await page.getByRole("heading", { name: prompt, exact: true }).waitFor();
  await page.getByRole("button", { name: "取消任务", exact: true }).click();
  await page.getByRole("heading", { name: "任务已取消", exact: true }).waitFor();
  checks.push("实际浏览器创建与取消");
  for (const viewport of [{ width: 1440, height: 1000 }, { width: 390, height: 844 }]) {
    await page.setViewportSize(viewport);
    const layout = await page.evaluate(() => ({
      width: document.documentElement.clientWidth,
      scrollWidth: document.documentElement.scrollWidth,
      buttons: Array.from(document.querySelectorAll("button")).filter(element => {
        const rectangle = element.getBoundingClientRect();
        return rectangle.width > 0 && rectangle.height > 0;
      }).length,
    }));
    assert.ok(layout.scrollWidth <= layout.width + 1, JSON.stringify({ viewport, layout }));
    assert.ok(layout.buttons > 0);
    checks.push(`布局宽度${viewport.width}无横向溢出`);
  }
  const completedTaskId = process.argv[2];
  if (completedTaskId) {
    const task = await context.request.get(`/api/harness/tasks/${completedTaskId}`);
    assert.ok(task.ok());
    const body = await task.json();
    const preview = await context.request.get(`/api/harness/tasks/${completedTaskId}/preview`);
    assert.ok(preview.ok());
    const html = await preview.text();
    const independentPath = path.resolve(directory, "campaign.html");
    await fs.writeFile(independentPath, html);
    const independent = await context.newPage();
    await independent.goto(pathToFileURL(independentPath).href);
    const images = await independent.locator("img").evaluateAll(elements => elements.map(image => ({
      loaded: image.complete && image.naturalWidth > 0,
      embedded: image.src.startsWith("data:image/"),
    })));
    assert.equal(images.length, body.result.recommendations.length);
    assert.ok(images.every(image => image.loaded && image.embedded));
    assert.equal(await independent.locator("h1").textContent(), body.result.headline);
    checks.push("独立HTML包含图片与文案并可加载");
    await independent.close();
  }
  assert.deepEqual(errors, []);
  const source = await fs.readFile("frontend/src/App.tsx");
  await fs.writeFile(path.join(directory, "report.json"), JSON.stringify({
    status: "passed", checks, browser: browser.version(),
    source_sha256: createHash("sha256").update(source).digest("hex"),
    completed_task_id: completedTaskId ?? null, errors,
  }, null, 2));
  console.log(JSON.stringify({ report: path.join(directory, "report.json"), checks }));
} finally {
  await browser.close();
}
