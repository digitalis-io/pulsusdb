// Issue #832: loads each page of the profileql_frontend scenario in a new
// browser context and reports what it saw, as one JSON document on stdout.
// It judges nothing: e2e/src/profileql.rs does.
//
// Usage: node pages.mjs <front end base URL> <specs JSON>
import { chromium } from 'playwright-core';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const [base, specsJson] = process.argv.slice(2);
const specs = JSON.parse(specsJson);
const BODY_LIMIT = 262144;
const EXPECT_WAIT_MS = 30000;
const QUIET_MS = 1500;
const QUIET_WAIT_MS = 10000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const until = async (pred, ms) => {
  const end = Date.now() + ms;
  while (!pred() && Date.now() < end) await sleep(200);
  return pred();
};
const recorded = (records, e) =>
  records.some((r) => r.path.endsWith(e.path) && (e.req || []).every((s) => r.request.includes(s)));
const scoped = (page, scope) => (scope ? page.getByTestId(scope).last() : page.locator('body'));
const shown = async (page, t) => ({
  scope: t.scope,
  text: t.text,
  shown: await scoped(page, t.scope).getByText(t.text).first().isVisible().catch(() => false),
});

async function load(browser, spec) {
  const context = await browser.newContext({ viewport: { width: 1600, height: 1000 } });
  const page = await context.newPage();
  const records = [];
  const pending = [];
  let last = Date.now();
  page.on('response', (res) => {
    const url = new URL(res.url());
    if (!url.pathname.startsWith('/api/')) return;
    last = Date.now();
    pending.push((async () => {
      let body = '';
      try {
        body = (await res.body()).subarray(0, BODY_LIMIT).toString('utf8');
      } catch (e) {}
      records.push({
        status: res.status(),
        method: res.request().method(),
        path: url.pathname,
        request: res.request().postData() || '',
        body,
      });
    })());
  });

  const expect = [...(spec.expect || [])];
  const absent = spec.absent || { requests: [], texts: [] };
  const report = { name: spec.name, complete: false, records, absentFound: [], visible: [], invisible: [] };
  try {
    await page.goto(base + spec.url, { waitUntil: 'load', timeout: 60000 });
    if (spec.upload) {
      const file = join(mkdtempSync(join(tmpdir(), 'profileql-')), spec.upload.file);
      writeFileSync(file, Buffer.from(spec.upload.base64, 'base64'));
      await page.locator('input[type=file]').first().setInputFiles(file);
    }
    if (spec.save) {
      await until(() => expect.every((e) => recorded(records, e)), EXPECT_WAIT_MS);
      await page.getByTestId('flamegraph-settings').getByRole('spinbutton').fill(String(spec.save.maxNodes));
      await page.getByRole('button', { name: /save settings/i }).click();
      expect.push({ path: '/settings.v1.SettingsService/Set', req: [] });
    }
    if (spec.row) {
      const rows = page.getByTestId('span-view');
      await rows.nth(spec.row.rows - 1).waitFor({ timeout: EXPECT_WAIT_MS });
      await rows.nth(spec.row.index).click({ position: { x: 20, y: 5 } });
      if (spec.row.link) {
        const name = spec.row.link;
        await page.getByRole('link', { name }).or(page.getByRole('button', { name })).first().click();
      }
    }
    // With `absent`, the whole wait runs: the page is watched for 30 s.
    const all = () => expect.every((e) => recorded(records, e));
    if (absent.requests.length > 0 || absent.texts.length > 0) await sleep(EXPECT_WAIT_MS);
    else await until(all, EXPECT_WAIT_MS);
    report.complete = all();
    await until(() => Date.now() - last > QUIET_MS, QUIET_WAIT_MS);
    await Promise.all(pending);
    if (spec.hide) await page.addStyleTag({ content: `${spec.hide} {display:none !important}` });
    for (const t of spec.visible || []) report.visible.push(await shown(page, t));
    for (const t of spec.invisible || []) report.invisible.push(await shown(page, t));
    for (const text of absent.texts) {
      if ((await page.getByText(text).filter({ visible: true }).count()) > 0) report.absentFound.push(text);
    }
  } catch (e) {
    report.complete = false;
    report.error = String(e);
    await Promise.all(pending);
  }
  await context.close();
  return report;
}

const browser = await chromium.launch();
const out = [];
for (const spec of specs) out.push(await load(browser, spec));
await browser.close();
process.stdout.write(JSON.stringify(out));
