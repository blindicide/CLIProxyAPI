// Runs the dashboard's real inline <script> in a VM with a minimal DOM stub and checks the
// pure logic: escaping (the XSS guard for client-controlled strings), formatters, DST-safe
// bucket filling, stale quota rendering. Invoked by tests/test_dashboard_js.py.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const html = readFileSync(new URL("../../dashboard.html", import.meta.url), "utf8");
const script = html.split("<script>")[1].split("</script>")[0];

const elements = new Map();
function element(id) {
  if (!elements.has(id)) {
    elements.set(id, {
      id, innerHTML: "", textContent: "", href: "", className: "", clientWidth: 400, dataset: {},
      attributes: {}, setAttribute(k, v) { this.attributes[k] = String(v); }, getAttribute(k) { return this.attributes[k]; },
      addEventListener() {}, classList: { toggle() {}, add() {} },
    });
  }
  return elements.get(id);
}
// Captions as declared in the HTML so table() output matches production.
for (const [, id, caption] of html.matchAll(/<table id="([^"]+)" data-caption="([^"]+)"/g)) element(id).dataset.caption = caption;

const context = {
  document: { getElementById: element, querySelectorAll: () => [], createElement: () => element("tmp-" + Math.random()), title: "" },
  window: { addEventListener() {} },
  localStorage: { getItem: () => null, setItem() {} },
  fetch: () => new Promise(() => {}),
  setInterval: () => 0, clearInterval() {}, setTimeout: () => 0, clearTimeout() {},
  Intl, Date, Math, JSON, Number, String, Object, Array, Map, Set, RegExp, Promise, console,
  encodeURIComponent,
};
context.window.document = context.document;
vm.createContext(context);
vm.runInContext(script, context);
const run = (code) => vm.runInContext(code, context);

let passed = 0;
function test(name, fn) { fn(); passed++; }

test("esc neutralises markup and attribute breakouts", () => {
  assert.equal(run(`esc('<img src=x onerror="alert(1)">')`), "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;");
  assert.equal(run(`esc("' & \\"")`), "&#39; &amp; &quot;");
  assert.equal(run("esc(null)"), "");
  assert.equal(run("esc(0)"), "0");
});

test("hostile client strings stay inert in the recent-requests table", () => {
  const evil = `<script>alert(1)</script>`;
  run(`renderRecent([{timestamp:"2026-09-27T12:00:00Z",model:${JSON.stringify(evil)},endpoint_path:"/v1/x",stream:false,failed:true,status_code:${JSON.stringify(evil)},fail_body:${JSON.stringify('"><img onerror=1>')},usage:{input_total:1,output:2},cost_usd:null,cost_quality:"complete",latency_ms:5,ttft_ms:0,api_key_name:${JSON.stringify(evil)},api_key_masked:"sha256:x"}])`);
  const out = element("t-recent").innerHTML;
  assert.ok(!out.includes("<script>") && !out.includes("<img"), out);
  assert.ok(out.includes("&lt;script&gt;alert(1)&lt;/script&gt;"));
  assert.ok(out.includes('title="&quot;&gt;&lt;img onerror=1&gt;"'));
  assert.ok(out.includes('<caption class="sr-only">Last 100 requests, newest first</caption>'));
  assert.ok(out.includes('<th scope="col"'));
});

test("formatters never invent numbers", () => {
  assert.equal(run("usd(null)"), "—");
  assert.equal(run("usd(0)"), "$0.00");
  assert.equal(run("usd(0.000346)"), "$0.00035");
  assert.equal(run("usd(0.1789355)"), "$0.1789");
  assert.equal(run("usd(1234.5)"), "$1,234.50");
  assert.equal(run("k(null)"), "—");
  assert.equal(run("k(165589)"), "165.6k");
  assert.equal(run("k(2500000)"), "2.50M");
  assert.equal(run("ms(null)"), "—");
  assert.equal(run("ms(2964.4)"), "2964 ms");
  assert.equal(run("ms(11818)"), "11.8 s");
  assert.equal(run("pct(null)"), "—");
  assert.equal(run("age(null)"), "never");
  assert.equal(run("age(75)"), "75 s ago");
});

test("hour buckets fill gaps and keep the repeated DST hour", () => {
  const keys = JSON.parse(run(`JSON.stringify(buckets({series_granularity:"hour",generated_at:"2026-10-25T03:00:00Z",window_start:"2026-10-24T23:00:00Z",series:[{bucket:"2026-10-25T02:00+02:00",requests:1,failed:0},{bucket:"2026-10-25T02:00+01:00",requests:2,failed:0}]}).map(b=>[b.bucket,b.requests]))`));
  assert.deepEqual(keys, [
    ["2026-10-25T01:00+02:00", 0], ["2026-10-25T02:00+02:00", 1], ["2026-10-25T02:00+01:00", 2],
    ["2026-10-25T03:00+01:00", 0], ["2026-10-25T04:00+01:00", 0],
  ]);
});

test("day buckets fill a 7d window", () => {
  const keys = JSON.parse(run(`JSON.stringify(buckets({series_granularity:"day",generated_at:"2026-09-27T13:00:00Z",window_start:"2026-09-20T13:00:00Z",series:[{bucket:"2026-09-27",requests:3,failed:0}]}).map(b=>b.bucket+"="+b.requests))`));
  assert.equal(keys.length, 8);
  assert.equal(keys[0], "2026-09-20=0");
  assert.equal(keys.at(-1), "2026-09-27=3");
});

test("a quota window that reset since observation is shown as unknown", () => {
  const stale = run(`qbar("5h",{used_pct:13,status:"allowed",reset_passed:true,reset:{utc:"2026-09-27T17:50:00Z"}})`);
  assert.ok(stale.includes("unknown") && stale.includes("last observed 13%") && !stale.includes("13% used"));
  const live = run(`qbar("7d",{used_pct:74,status:"allowed",reset_passed:false,reset:{utc:"2026-09-27T19:00:00Z"}})`);
  assert.ok(live.includes("74% used") && live.includes('aria-valuenow="74"') && live.includes("qfill warn"));
});

test("archived rows and coverage are labelled, never silently mixed", () => {
  assert.equal(run(`archTag({requests:3,archived_requests:0})`), "");
  assert.ok(run(`archTag({requests:3,archived_requests:3})`).includes(">archive<"));
  assert.ok(run(`archTag({requests:5,archived_requests:2})`).includes("+2 archived"));
  assert.ok(run(`coverageText({coverage:{includes_archives:false,archive_after_days:180}})`).startsWith("live history"));
  assert.ok(run(`coverageText({coverage:{includes_archives:true,archived_records:0,archive_after_days:180}})`).includes("nothing archived yet"));
  const text = run(`coverageText({coverage:{includes_archives:true,live_records:28,archived_records:40,archived_from:"2026-01-02T10:00:00Z",archived_until:"2026-03-01T10:00:00Z"}})`);
  assert.ok(text.includes("28 live + 40 archived") && text.includes("02/01/2026"), text);
});

test("charts announce a data summary", () => {
  run(`bars("ch-req",[{bucket:"2026-09-27",requests:5,failed:1},{bucket:"2026-09-28",requests:2,failed:0}],[{name:"ok",c:"x",v:d=>d.requests-d.failed},{name:"failed",c:"y",v:d=>d.failed}],v=>String(Math.round(v)),b=>b,"Requests")`);
  assert.equal(element("ch-req").attributes["aria-label"], "Requests: ok 6, failed 1 over 2 buckets; peak 5 at 2026-09-27");
});

// The whole refresh path with a realistic API response: catches ordering bugs (e.g. a const
// used before its declaration) that only show when refresh() runs end to end.
async function refreshScenario(ingestExtra, expectBanner) {
  const analytics = {
    window: "24h", version: "0.2.0", generated_at: "2026-09-27T13:00:00Z", window_start: "2026-09-26T13:00:00Z",
    series_granularity: "hour", series: [{bucket: "2026-09-27T15:00+02:00", requests: 2, failed: 1, input_tokens: 10, output_tokens: 5, cost_usd: 0.001}],
    summary: {requests: 2, success: 1, failed: 1, success_pct: 50, failed_pct: 50, input_tokens: 10, output_tokens: 5, cache_read_tokens: 0, cache_write_tokens: 0, reasoning_tokens: 0,
      estimated_cost_usd: 0.001, unpriced_requests: 0, unpriced_models: {}, avg_latency_ms: 10, p50_latency_ms: 10, p95_latency_ms: 10, avg_ttft_ms: 5, ttft_samples: 1,
      stream_requests: 1, non_stream_requests: 1, requests_per_min: 0.001},
    per_model: [], per_day: [], per_endpoint: [], per_key: [], per_client_ip: [], per_user_agent: [],
    management: {reachable: true},
    ingest: {last_ingest_age_s: 3, last_ingest_at: "2026-09-27T13:00:00Z", pending_writes: 0, corrupt_lines: 0, loss_windows: [], loss_windows_total: 0,
      queue_retention_s: 60, queue_retention_source: "cproxy config",
      storage: {requests_file_bytes: 54607, backups: 1, newest_backup_at: "2026-09-27T12:00:00Z", warnings: []}, ...ingestExtra},
  };
  const responses = {"/api/analytics": analytics, "/api/requests": {requests: []}, "/api/quota": {available: true, credentials: [], client_keys: []}};
  context.fetch = async (url) => ({ok: true, status: 200, json: async () => responses[Object.keys(responses).find((p) => url.startsWith(p))]});
  await run("refresh()");
  const banner = element("banner").textContent;
  assert.ok(!banner.includes("unreachable"), banner);
  assert.ok(!banner.includes("before initialization"), banner);
  if (expectBanner) assert.ok(banner.includes(expectBanner), banner);
  assert.ok(element("f-storage").textContent.includes("1 backup(s)"), element("f-storage").textContent);
  assert.equal(element("live-text").textContent, "live · ingest ok");
}
await refreshScenario({}, null);
await refreshScenario({storage: {requests_file_bytes: 1, backups: 1, newest_backup_at: "2026-09-20T00:00:00Z", warnings: ["newest backup is 180 h old"]}}, "Storage: newest backup is 180 h old");
passed++;

console.log(`dashboard js: ${passed} passed`);
