import assert from "node:assert/strict";
import test from "node:test";
import { checkReferenceAndDispatch, referenceAuditComplete } from "../cloudflare/high-low-trigger-worker.js";

test("17:00 Taipei dispatches reference audit, not the original fast push", async () => {
  const original = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (url, init = {}) => {
    calls.push({ url, init });
    if (url.includes("raw.githubusercontent.com")) return new Response("", { status: 404 });
    if (url.endsWith("dispatches")) return new Response(null, { status: 204 });
    return Response.json({ workflow_runs: [] });
  };
  try {
    const response = await checkReferenceAndDispatch({ GITHUB_TOKEN: "test" }, new Date("2026-11-10T09:00:00Z"));
    assert.equal(response.status, 202);
    const dispatch = calls.find((call) => call.url.endsWith("dispatches"));
    assert.match(dispatch.url, /wantgoo-reconcile.yml/);
    const input = JSON.parse(dispatch.init.body).inputs;
    assert.equal(input.report_date, "2026/11/10");
    assert.equal(input.dry_run, "false");
  } finally { globalThis.fetch = original; }
});

test("weekends, before17:00 and overnight delayed events never dispatch", async () => {
  const original = globalThis.fetch;
  globalThis.fetch = () => { throw new Error("must not make requests"); };
  try {
    for (const date of ["2026-11-10T08:59:59Z", "2026-11-10T11:00:00Z", "2026-11-10T17:00:00Z", "2026-11-14T09:00:00Z"]) {
      const response = await checkReferenceAndDispatch({}, new Date(date));
      assert.equal(response.status, 200);
      assert.match(await response.text(), /Outside/);
    }
  } finally { globalThis.fetch = original; }
});

test("a completed daily audit stops retries without publishing again", async () => {
  const original = globalThis.fetch;
  const audit = { date: "2026/11/10", status: "matched", notification: { complete: true, items: { text: 42 } } };
  let calls = 0;
  globalThis.fetch = async () => { calls += 1; return Response.json(audit); };
  try {
    const response = await checkReferenceAndDispatch({}, new Date("2026-11-10T09:10:00Z"));
    assert.match(await response.text(), /Already reconciled/);
    assert.equal(calls, 1);
  } finally { globalThis.fetch = original; }
});

test("partial, wrong-date and forged delivery flags do not stop retries", () => {
  const base = { date: "2026/11/10", status: "corrected", notification: { complete: true, items: { text: 1 } } };
  assert.equal(referenceAuditComplete(base, "2026/11/10"), false);
  base.notification.items = { text: 1, high_low_cards: 2, high_low_summary: 3, weekly_quick_view: 4 };
  assert.equal(referenceAuditComplete(base, "2026/11/10"), true);
  assert.equal(referenceAuditComplete(base, "2026/11/09"), false);
  base.notification.items.weekly_quick_view = true;
  assert.equal(referenceAuditComplete(base, "2026/11/10"), false);
});
