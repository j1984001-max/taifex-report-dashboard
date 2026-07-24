const OWNER = "j1984001-max";
const REPO = "taifex-report-dashboard";
const WORKFLOW_ID = "high-low-fast-push.yml";
const HOLIDAYS = new Set([
  "2026/01/01", "2026/02/16", "2026/02/17", "2026/02/18", "2026/02/19",
  "2026/02/20", "2026/02/27", "2026/04/03", "2026/04/06", "2026/05/01",
  "2026/06/19", "2026/09/25", "2026/10/09",
]);

function taipeiBusinessDate(now = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Taipei",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  let date = new Date(formatter.format(now) + "T00:00:00Z");
  while (date.getUTCDay() === 0 || date.getUTCDay() === 6 || HOLIDAYS.has(formatDate(date))) {
    date = new Date(date.getTime() - 24 * 60 * 60 * 1000);
  }
  return formatDate(date);
}

function formatDate(date) {
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, "0"),
    String(date.getUTCDate()).padStart(2, "0"),
  ].join("/");
}

async function fetchJson(url, init = {}) {
  const response = await fetch(url, {
    ...init,
    headers: {
      "Accept": "application/vnd.github+json",
      "Cache-Control": "no-cache",
      "Pragma": "no-cache",
      "User-Agent": "taifex-high-low-trigger",
      ...(init.headers || {}),
    },
  });
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Fetch failed: ${response.status} ${await response.text()}`);
  }
  return response.json();
}

async function deliveryIsComplete(expectedDate) {
  const slug = expectedDate.replaceAll("/", "-");
  const delivery = await fetchJson(
    `https://raw.githubusercontent.com/${OWNER}/${REPO}/main/snapshots/${slug}.delivery.json`,
  );
  return Boolean(delivery?.date === expectedDate && delivery?.highLowTelegram === true);
}

async function fastPushIsRunning(env) {
  if (!env.GITHUB_TOKEN) {
    return false;
  }
  const runs = await fetchJson(
    `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW_ID}/runs?branch=main&per_page=10`,
    {
      headers: {
        "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
        "X-GitHub-Api-Version": "2022-11-28",
      },
    },
  );
  return Boolean((runs?.workflow_runs || []).some((run) => (
    ["queued", "in_progress", "waiting", "requested", "pending"].includes(run.status)
  )));
}

async function dispatchHighLowWorkflow(env, reason) {
  if (!env.GITHUB_TOKEN) {
    return new Response("Missing GITHUB_TOKEN secret", { status: 500 });
  }

  const response = await fetch(
    `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW_ID}/dispatches`,
    {
      method: "POST",
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
        "Content-Type": "application/json",
        "User-Agent": "taifex-high-low-trigger",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({
        ref: "main",
        inputs: {
          trigger_reason: reason,
        },
      }),
    },
  );

  if (!response.ok) {
    const body = await response.text();
    return new Response(`GitHub dispatch failed: ${response.status} ${body}`, {
      status: 502,
    });
  }

  return new Response(`Dispatched ${WORKFLOW_ID}: ${reason}`, { status: 202 });
}

async function checkAndDispatch(env, reason) {
  const expectedDate = taipeiBusinessDate();
  if (await deliveryIsComplete(expectedDate)) {
    return new Response(`Already delivered: ${expectedDate}`, { status: 200 });
  }
  if (await fastPushIsRunning(env)) {
    return new Response(`Fast push already running: ${expectedDate}`, { status: 200 });
  }
  return dispatchHighLowWorkflow(env, `${reason}:${expectedDate}`);
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(checkAndDispatch(env, `cloudflare-cron:${event.cron}`));
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/status") {
      const expectedDate = taipeiBusinessDate();
      const delivered = await deliveryIsComplete(expectedDate);
      const running = await fastPushIsRunning(env);
      return Response.json({ expectedDate, delivered, running });
    }
    if (url.pathname !== "/trigger") {
      return new Response("Not found", { status: 404 });
    }
    if (!env.TRIGGER_SECRET) {
      return new Response("Manual trigger is disabled", { status: 403 });
    }
    if (url.searchParams.get("secret") !== env.TRIGGER_SECRET) {
      return new Response("Forbidden", { status: 403 });
    }
    return checkAndDispatch(env, "manual-http-trigger");
  },
};
