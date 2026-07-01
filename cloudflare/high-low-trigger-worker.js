const OWNER = "j1984001-max";
const REPO = "taifex-report-dashboard";
const WORKFLOW_ID = "high-low-fast-push.yml";

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

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(dispatchHighLowWorkflow(env, `cloudflare-cron:${event.cron}`));
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname !== "/trigger") {
      return new Response("Not found", { status: 404 });
    }
    if (!env.TRIGGER_SECRET) {
      return new Response("Manual trigger is disabled", { status: 403 });
    }
    if (url.searchParams.get("secret") !== env.TRIGGER_SECRET) {
      return new Response("Forbidden", { status: 403 });
    }
    return dispatchHighLowWorkflow(env, "manual-http-trigger");
  },
};
