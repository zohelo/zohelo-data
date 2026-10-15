// zohelo-scheduler: external clock for the Pipeline scheduler (#802).
//
// GitHub's `schedule` trigger fires only a fraction of its slots, so this Cloudflare
// Worker's Cron Trigger dispatches the Pipeline run "scheduler: dispatch due runs"
// instead. The scheduler's catch-up window (ingestion/common/scheduler.py) makes a missed
// or repeated firing harmless. Deployed and paused with `ops/scheduler_trigger.py`.
//
// Secret GITHUB_TOKEN: a fine-grained token limited to this repository with
// "Actions: read and write" only. The owner sets it in the Cloudflare dashboard; it never
// passes through the repository or its workflows.

export default {
  // A thrown error marks the invocation as failed in the Worker's Cron events.
  async scheduled(controller, env) {
    const url = `https://api.github.com/repos/${env.GITHUB_REPOSITORY}/actions/workflows/${env.WORKFLOW}/dispatches`;
    const response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "zohelo-scheduler",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({ ref: env.REF, inputs: { source: env.SOURCE, run: env.RUN } }),
    });
    if (response.status !== 204) {
      throw new Error(`dispatch failed: HTTP ${response.status} ${(await response.text()).slice(0, 300)}`);
    }
  },
};
