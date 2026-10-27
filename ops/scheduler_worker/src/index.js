// zohelo-scheduler: external clock for the Pipeline scheduler (#802).
//
// GitHub's `schedule` trigger fires only a fraction of its slots, so this Cloudflare
// Worker's Cron Trigger dispatches the Pipeline run "scheduler: dispatch due runs"
// instead. The scheduler's catch-up window (ingestion/common/scheduler.py) makes a missed
// or repeated firing harmless. Deployed and paused with `ops/scheduler_trigger.py`.
//
// Identity: the organisation's GitHub App (zohelo-automation). Secrets APP_ID and
// APP_PRIVATE_KEY are set on the Worker by `scheduler_trigger.py deploy` from the
// organisation variable ORG_BOT_APP_ID and secret ORG_BOT_PRIVATE_KEY. Each firing signs a
// short JWT, exchanges it for an installation token limited to this repository with
// "Actions: write", and dispatches. No personal token is involved.

const GITHUB = "https://api.github.com";
const HEADERS = {
  Accept: "application/vnd.github+json",
  "User-Agent": "zohelo-scheduler",
  "X-GitHub-Api-Version": "2022-11-28",
};

function base64url(bytes) {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function derLength(n) {
  if (n < 0x80) return [n];
  const out = [];
  while (n > 0) {
    out.unshift(n & 0xff);
    n >>= 8;
  }
  return [0x80 | out.length, ...out];
}

// GitHub App keys are PKCS#1 ("BEGIN RSA PRIVATE KEY"); WebCrypto imports PKCS#8, so wrap it.
function pkcs8(pem) {
  const body = Uint8Array.from(atob(pem.replace(/-----[^-]+-----/g, "").replace(/\s+/g, "")), (c) => c.charCodeAt(0));
  if (pem.includes("BEGIN PRIVATE KEY")) return body;
  const algorithm = [0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00];
  const octet = [0x04, ...derLength(body.length), ...body];
  const inner = [0x02, 0x01, 0x00, ...algorithm, ...octet];
  return Uint8Array.from([0x30, ...derLength(inner.length), ...inner]);
}

async function appJwt(env) {
  const key = await crypto.subtle.importKey(
    "pkcs8",
    pkcs8(env.APP_PRIVATE_KEY),
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const now = Math.floor(Date.now() / 1000);
  const enc = (o) => base64url(new TextEncoder().encode(JSON.stringify(o)));
  const unsigned = `${enc({ alg: "RS256", typ: "JWT" })}.${enc({ iat: now - 60, exp: now + 540, iss: String(env.APP_ID) })}`;
  const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(unsigned));
  return `${unsigned}.${base64url(new Uint8Array(signature))}`;
}

async function github(path, token, init = {}) {
  const response = await fetch(GITHUB + path, {
    ...init,
    headers: { ...HEADERS, Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  });
  if (!response.ok) throw new Error(`${init.method || "GET"} ${path}: HTTP ${response.status} ${(await response.text()).slice(0, 300)}`);
  return response.status === 204 ? null : response.json();
}

async function installationToken(env) {
  const jwt = await appJwt(env);
  const [, repo] = env.GITHUB_REPOSITORY.split("/");
  const installation = await github(`/repos/${env.GITHUB_REPOSITORY}/installation`, jwt);
  const token = await github(`/app/installations/${installation.id}/access_tokens`, jwt, {
    method: "POST",
    body: JSON.stringify({ repositories: [repo], permissions: { actions: "write" } }),
  });
  return token.token;
}

export default {
  // A thrown error marks the invocation as failed in the Worker's Cron events.
  async scheduled(controller, env) {
    const token = await installationToken(env);
    await github(`/repos/${env.GITHUB_REPOSITORY}/actions/workflows/${env.WORKFLOW}/dispatches`, token, {
      method: "POST",
      body: JSON.stringify({ ref: env.REF, inputs: { source: env.SOURCE, run: env.RUN } }),
    });
  },
};
