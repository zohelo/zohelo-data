// /api/* on the portal's origin: the R2 read API (portal/worker/src/index.js) as a Pages Function.
// Bindings and variables (LANDING, LAKEHOUSE, DOWNLOAD_SIGNING_KEY, PORTAL_SIGNIN_ALLOWED_EMAILS)
// are set on the Pages project by portal/cloudflare_setup.py.
import api from "../../../worker/src/index.js";

export const onRequest = (context) => api.fetch(context.request, context.env);
