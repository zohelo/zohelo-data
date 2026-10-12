# Portal app

The internal portal at [data.zohelo.com](https://data.zohelo.com/): an R2 file
explorer and a browser DuckDB SQL workspace (React, TypeScript, Vite). It reads
private R2 through the Worker in `../worker/` and uses Google sign-in for identity
only. Deployed by the **Deploy portal** workflow.

```bash
npm ci --ignore-scripts --no-audit --no-fund
npm run dev        # http://localhost:5173
npm run build
```

Copy `.env.example` to `.env` for local public settings; every value in a Vite build
is public, so never put secrets there.

Derived from [Duck-UI](https://github.com/caioricciuti/duck-ui) (MIT, see
[LICENSE.md](LICENSE.md)).
