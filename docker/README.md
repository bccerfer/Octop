# Octop Docker Deployment

---

This directory contains the Docker build and deployment assets for Octop.

### Files

| File | Description |
|------|-------------|
| `Dockerfile` | Multi-stage image definition (build context is the repo root) |
| `../.dockerignore` | Build context ignore rules (applies to both Podman and BuildKit) |
| `docker_build.sh` | Build image from source (BuildKit cache enabled by default) |
| `docker-compose.yml` | One-command local / self-hosted deployment |
| `docker-compose.cn.yml` | Same stack, pulls `ghcr.nju.edu.cn/tencentcloud/octop:latest` (no local build) |
| `docker-compose.postgres.yml` | PostgreSQL (+ pgvector) for dual-backend dev/tests |
| `postgres/init-vector.sql` | Instance-level `CREATE EXTENSION vector` (initdb.d; **not** Octop migrations) |
| `docker-entrypoint.sh` | Container entrypoint: first-run init + start server |

### Quick start

**Option 1: Compose (recommended)**

From the repository root:

```bash
docker compose -f docker/docker-compose.yml up -d --build
```

Open `http://localhost:8088`. A valid `OCTOP_DEFAULT_PASSWORD` (≥8 characters with letters and digits) creates the admin on first boot and writes credentials to `/data/.octop/credential.txt`. If the variable is unset, or the password fails policy (too short, missing letters/digits, or a common password), **no** admin is created — open the dashboard and complete the setup wizard. The wizard unlock password is in `/data/.octop/octop-login.txt` (also noted in `credential.txt`).

Compose sets the open-file limit to 65535 for multi-agent workloads. Server startup also raises the soft limit up to 65535 within the inherited hard limit and logs the effective limits. Recreate existing containers to apply changed Compose limits; a restart alone does not change their hard limit.

**Option 2: Build script**

```bash
bash docker/docker_build.sh
docker run -d \
  --name octop \
  --ulimit nofile=65535:65535 \
  -p 8088:8088 \
  -v octop-data:/data/.octop \
  -e HOME=/data \
  octop:latest
```

### Faster downloads (China mirrors)

Pass mirror env vars when building:

```bash
PIP_INDEX_URL=https://mirrors.cloud.tencent.com/pypi/simple \
PIP_TRUSTED_HOST=mirrors.cloud.tencent.com \
NPM_REGISTRY=https://mirrors.cloud.tencent.com/npm/ \
APT_MIRROR=mirrors.cloud.tencent.com \
bash docker/docker_build.sh
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HOME` | `/data` | Must be `/data` so `~/.octop` maps to the data volume |
| `OCTOP_PORT` | `8088` | HTTP listen port |
| `OCTOP_DEFAULT_PASSWORD` | _(unset)_ | First-run admin password (≥8 chars, letters + digits). Valid → `octop init`; unset or rejected → web setup wizard |
| `OCTOP_ADMIN_USERNAME` | `admin` | Initial admin username |
| `OCTOP_DATABASE_URL` | — | PostgreSQL DSN (or other `OCTOP_DATABASE_*`; see [configuration.md](../docs/configuration.md)) |
| `OCTOP_DATABASE_DRIVER` | — | `sqlite` \| `postgresql` when overriding defaults via env |
| `OPENAI_API_KEY` | — | OpenAI-compatible API key |
| `DASHSCOPE_API_KEY` | — | Alibaba DashScope API key |

For Compose, put these in `docker/.env`. Values only reach the container if listed under `environment:` in `docker-compose.yml` (Compose interpolates `.env`; it does not auto-export every key). Alternatively write the same keys into the mounted data dir as `~/.octop/env`.

### Optional MemoryLake CLI (preview)

MemoryLake remains an external service: this stack does not deploy or operate it.
Octop retains its native memory. The optional integration uses a separately
provided CLI; the standard image does not download a binary or add a sidecar.
It is **disabled by default**. Current HTTP/UI integration is under fake-runner
validation, not a production-readiness claim; live PostgreSQL and multi-worker
operation have not been validated.

| Variable | Default | Description |
|----------|---------|-------------|
| `OCTOP_MEMORYLAKE_CLI_ENABLED` | `false` | Explicit operator opt-in; leave disabled until deployment validation is complete |
| `OCTOP_MEMORYLAKE_CLI_EXECUTABLE` | empty | Fixed absolute executable path inside the container |
| `OCTOP_MEMORYLAKE_CLI_EXPECTED_VERSION` | empty | Exact release version, without the `memorylake ` prefix |
| `OCTOP_MEMORYLAKE_CLI_SHA256` | empty | Exact 64-hex SHA-256 of the approved binary |

These map to `memorylake_cli.enabled`, `.executable`, `.expected_version`, and
`.sha256` in Octop configuration. Both standard Compose variants pass the four
variables through without supplying defaults. Export them in the shell invoking
Compose (do not assume an unexported `.env` entry reaches the container).

The operator must obtain and independently verify a trusted release artifact for
the container's platform. **No release source, version, or hash is pinned by this
preview.** Do not substitute an arbitrary download or guessed digest. The file
must be a regular POSIX executable, use an absolute path with no symlink
components, have no group/other write permission or setuid/setgid bits, and live
at a trusted read-only location. Keep its parent directories under trusted
operator control; a digest is not a sandbox against privileged/local mutation.

Compose deliberately has no default CLI mount. Add a read-only bind mount in
**your own override**, for example this fragment (not a complete deployment):

```yaml
services:
  octop:
    volumes:
      - type: bind
        source: ${MEMORYLAKE_CLI_HOST_FILE:?set an independently verified absolute host file path}
        target: /opt/memorylake/memorylake
        read_only: true
        bind:
          create_host_path: false
```

Set `OCTOP_MEMORYLAKE_CLI_EXECUTABLE=/opt/memorylake/memorylake` to match that
container path, and provide the independently verified version/hash before
opting in. Use the existing standard Compose file plus your override; do not
replace the native data volume or add a MemoryLake service. Standard non-Docker
installs use the same four settings with an operator-managed local executable.

Activation checks content hash **before executing** the fixed `--version`
command, using private HOME/config directories. Its output must exactly match
`memorylake {expected_version}\n`. Missing or rejected artifacts disable only the
MemoryLake integration; native Octop startup remains available. This checks a
local artifact, not remote connectivity or service compatibility. Connection
credentials, project authorization, write opt-in and explicit per-file approval
remain separate requirements; deployment settings do not authorize an upload.

### Knowledge-base dependencies

The image includes the `local-embedding` and `knowledge-ocr` extras alongside
`browser`. Both dependency installation steps use these extras, so the final
`uv sync` retains them. The build checks that the embedding and OCR modules can
be imported; it does not download model weights or enable either feature.

Dependencies installed from the dashboard into a running container live in its
writable layer and disappear when that container is recreated. Rebuild or pull
an image containing these extras instead of relying on runtime installation.
Keep the `/data/.octop` volume to preserve settings, knowledge indexes and the
local embedding model cache. Existing deployments must rebuild or upgrade their
image to receive these dependencies; restarting an older image is insufficient.

### Data persistence

- Compose mounts host `~/.octop` → container `/data/.octop`
- `docker run` example uses named volume `octop-data`
- First boot: a valid `OCTOP_DEFAULT_PASSWORD` runs `octop init` and writes `credential.txt`; unset or rejected passwords skip admin creation and write the wizard unlock password to `/data/.octop/octop-login.txt`. The container does not exit because of a weak default.

### Health check

The image probes `GET /api/health`:

```bash
curl http://localhost:8088/api/health
```

### Operations

```bash
docker logs -f octop
docker exec -it octop octop --version
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up -d --build
```
