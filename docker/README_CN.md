# Octop Docker 部署

---

本目录包含 Octop 的 Docker 构建与部署相关文件。

### 文件说明

| 文件 | 说明 |
|------|------|
| `Dockerfile` | 多阶段镜像定义（构建上下文为仓库根目录） |
| `../.dockerignore` | 构建上下文忽略规则（Podman 与 BuildKit 均适用） |
| `docker_build.sh` | 从源码构建镜像（默认开启 BuildKit 缓存） |
| `docker-compose.yml` | 本地开发 / 自托管一键启动 |
| `docker-compose.cn.yml` | 同一套服务，拉取 `ghcr.nju.edu.cn/tencentcloud/octop:latest`（不本地构建） |
| `docker-compose.postgres.yml` | 仅 PostgreSQL（+ pgvector）开发/测试库；扩展在 `postgres/init-vector.sql` |
| `postgres/init-vector.sql` | 实例级 `CREATE EXTENSION vector`（initdb.d；**不**进 Octop 迁移） |
| `docker-entrypoint.sh` | 容器入口：首次初始化数据库并启动服务 |

### 快速开始

**方式一：Compose（推荐）**

在仓库根目录执行：

```bash
docker compose -f docker/docker-compose.yml up -d --build
```

访问 `http://localhost:8088`。设置了合格的 `OCTOP_DEFAULT_PASSWORD`（≥8 位且同时包含字母和数字）时，首次启动会创建管理员，并把凭据写入 `/data/.octop/credential.txt`。未设置，或密码被策略拒绝（过短、缺字母/数字、常见弱密码）时，**不**自动建账号，打开页面走设置向导；向导口令在 `/data/.octop/octop-login.txt`（`credential.txt` 里也有说明）。在向导里自行设置管理员密码。

Compose 将多 agent 服务的文件描述符限制设为 65535。服务启动时也会在继承的 hard limit 内将 soft limit 提升至最多 65535，并记录实际限额。已有容器需重新创建才能应用 Compose 限额变化；仅重启不会改变容器的 hard limit。

**方式二：构建脚本**

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

### 国内镜像加速

构建时可通过环境变量加速依赖下载：

```bash
PIP_INDEX_URL=https://mirrors.cloud.tencent.com/pypi/simple \
PIP_TRUSTED_HOST=mirrors.cloud.tencent.com \
NPM_REGISTRY=https://mirrors.cloud.tencent.com/npm/ \
APT_MIRROR=mirrors.cloud.tencent.com \
bash docker/docker_build.sh
```

### 常用环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `HOME` | `/data` | 必须为 `/data`，数据目录映射到 `~/.octop` |
| `OCTOP_PORT` | `8088` | HTTP 服务端口 |
| `OCTOP_DEFAULT_PASSWORD` | _(未设置)_ | 首次管理员密码（≥8 位，字母+数字）。合格则 `octop init` 建账号；未设置或不合格则走 Web 设置向导 |
| `OCTOP_ADMIN_USERNAME` | `admin` | 首次管理员用户名 |
| `OCTOP_DATABASE_URL` | — | PostgreSQL DSN（或其他 `OCTOP_DATABASE_*`，见 [configuration.md](../docs/configuration.md)） |
| `OCTOP_DATABASE_DRIVER` | — | 通过环境变量覆盖时：`sqlite` \| `postgresql` |
| `OPENAI_API_KEY` | — | OpenAI 兼容 API Key |
| `DASHSCOPE_API_KEY` | — | 阿里云通义千问 API Key |

Compose 可在 `docker/.env` 中配置上述变量。注意：`.env` 只参与 Compose 插值，变量必须出现在 `docker-compose.yml` 的 `environment:` 中才会进入容器。也可把相同键写入挂载数据目录下的 `~/.octop/env`。

### 可选 MemoryLake CLI（预览）

MemoryLake 是外部既有服务，本部署不负责部署或运维该服务。Octop 保留原生记忆，
仅通过单独提供的 CLI 接入；标准镜像不会下载二进制，也不添加 sidecar。
功能**默认关闭**。HTTP/UI 接线仍在使用 fake runner 验证，不能视为生产可用；
真实 PostgreSQL 与多 worker 运行尚未验证。

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OCTOP_MEMORYLAKE_CLI_ENABLED` | `false` | 操作者显式启用；完成部署验证前保持关闭 |
| `OCTOP_MEMORYLAKE_CLI_EXECUTABLE` | 空 | 容器内固定的可执行文件绝对路径 |
| `OCTOP_MEMORYLAKE_CLI_EXPECTED_VERSION` | 空 | 精确发布版本，不含 `memorylake ` 前缀 |
| `OCTOP_MEMORYLAKE_CLI_SHA256` | 空 | 经核验二进制的完整 64 位十六进制 SHA-256 |

以上对应 Octop 配置中的 `memorylake_cli.enabled`、`.executable`、
`.expected_version` 和 `.sha256`。两个标准 Compose 文件均无默认值地透传这四个变量。
请在调用 Compose 的 shell 中显式 export；不要假设未导出的 `.env` 项必然进入容器。

操作者需自行获取并独立核验与容器平台匹配的可信发布产物。
**本预览尚未确认具体发布来源、版本或哈希**，不要使用随意下载的文件或编造摘要。
文件必须是普通 POSIX 可执行文件，采用无符号链接路径组件的绝对路径，不允许
组/其他用户写入或 setuid/setgid 位，并置于可信只读位置。父目录也应由可信操作者
控制；哈希校验不构成对本地特权修改者的隔离。

Compose 默认不挂载 CLI，避免未配置时影响原生启动。请在**自己的 override** 中
添加只读绑定，例如以下片段（不是完整部署配置）：

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

将 `OCTOP_MEMORYLAKE_CLI_EXECUTABLE=/opt/memorylake/memorylake` 设置为匹配的
容器路径，提供独立核验的版本与摘要后再显式启用。沿用标准 Compose 文件叠加自己的
override，不替换原生数据卷，也不添加 MemoryLake 服务。标准非 Docker 安装使用
相同四项配置，路径改为操作者管理的本地可执行文件。

激活时**先校验内容哈希，再执行**固定的 `--version` 命令，使用私有 HOME/config
目录；输出必须精确等于 `memorylake {expected_version}\n`。文件缺失或校验不通过时，
只禁用 MemoryLake 接入，不阻止原生 Octop 启动。这只验证本地产物，不代表远端连通性
或服务兼容性通过。连接凭据、项目权限、写入开关与逐文件显式批准仍各自必需；
部署配置本身不授权上传。

### 知识库依赖

镜像除 `browser` 外，还包含 `local-embedding` 和 `knowledge-ocr` 可选依赖。
两次依赖安装均指定这些 extras，避免最后一次 `uv sync` 将其移除。构建时会检查
嵌入与 OCR 模块能否导入；不会下载模型权重，也不会自动开启知识库或 OCR。

通过控制台安装到运行中容器的依赖位于容器可写层，重建容器后会丢失。应重新构建或
拉取已包含这些 extras 的镜像，而不是依赖运行时安装。保留 `/data/.octop` 数据卷，
即可保留配置、知识库索引与本地嵌入模型缓存。已有部署需要重新构建或升级镜像才能
获得这些依赖，仅重启旧镜像无效。

### 数据持久化

- Compose 默认将宿主机 `~/.octop` 挂载到容器 `/data/.octop`
- `docker run` 示例使用命名卷 `octop-data`
- 首次启动：`OCTOP_DEFAULT_PASSWORD` 合格则执行 `octop init` 并写入 `credential.txt`；未设置或不合格则不建管理员，向导口令写入 `/data/.octop/octop-login.txt`，打开页面完成设置（容器不会因弱密码退出）。

### 健康检查

镜像内置 `HEALTHCHECK`，探测 `GET /api/health`：

```bash
curl http://localhost:8088/api/health
```

### 常用运维命令

```bash
docker logs -f octop
docker exec -it octop octop --version
docker compose -f docker/docker-compose.yml down
docker compose -f docker/docker-compose.yml up -d --build
```
