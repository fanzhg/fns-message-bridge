# FNS Message Bridge

通过 Telegram、钉钉、飞书机器人收集文本和图片，写入 Fast Note Sync，再同步到 Obsidian。
已针对 Fast Note Sync 服务端 3.6.1 验证接口，适合支持 Docker Compose v2 的 Linux 服务器。
三个渠道在**一个容器、一个 Python 进程**里运行。支持纯文本、图片及带图片的配文。
保留消息队列、去重、失败重试和保存成功确认。

## 内存配置

默认容器内存总上限 **128 MiB**，三个渠道共用此额度；不额外使用 swap。
CPU 上限 0.25 核，日志最多 2 × 5 MB。队列在磁盘，配置只读挂载。
飞书仅加载长连接与回复所需模块，避免完整 SDK 的大量 API 模型。

本地 Linux Docker 模拟三平台图片冒烟测试：三个渠道完成接收图片、上传附件、写入 MD 和回复后，进程 RSS 约 **51 MiB**。
这是短时模拟测试，不是实际平台连接或长期负载下的保证。
先用默认内存限制，并通过 `docker stats` 观察实际占用，确认宿主机仍有余量。
Docker 引擎自身也占内存，容器限制不包括它。若服务器尚未安装 Docker，需要把引擎开销算进去。

## 1. 获取文件并配置

前提：服务器已有 Docker Engine 和 Docker Compose v2，`docker compose version` 能正常执行。
从 GitHub Release 下载部署包，上传服务器并解压后，进入其中的 `fns-message-bridge` 目录。
也可以直接使用仓库源码。低内存服务器建议从同一 Release 下载预构建 amd64 镜像。
后续命令都在该目录执行；没有 Docker 用户权限时给命令加 `sudo`。

```bash
cp config.example.toml config.toml
nano config.toml
```

配置中的 FNS 地址 `https://fns.example.com` 和仓库 `your-vault` 都是占位示例，必须替换成自己的值。
队列目录 `/data` 对应容器中的持久化数据卷。
在 `[fns]` 填独立 API Token；在各渠道填凭证并设 `enabled = true`。
尚未准备好的渠道保留 `enabled = false`，只加载已经开启的渠道。
至少启用一个渠道才能启动。

FNS Token 在管理后台手动签发，权限维度：

- 协议：`rest`
- 客户端类型和客户端维度：`fns-message-bridge`
- 功能：`note_r,note_w,file_r,file_w`，分别用于笔记及附件的读写
- 限制仓库：选择要保存笔记的仓库

对应表达式 `p:rest c:fns-message-bridge f:note_r,note_w,file_r,file_w`。
不要复用网页登录 Token。有效期到期后替换 Token，再重启容器。

保护配置文件，并让容器用户 UID/GID 10001 能读取：

```bash
sudo chown "$(id -u):10001" config.toml
chmod 640 config.toml
```

## 2. 构建与启动

如果服务器是 x86_64 / amd64，可以使用另附的预构建镜像，避免在低内存服务器上安装构建依赖。将 `fns-message-bridge-linux-amd64.tar.gz` 上传到服务器后：

```bash
docker load -i fns-message-bridge-linux-amd64.tar.gz
docker compose run --rm --no-deps bridge --check
docker compose up -d --no-build
```

其他架构或自行构建时：

```bash
docker compose build
docker compose run --rm --no-deps bridge --check
docker compose up -d
docker compose logs --tail=50 -f bridge
```

`--check` 检查 FNS 笔记读访问；图片功能开启时也检查附件读访问，不创建文件，也不测试写权限。
主进程仅启用配置中开启的渠道。三个渠道始终共享一个容器。
无需开放入站端口，FNS 使用现有 HTTPS 地址。
不要将 FNS URL 配置为容器内的 `127.0.0.1`；该地址指向桥接容器自身。

给机器人发 `/whoami` 或 `身份`，得到 ID 后填写对应 `allowed_users`，然后：

```bash
docker compose restart bridge
```

机器人创建及后台配置见 [机器人配置说明](docs/bot-setup.md)。
笔记目录为 `Inbox/telegram`、`Inbox/dingtalk`、`Inbox/feishu`，按月份归档，每条消息一篇。
文件名使用消息发送时的日期和时间（默认北京时间），例如 `2026-10-04_19-51-06.md`。
同一渠道同一秒的多条消息依次加 `-2`、`-3`，重试和重启沿用已经分配的文件名。
消息 ID 仅用于内部去重和笔记元数据。已有笔记和升级前排队的消息保留原文件名。
只有 FNS 确认保存成功，才回复“已保存”。

## 图片保存

- TG：普通照片及以图片文件形式发送的原图，保留 caption 配文。相册每张图对应独立消息，因此每张图生成一篇笔记。
- 钉钉：`picture` 图片消息，以及 `richText` 中的文字和图片。
- 飞书：`image` 图片消息，以及 `post` 富文本中的标题、文字和图片；图片追加在笔记正文下方。
- 支持 JPEG、PNG、GIF、WebP、BMP。保留平台提供的图片字节，不做 OCR、压缩或转码；普通 TG 照片可能已被平台压缩，原图请以图片文件发送。

示例：

```text
Inbox/feishu/2026-10/2026-10-08_14-30-25.md
Inbox/feishu/2026-10/assets/2026-10-08_14-30-25/2026-10-08_14-30-25-01.png
```

MD 内容会包含：

```markdown
图片配文

![](assets/2026-10-08_14-30-25/2026-10-08_14-30-25-01.png)
```

在 Obsidian 中显示图片需要 FNS 客户端启用附件同步，并允许对应图片格式。图片保存为仓库附件，不保留平台临时链接。

旧配置无需新增字段就默认开启图片，默认单张 5 MiB、单条消息最多 10 张。可在 `config.toml` 增加：

```toml
[images]
enabled = true
max_size_mb = 5
max_per_message = 10
```

下载和 multipart 上传均使用流式读写，每个渠道一次下载一张图片。多图消息会先逐张下载并校验，再逐张上传；临时缓存位于队列卷 `/data/media`，上传确认后清理。
队列持久保存下载标识和上传进度；网络或权限失败时重试，图片和 MD 都成功后才回复“已保存”。大小、数量或格式不符合时回复拒绝原因。
平台下载标识存在有效期，长期离线后部分资源可能无法恢复。FNS 附件上传没有原子 `createOnly`：程序上传前检查同路径，已有不同内容时拒绝覆盖，但无法排除其他客户端在检查与上传之间同时写入的情况。
若多图上传中途失败，已上传部分留在 FNS，队列会继续补齐；桥接程序不自动删除附件，也不监听笔记删除。

## 3. 内存、队列与故障查看

```bash
docker compose ps
docker stats --no-stream
docker compose exec bridge python /app/bridge.py \
  --config /config/config.toml --source telegram --status
```

将最后一条命令的 `telegram` 换成 `dingtalk` 或 `feishu`，可以查看其他渠道队列。
`pending` 等待写入；`saved` 已保存、等待确认；`done` 已完成。
`rejected` 等待发送图片拒绝原因；`rejected_done` 为已拒绝的消息，不表示笔记已保存。
健康检查判断主进程及渠道线程有没有退出，不表示平台或 FNS 当前一定可达。
FNS 暂时不可达时，收到的消息保存在磁盘队列中，稍后重试。

如果发现容器反复重启，可查看是否 OOM：

```bash
docker inspect "$(docker compose ps -q bridge)" --format '{{.State.OOMKilled}}'
```

只有确认宿主机有余量时才调高上限。例如在 `.env` 写入：

```dotenv
BRIDGE_MEMORY_LIMIT=160m
```

然后 `docker compose up -d` 应用新限制；`restart` 不会更新容器资源限制。
若 128 MiB 仍不够且宿主机无余量，可以在配置中暂时关闭一个渠道，重启容器。

## 4. 升级和旧版迁移

从 v0.1.0 升级图片版时，先为 FNS Token 增加 `file_r,file_w`，飞书应用增加资源下载权限并发布版本。保留原配置与队列卷。
首次启动会自动给 SQLite 队列增加附件信息列，旧消息内容及文件名保持不变。建议停容器后备份队列再升级。

升级前 `docker compose stop`，备份配置和队列。使用新的预构建镜像时：

```bash
docker load -i fns-message-bridge-linux-amd64.tar.gz
docker compose up -d --no-build --force-recreate
```

保留原有 `config.toml` 和队列卷；不要用示例配置覆盖真实凭证，也不要执行 `down -v`。
若选择自行构建，替换代码后执行：

```bash
docker compose build
docker compose up -d
```

旧版还没部署时，直接使用本包。
如果之前已经装过 systemd 版本，要先停止它，避免同一机器人被两个接收程序消费：

```bash
sudo systemctl disable --now fns-message-bridge@telegram fns-message-bridge@dingtalk fns-message-bridge@feishu
```

旧版队列位于 `/var/lib/fns-message-bridge`。不要在旧版仍运行时复制 SQLite 文件。
若有待处理消息，建议先处理完再切换；需要迁移队列时，在停旧服务后将目录内容复制到本项目队列卷，并设为 UID/GID 10001。
迁移时保留相同 FNS 账号、仓库和机器人的配置。

## 5. 停止与删除

停止并移除容器、网络，**保留消息队列**，以后可以重新启动：

```bash
docker compose down
```

完全移除本项目的容器、网络、队列卷和本地镜像：

```bash
docker compose down -v --rmi all
```

后者会删除尚未写入 FNS 的排队消息及去重记录。确认队列处理完或已备份后再执行。
已保存到 FNS 的笔记仍然保留。配置文件和部署文件夹保留，按需自行删除。
无需清理宿主机 Python 或系统服务。本项目不会创建额外数据库、Redis 或 systemd 服务。

## 验证与限制

图片版的测试和容器验证记录见下方链接。
三渠道模拟接收、HTTP 写入和保存确认通过。真实平台收消息与 FNS 多端同步尚需配置凭证后验收。
详情见 [verification.md](verification.md)。

飞书采用依据官方 SDK 协议实现的文本连接，若平台未来修改长连接协议，需要更新本模块。
当前不处理 PDF 等普通文件、视频、语音、消息编辑或删除；飞书富文本仅提取文字和图片，不保留排版。
Telegram 接收服务长期离线仍可能超过平台的 24 小时留存窗口，本地队列只保护已经收到的消息。

参考：[FNS 3.6.1 API](https://github.com/haierkeys/fast-note-sync-service/blob/3.6.1/docs/swagger.yaml)、[飞书官方长连接实现](https://github.com/larksuite/oapi-sdk-python/blob/v2_main/lark_oapi/ws/client.py)、[Docker 内存限制](https://docs.docker.com/reference/compose-file/services/#mem_limit)。

## 本地测试

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

测试使用虚拟凭证和本地模拟服务，不需要真实机器人账号。
