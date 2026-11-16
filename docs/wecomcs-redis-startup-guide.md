# 企业微信客服 Redis 启动说明

## 1. 服务器 compose 关键配置

当前项目里的 `docker/docker-compose.yaml` 已经包含 Redis：

- `redis:7-alpine`
- 开启 `appendonly yes`
- 挂载 `./data/redis:/data`

这意味着：

- Redis 数据会写到宿主机 `data/redis`
- 容器重建后数据仍然保留
- access_token、消息状态、Streams、重试队列都不会因为容器重建直接丢失

## 2. data/config.yaml 需要开启的配置

运行时实际读取的是 `data/config.yaml`，不是模板文件。

至少加上：

```yaml
redis:
  enabled: true
  url: 'redis://redis:6379/0'
  key_prefix: 'langbot'

wecomcs_scheduler:
  enabled: true
  token_refresh_skew_seconds: 300
  pull_stream_shard_count: 2
  process_stream_shard_count: 4
  pull_consumer_group: 'wecomcs-pull-group'
  process_consumer_group: 'wecomcs-process-group'
  stream_block_ms: 1000
  stream_batch_size: 10
  retry_poll_interval_seconds: 3
  retry_max_attempts: 3
  retry_backoff_seconds: [15, 30, 45]
  message_state_ttl_seconds: 604800
  cursor_bootstrap_mode: 'latest'
  lock_ttl_seconds: 60
```

## 3. 推荐部署命令

### 更新 LangBot，但不要动 Redis

如果服务名叫 `langbot`，推荐这样：

```bash
docker compose pull langbot
docker compose up -d --no-deps langbot
```

如果是本地重新构建镜像：

```bash
docker compose build langbot
docker compose up -d --no-deps langbot
```

### 不建议这样做

```bash
docker compose down
```

因为它会把整个 compose 里的容器都停掉，Redis 也会一起停。

## 4. `docker compose up -d` 到底会不会重启 Redis

通常情况下：

```bash
docker compose up -d
```

**如果 Redis 服务定义没有变化，一般不会被重建。**

但为了避免误操作，线上更新 LangBot 时，还是建议使用：

```bash
docker compose up -d --no-deps langbot
```

这样最稳，语义也最清楚：

- 只更新 LangBot
- 不处理依赖服务
- 不碰 Redis

## 5. 本次实现后的状态存储方式

- `cursor`：存数据库，持久保留
- 最近消息状态：存 Redis，默认保留 7 天
- Redis Streams：用于异步处理链路

## 6. 联调时建议重点看这些日志

- `[wecomcs][cursor-store]`
- `[wecomcs][state]`
- `[wecomcs][message-state]`
- `[wecomcs][pull-worker]`
- `[wecomcs][message-worker]`
- `[wecomcs][runtime]`
- `[wecomcs][retry]`
- `[wecomcs][token-cache]`

## 7. 这次新增的关键行为

### 冷启动默认跳过历史 backlog

当某个 `open_kfid` 还没有 cursor 时：

- 默认 `cursor_bootstrap_mode: latest`
- 先把 cursor 推进到最新位点
- 跳过更早历史页
- 但会保留 bootstrap 末页中的最新消息继续进入处理链路

这样可以避免新部署后把前几天的消息重新消费，同时不吞掉用户刚刚发来的第一条真实消息。

### 最近消息状态机

消息状态最小集：

- `queued`
- `processing`
- `done`
- `failed`

用于避免重复处理，并辅助排查问题。


## 8. Bot 级高级配置

现在企业微信客服 bot 页面支持直接配置以下高级参数，未修改时默认沿用系统默认值：

- 历史消息过滤时间窗口（秒）
- 昵称查询超时（秒）
- 重试次数
- 重试间隔
- pull 锁超时（秒）

其中 `retry_backoff_seconds` 在 bot 页面中使用逗号分隔格式，例如：`15,30,45`。

配置优先级为：

1. bot 页面填写的配置
2. `wecomcs_scheduler` 全局默认配置
3. 代码内置默认值

## 9. 客服欢迎语

在 LangBot 的机器人管理中编辑“企业微信客服”或“企微应用授权管理微信客服”机器人，在适配器配置中填写“客服欢迎语”，保存后生效。文案保存在现有 `adapter_config` JSON 的新增 `welcome_message` 字段中，无需数据库迁移，也无需修改或重新构建公共前端页面。

- 默认为空；留空或仅填写空白字符时关闭欢迎语，原有回调接收、询价和消息回复继续正常运行。
- 文案支持换行，最多2048个 UTF-8 字节，中文通常每字占3字节。超过限制时跳过发送，并在日志中提示。
- 在 `sync_msg` 拉取到 `enter_session` 事件后立即调用 `kf/send_msg_on_event`，不经过 Dify 或普通消息处理队列。Redis 调度和直接回调处理复用同一逻辑。
- 企微提供的 `welcome_code` 有效期为20秒；存在消息时间戳时跳过已过期事件。单次欢迎语处理最多等待5秒，且不超过根据时间戳计算出的剩余有效期。
- 使用稳定消息 ID 和短时去重记录避免重复发送。Redis 可用时共享去重记录；不可用时使用当前进程的短时记录。访问令牌失效时最多刷新并重试一次；其他失败仅记录日志，不阻断普通消息处理，也不进入常规延迟重试队列。
- 企微只在满足条件时返回 `welcome_code`，包括客户过去48小时未收到欢迎语且未向客服发过消息。通过客服链接进入会话才触发进入事件，从已有会话列表打开不会触发。测试时应使用符合条件的客户和新的客服入口。

仅欢迎语文案发生变更时，在机器人配置页保存即可，无需重启整个服务。保存配置会沿用现有机制重载该机器人，并重置该机器人的现有 Dify 会话关联；首次升级代码需重启 LangBot 服务。

官方说明：[接收消息和事件](https://developer.work.weixin.qq.com/document/path/94670)、[发送欢迎语等事件响应消息](https://developer.work.weixin.qq.com/document/path/95122)。
