[English](README.md) | **简体中文** | [日本語](README.ja.md)

<h1 align="center">pgrust</h1>

<p align="center">
  <strong>用 Rust 重写 Postgres。</strong>
</p>

<p align="center">
  <img alt="Postgres 18.3" src="https://img.shields.io/badge/Postgres-18.3-336791">
  <img alt="回归查询：46k+" src="https://img.shields.io/badge/regression_queries-46k%2B-brightgreen">
  <a href="https://github.com/malisper/pgrust/blob/main/LICENSE">
    <img alt="许可证：AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-blue">
  </a>
</p>

<div align="center">
  <a href="https://pgrust.com">浏览器演示</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://discord.gg/FZZ4dbdvwU">Discord</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://pgrust.com/#updates">获取 pgrust 更新</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://github.com/malisper/pgrust/issues">问题反馈</a>
</div>

<br />

pgrust 以兼容 Postgres 18.3 为目标，并且在 46,000 多条回归查询中，
其输出与 Postgres 的预期输出一致。

pgrust 与 Postgres 磁盘兼容，可以从现有的 Postgres 18.3 数据目录启动。

项目目标是让 Postgres 更容易从内部进行修改：保持与 Postgres 一致的行为，
以真正的 Postgres 测试作为判定标准，并使用 Rust 和 AI 辅助编程探索更深层的
服务器改造。

更新：我们正在开发一个尚未发布的新版 pgrust。它目前已通过 100% 的 Postgres
回归测试套件，采用每个连接一个线程而非每个连接一个进程的模型，在事务型工作负载上
比 Postgres 快 50%，在分析型工作负载上快约 300 倍（在 clickbench 上比 ClickHouse
慢 2 倍，我们认为它可以变得比 ClickHouse 更快）。关注 pgrust 或加入我们的 Discord
以获取更新！

## 关注 pgrust

[通过电子邮件获取项目更新](https://pgrust.com/#updates)，包括新版本、
兼容性里程碑和架构实验。

## 状态

pgrust 尚未达到生产就绪状态，也尚未进行性能优化。

现有的 Postgres 扩展和 PL/Python、PL/Perl、PL/Tcl 等过程语言扩展通常还不兼容。
部分随附的 contrib 模块已完成移植，未来可能支持更多兼容功能。

## 路线图

- 多线程 Postgres 内部实现
- 内置连接池
- 更好地支持 JSON 密集型工作负载
- 快速派生和分支工作流
- 存储实验，包括无需 vacuum 的设计
- 针对不良查询和 AI 生成 SQL 的运行时防护
- 减少执行计划突然恶化的情况

## 试用

访问 https://pgrust.com 试用 WebAssembly 演示。

Docker：

```bash
docker run -d --name pgrust -e POSTGRES_PASSWORD=secret malisper/pgrust:v0.1 && until docker exec -e PGPASSWORD=secret pgrust psql -h 127.0.0.1 -U postgres -c '\q' >/dev/null 2>&1; do sleep 1; done && docker exec -it -e PGPASSWORD=secret pgrust psql -h 127.0.0.1 -U postgres; docker rm -f pgrust
```

此命令使用 Docker 镜像内的 `psql` 客户端。

`malisper/pgrust:latest` 当前指向同一版本，但 `v0.1` 是固定的首发镜像。

## 从源码构建

macOS：

```bash
brew install icu4c openssl@3 libpq

export LIBRARY_PATH="$(brew --prefix openssl@3)/lib:${LIBRARY_PATH:-}"
export PKG_CONFIG_PATH="$(brew --prefix openssl@3)/lib/pkgconfig:$(brew --prefix icu4c)/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
export PATH="$(brew --prefix libpq)/bin:$PATH"
```

Debian/Ubuntu：

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libicu-dev libssl-dev libldap2-dev libpam0g-dev postgresql-client-18
```

构建：

```bash
PGRUST_PGSHAREDIR="$PWD/vendor/postgres-18.3/share" \
cargo build --release --locked --bin postgres
```

创建数据目录：

```bash
target/release/postgres --initdb \
  -D /tmp/pgrust-data \
  -L "$PWD/vendor/postgres-18.3/share" \
  --no-locale \
  --encoding UTF8 \
  -U postgres
```

运行 pgrust：

```bash
ulimit -s 65520

RUST_MIN_STACK=33554432 target/release/postgres \
  -D /tmp/pgrust-data \
  -F \
  -c listen_addresses= \
  -k /tmp \
  -p 5432 \
  -c io_method=sync \
  -c max_stack_depth=60000
```

连接：

```bash
psql -h /tmp -p 5432 -U postgres -d postgres \
  -c "select version(), 1 + 1 as two"
```

## 回归测试

针对 pgrust 运行 Postgres 回归测试：

```bash
PGRUST_BIN="$PWD/target/release/postgres" \
scripts/run-regression
```

运行器使用 pgrust 自带的 `--initdb` 和本仓库中随附的 Postgres 18.3 测试文件。
它要求 Postgres 18 的 `psql` 客户端存在于 `PATH` 中；如果 `psql` 位于其他位置，
请设置 `PGRUST_PSQL=/path/to/psql`。

经验证的首发结果：pgrust 在 46,000 多条回归查询中与 Postgres 的预期输出一致。

## 历史

本仓库现在包含达到回归测试里程碑的新版 pgrust 实现。

旧的公开实现已归档至 `archive/pre-fabled-2026-06-23`。

背景资料：

- pgrust 最初发布：https://malisper.me/pgrust-rebuilding-postgres-in-rust-with-ai/
- 67% 回归测试进展：https://malisper.me/pgrust-update-at-67-postgres-compatibility-and-accelerating/
- Four Horsemen 路线图：https://malisper.me/the-four-horsemen-behind-thousands-of-postgres-outages/

## 反馈

如果出现故障、设置说明令人困惑，或你希望优先实现某项 Postgres 改进，请提交 issue。

## 联系方式

- 电子邮件：maintainers@pgrust.com
- Discord：https://discord.gg/FZZ4dbdvwU
- 项目更新：https://pgrust.com/#updates

## 许可证

pgrust 采用 AGPL-3.0 许可证。请参阅 `LICENSE`。
