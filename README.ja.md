[English](README.md) | [简体中文](README.zh-CN.md) | **日本語**

<h1 align="center">pgrust</h1>

<p align="center">
  <strong>Postgres を Rust で書き直した実装です。</strong>
</p>

<p align="center">
  <img alt="Postgres 18.3" src="https://img.shields.io/badge/Postgres-18.3-336791">
  <img alt="回帰クエリ：46k+" src="https://img.shields.io/badge/regression_queries-46k%2B-brightgreen">
  <a href="https://github.com/malisper/pgrust/blob/main/LICENSE">
    <img alt="ライセンス：AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-blue">
  </a>
</p>

<div align="center">
  <a href="https://pgrust.com">ブラウザデモ</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://discord.gg/FZZ4dbdvwU">Discord</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://pgrust.com/#updates">pgrust の最新情報</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://github.com/malisper/pgrust/issues">Issues</a>
</div>

<br />

pgrust は Postgres 18.3 との互換性を目標とし、46,000 件を超える回帰クエリで
Postgres の期待される出力と一致しています。

pgrust は Postgres とディスク互換性があり、既存の Postgres 18.3
データディレクトリから起動できます。

目標は、Postgres を内部から変更しやすくすることです。Postgres らしい動作を保ち、
実際の Postgres テストを正解基準として使い、Rust と AI 支援プログラミングによって
より深いサーバー変更を探ります。

更新：現在、未公開の新しい pgrust を開発しています。このバージョンは Postgres
回帰テストスイートの 100% に合格し、接続ごとにプロセスを割り当てる代わりに
接続ごとにスレッドを割り当てるモデルを採用しています。トランザクション系ワークロードでは
Postgres より 50% 高速で、分析系ワークロードでは約 300 倍高速です
（clickbench では ClickHouse の 2 倍遅いものの、ClickHouse より高速にできると
考えています）。最新情報を得るには pgrust をフォローするか、Discord にご参加ください！

## pgrust をフォロー

[プロジェクトの最新情報をメールで受け取る](https://pgrust.com/#updates)と、
新しいリリース、互換性のマイルストーン、アーキテクチャ実験について確認できます。

## 状況

pgrust はまだ本番環境で利用できる段階ではなく、パフォーマンス最適化も未実施です。

既存の Postgres 拡張機能や、PL/Python、PL/Perl、PL/Tcl などの
手続き型言語拡張機能には、まだ通常の互換性がありません。バンドルされた contrib
モジュールの一部はすでに移植されており、今後さらに互換性を高められる可能性があります。

## ロードマップ

- Postgres 内部のマルチスレッド化
- 組み込みのコネクションプーリング
- JSON を多用するワークロードのサポート改善
- 高速なフォークおよびブランチワークフロー
- vacuum 不要の設計を含むストレージ実験
- 問題のあるクエリと AI 生成 SQL に対するランタイムガードレール
- 突発的な不適切な実行プラン切り替えの削減

## 試す

https://pgrust.com で WebAssembly デモをお試しください。

Docker：

```bash
docker run -d --name pgrust -e POSTGRES_PASSWORD=secret malisper/pgrust:v0.1 && until docker exec -e PGPASSWORD=secret pgrust psql -h 127.0.0.1 -U postgres -c '\q' >/dev/null 2>&1; do sleep 1; done && docker exec -it -e PGPASSWORD=secret pgrust psql -h 127.0.0.1 -U postgres; docker rm -f pgrust
```

このコマンドは Docker イメージ内の `psql` クライアントを使用します。

`malisper/pgrust:latest` は現在同じリリースを参照していますが、
`v0.1` が固定された初回リリース用イメージです。

## ソースからビルド

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

ビルド：

```bash
PGRUST_PGSHAREDIR="$PWD/vendor/postgres-18.3/share" \
cargo build --release --locked --bin postgres
```

データディレクトリを作成：

```bash
target/release/postgres --initdb \
  -D /tmp/pgrust-data \
  -L "$PWD/vendor/postgres-18.3/share" \
  --no-locale \
  --encoding UTF8 \
  -U postgres
```

pgrust を実行：

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

接続：

```bash
psql -h /tmp -p 5432 -U postgres -d postgres \
  -c "select version(), 1 + 1 as two"
```

## 回帰テスト

pgrust に対して Postgres の回帰テストを実行します。

```bash
PGRUST_BIN="$PWD/target/release/postgres" \
scripts/run-regression
```

テストランナーは pgrust 自身の `--initdb` と、このリポジトリに同梱された
Postgres 18.3 のテストファイルを使用します。Postgres 18 の `psql`
クライアントが `PATH` 上に必要です。`psql` が別の場所にある場合は、
`PGRUST_PSQL=/path/to/psql` を設定してください。

初回リリース時の検証結果：pgrust は 46,000 件を超える回帰クエリで
Postgres の期待される出力と一致しました。

## 履歴

このリポジトリには現在、回帰テストのマイルストーンに到達した新しい
pgrust 実装が含まれています。

以前公開されていた実装は `archive/pre-fabled-2026-06-23` に
アーカイブされています。

背景情報：

- pgrust の初回リリース：https://malisper.me/pgrust-rebuilding-postgres-in-rust-with-ai/
- 回帰テスト 67% 到達時の更新：https://malisper.me/pgrust-update-at-67-postgres-compatibility-and-accelerating/
- Four Horsemen ロードマップ：https://malisper.me/the-four-horsemen-behind-thousands-of-postgres-outages/

## フィードバック

問題が発生した場合、セットアップが分かりにくい場合、または優先してほしい
Postgres の改善がある場合は、issue を作成してください。

## 連絡先

- メール：maintainers@pgrust.com
- Discord：https://discord.gg/FZZ4dbdvwU
- プロジェクトの最新情報：https://pgrust.com/#updates

## ライセンス

pgrust は AGPL-3.0 でライセンスされています。`LICENSE` を参照してください。
