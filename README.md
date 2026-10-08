# EcoRun Logger Server

EcoRun 2026 の音声・テレメトリ基盤を Rust へ移行するためのサーバです。Discord を音声経路から外し、Raspberry Pi / SIM7070G とブラウザ/PWAを同じ room に接続できる構成を目指しています。

設計・移行順序は [`PLAN.md`](./PLAN.md)、作業判断と検証記録は [`agent-memo/`](./agent-memo/) に残しています。

## 必要なもの

- Rust stable / Cargo
- libopus（Rust側の音声codec用）
- Node.js + **pnpm**（Web/PWAのビルド用）
- WebCodecs の Opus に対応したブラウザ（PWA音声機能を使う場合）

## まず動かす

### 1. Rustサーバ

```bash
cargo run
```

デフォルトでは `0.0.0.0:3000` で待ち受けます。環境変数で変更できます。

```bash
ECORUN_BIND=127.0.0.1:3000 RUST_LOG=ecorun_logger_server=debug cargo run
```

ヘルスチェック:

```bash
curl http://127.0.0.1:3000/healthz
```

`ok` が返ればサーバは起動しています。

### 2. Web/PWAをビルド

Web側は pnpm を使用します。

```bash
cd web
pnpm install
pnpm run build
cd ..
cargo run
```

ビルドされた `web/build` は Rust サーバから静的配信されます。ブラウザで `http://<server>:3000/` を開いてください。

依存関係を更新した場合は `web/pnpm-lock.yaml` をコミットしてください。

### Web UIだけを開発する

```bash
cd web
pnpm install
pnpm dev
```

Vite の開発サーバが表示するURLを開き、画面上の **WebSocket endpoint** に Rust サーバの `ws://<server>:3000/ws/audio/{room_id}` を指定します。Web UI単体では、Rust側の静的ファイル配信は使いません。

## 使い方

### ブラウザ/PWA

1. Rustサーバを起動する。
2. `/` をブラウザで開く。
3. WebSocket endpoint を確認する（同一サーバなら自動入力）。
4. `Connect` を押す。
5. `Room` に参加したい room ID を入力して `Join`。
6. **HOLD TO TALK** を押している間だけマイク送信する。
7. `ROOM` で参加者、`TELEMETRY` で受信したイベントを確認する。

ブラウザ音声は WebCodecs の Opus encoder/decoder を使い、既存Pi経路に合わせて **8 kHz / mono / 60 ms / 8 kbps** を基本設定としています。ブラウザが WebCodecs の Opus に対応していない場合、音声機能は利用できません。

### Room / WebSocket

WebSocket endpoint は `GET /ws/audio/{room_id}` です。`room_id` で最初から参加する room を指定し、接続直後に `hello`、`audio.config`、`room.state` が送られます。

room参加:

```json
{"type":"room.join","room_id":"race"}
```

room退出:

```json
{"type":"room.leave"}
```

音声は binary WebSocket message の 8 kHz mono Opus packet として送受信します。text message は JSON の制御イベントに使います。音声は同じ room の送信元以外へ fan-out され、遅い受信者には bounded queue により古いpacketをdropします。

### Phase 1 test client

サーバを起動した状態で、別ターミナルから実行します。

```bash
cargo run --bin phase1_test_client
```

別ホストへ接続する場合:

```bash
cargo run --bin phase1_test_client -- ws://192.168.1.10:3000/ws/audio/main
```

hello/config、room.join、8 kHz mono / 60 ms Opus、room内fan-outをまとめて確認できます。

## 設定

`.env.example` を参照してください。主な環境変数は以下です。

| 変数 | デフォルト | 用途 |
| --- | --- | --- |
| `ECORUN_BIND` | `0.0.0.0:3000` | HTTP/WebSocket待受アドレス |
| `RUST_LOG` | `ecorun_logger_server=info` | ログレベル |

## 現在の機能

- Rust + Tokio + Axum の HTTP/WebSocket サーバ
- `GET /healthz`
- `GET /ws/audio/{room_id}`
- 8 kHz mono / 60 ms Opus 音声の room fan-out
- bounded queue による slow receiver 対策
- room join / leave と participant 状態
- SIGINT / SIGTERM による graceful shutdown
- SvelteKit PWA（room UI / PTT / telemetry表示）
- WebCodecs Opus のブラウザ送受信経路
- `web/build` の Rust 静的配信

## 現在の状態と残作業

Phase 2 の実機検証（Raspberry Pi / SIM7070G / Tailscale）はまだ完了していません。Phase 3 のWeb/PWA実装は入っていますが、ブラウザの WebCodecs Opus を実機で確認する必要があります。

次に行う検証:

```bash
cd web
pnpm install
pnpm run build
```

その後、ブラウザ実機で音声送受信を確認し、さらに実Piで連続運用・瞬断・帯域・遅延を測定します。

詳細な移行計画とPhaseごとの完了条件は [`PLAN.md`](./PLAN.md) を参照してください。
## Dev Container

`.devcontainer/` にRustサーバとWebクライアントの開発環境を用意しています。Docker対応のVS Codeで「Reopen in Container」を実行すると、Rust、libopus、Node.js、pnpm 10.0.0をまとめて利用できます。

コンテナ作成後は `web` の依存関係が自動でインストールされます。

### サーバを起動

```bash
cd web
pnpm build
cd ..
cargo run
```

サーバは `http://localhost:3000`、ヘルスチェックは `http://localhost:3000/healthz` です。`ECORUN_BIND` と `RUST_LOG` は `.env.example` を参考に設定できます。

### Webを開発

```bash
cd web
pnpm dev --host 0.0.0.0
```

Viteは `5173` 番ポートで動作します。詳細は `.devcontainer/README.md` を参照してください。
