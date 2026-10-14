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

### 音声再生の診断用テスト音源

ブラウザの受信・デコード・再生経路を確認するには、別ターミナルで合成した440 Hzのトーンを送信します（マイク不要）。このテストクライアントは実際の音声経路と同じ8 kHz mono / 60 ms Opusパケットを送ります。MP3ファイルをそのままWebSocketに送ることはできません。サーバーの音声プロトコルはOpusパケットを要求します。

```bash
cargo run --bin audio_test_client
```

引数は `WebSocket URL`、`room ID`、`再生秒数` の順で、省略時は `ws://127.0.0.1:3000/ws/audio/main`、`main`、30秒です。例えば:

```bash
cargo run --bin audio_test_client -- ws://127.0.0.1:3000/ws/audio/main main 60
```

ブラウザも同じroom（`main`）に接続して受信ログと音を確認してください。

## 設定

`.env.example` を参照してください。主な環境変数は以下です。

| 変数 | デフォルト | 用途 |
| --- | --- | --- |
| `ECORUN_BIND` | `0.0.0.0:3000` | HTTP/WebSocket待受アドレス |
| `RUST_LOG` | `ecorun_logger_server=info` | ログレベル |
| `ECORUN_PODIUM_KEYS` | 空（中継しない） | room ID → Podiumストリーミングキー のJSON。例 `{"main":"XXXXXXXX"}`。**キーはGitに入れない** |
| `ECORUN_PODIUM_ENDPOINT` | `telemetry.podium.live:8080` | Podium Connectの接続先（試験時は偽Podiumへ向ける） |
| `ECORUN_PODIUM_SERIAL` | `C3PROTO01` | 認証の `sn`（この値で認証が通ることを確認済み） |

## Podium中継（Phase 5の一部）

ロガー（EcoLogger）は既存の `/ws/audio/{room_id}`（Socket.IOではない方）でPodium形式の行を送り、
サーバがPodium Connectとの接続（認証・連番・生存確認・再接続）を受け持ちます。ストリーミングキーはサーバ側の
`ECORUN_PODIUM_KEYS` だけに置き、ロガーとの間では流しません（Podium Connect自体は暗号化なしのTCPのため）。

ロガー → サーバ（text message）:

```json
{"type":"hello","client_id":"ecologger-v2","protocol":2,"audio_formats":[]}
{"type":"podium.meta","meta":[{"nm":"Interval","ut":"ms","sr":10,"min":0,"max":0,"prec":0}, ...]}
{"type":"podium.samples","samples":[[12300,1791077786762,34.843112,...,63], ...]}
```

- `meta` はPodiumの `{"s":{"meta":[...]}}` の中身そのまま。チャンネル構成が変わったら送り直す
- `samples` の各行はPodiumの `d`（値をmeta順に並べ、最後にビットマスク）。1メッセージ最大50行（ロガーは5行ずつ）。`t` はサーバが振る
- hello の `audio_formats` はクライアントが再生できる音声形式の一覧（`audio.config` と同じ `sample_rate`・`channels`・`frame_ms`）。
  サーバの形式を含まないクライアントには通話音声を流さない。ロガーは空の一覧を送る（LTEの通信量対策）。
  省略したクライアント（既存のPi・ブラウザ）はこれまでどおり受け取る
- 中継はroomごとに1本。1つのroomに送るロガーは1台にする

サーバ → room内の全員（text message）:

```json
{"type":"podium.status","room_id":"main","state":"streaming","lt":44}
{"type":"error","code":"podium_not_configured","message":"..."}
```

`state` は `connecting` / `streaming` / `error` / `idle`（ロガーが60秒黙ったら切断）/ `waiting_meta`。
`lt` はPodiumの生存確認（約5秒ごと）で返る「最後に受け取ったサンプル番号」。

注意：新Podiumは**GNSS由来の `Utc` が入るまでストリームを表示しない**。測位待ちの判断はロガー側で行い、サーバは中身を変えずに中継する。

偽Podiumを使った通し試験:

```bash
ECORUN_PODIUM_ENDPOINT=127.0.0.1:18080 ECORUN_PODIUM_KEYS='{"podium-test":"TESTKEY1"}' cargo run
cargo run --bin podium_relay_test
```

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
- `/ws/audio/{room_id}` で受けたテレメトリのPodium Connect中継（下記）

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

### Phase 4.1 実装

ブラウザ/PWAのリアルタイム通信は `socketioxide` + `socket.io-client` を使用します。複数Loggerプロセス間のイベント配信には `socketioxide-redis` を利用できます。`ECORUN_REDIS_ENABLED=true` で有効化し、`ECORUN_REDIS_URL` と `ECORUN_REDIS_NAMESPACE` で接続先とprefixを指定します。無効時はプロセス内adapterを使用します。クライアントは `/api/servers` から接続先候補を取得し、`/{room_id}` namespaceでroomへ自動参加します。room切替時はnamespace URLを変更して再接続します。切断時は自動再接続し、復帰後にroom状態を再取得します。サーバが復旧しない場合は選択を維持し、ユーザーが接続先を切り替えます。Pi向け `/ws/audio/{room_id}` は従来のWebSocket互換経路として維持します。

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
