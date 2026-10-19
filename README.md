# EcoRun Logger Server

EcoRun 2026 の音声・テレメトリ基盤を Rust へ移行するためのサーバです。Discord を音声経路から外し、ロガー(SIM7070G) とブラウザ/PWAを同じ room に接続できる構成を目指しています。

設計・移行順序は [`PLAN.md`](./PLAN.md)、作業判断と検証記録は [`agent-memo/`](./agent-memo/) に残しています。

## 必要なもの

- Rust stable / Cargo
- libopus（Rust側の音声codec用）
- Node.js + **pnpm**（Web/PWAのビルド用）
- WebCodecs の Opus に対応したブラウザ（PWA音声機能を使う場合）

## 現在の機能

- Rust + Tokio + Axum の HTTP/WebSocket サーバ
- `GET /healthz`
- `GET /ws/audio/{room_id}`
- `GET/PUT /api/rooms/{room_id}/map`（center / zoom / point / line をSQLiteへ保存）
- `GET /api/rooms/{room_id}/telemetry`（受信テレメトリ履歴）
- 8 kHz mono / 60 ms Opus 音声の room fan-out
- bounded queue による slow receiver 対策
- room join / leave と participant 状態
- SIGINT / SIGTERM による graceful shutdown
- SvelteKit PWA（room UI / PTT / telemetry表示）
- OpenStreetMap + Leaflet のライブmap（logger位置、point、line、動的telemetry表示）
- 接続 / 音声設定をモーダルへ分離
- 任意でWebクライアント位置情報を1分ごとにtelemetryとして送信
- WebCodecs Opus のブラウザ送受信経路
- `web/build` の Rust 静的配信
- `/ws/audio/{room_id}` で受けたテレメトリのPodium Connect中継（下記）

## 使い方

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

### ブラウザ/PWA

1. Rustサーバを起動する。
2. `/` をブラウザで開く。
3. WebSocket endpoint を確認する（同一サーバなら自動入力）。
4. `Connect` を押す。
5. `Room` に参加したい room ID を入力して `Join`。
6. **HOLD TO TALK** を押している間だけマイク送信する。
7. `ROOM` で参加者、`TELEMETRY` で受信したイベントを確認する。

ブラウザ音声は WebCodecs の Opus encoder/decoder を使用しています。ブラウザが WebCodecs の Opus に対応していない場合、音声機能は利用できません。

### 設定

`.env.example` を参照してください。主な環境変数は以下です。

| 変数 | デフォルト | 用途 |
| --- | --- | --- |
| `ECORUN_BIND` | `0.0.0.0:3000` | HTTP/WebSocket待受アドレス |
| `RUST_LOG` | `ecorun_logger_server=info` | ログレベル |
| `ECORUN_DB_PATH` | `ecorun.sqlite3` | room定義・認証キー・Podium key・map設定・受信テレメトリを保存するSQLiteファイル |
| `ECORUN_PODIUM_ENDPOINT` | `telemetry.podium.live:8080` | Podium Connectの接続先（試験時は偽Podiumへ向ける） |
| `ECORUN_PODIUM_SERIAL` | `C3PROTO01` | 認証の `sn`（この値で認証が通ることを確認済み） |

roomはサーバ側のSQLiteで管理します。初回起動時に `main` / `race` / `test` を自動作成し、TUIで追加・削除・認証key再発行・Podium key設定ができます。存在しないroomへのSocket.IO接続は拒否します。認証keyもroom単位でDBに保存します。`GET /api/rooms` はroom IDと表示名だけを返し、認証keyやPodium keyは返しません。

管理TUIは同じサーババイナリから起動します。

```bash
cargo run -- admin
```

`↑/↓` でroom選択、`n` で作成、`k` で認証key表示、`r` で認証key再発行、`p` でPodium key編集、`d` で削除、`q` で終了です。`main` / `race` / `test` は削除できません。

## プロトコル仕様

### Raw WebSocket

ロガーとの互換経路は `GET /ws/audio/{room_id}` です。`room_id` は接続時の初期roomで、接続後に変更する `room.join` / `room.leave` は廃止しています。WebSocket text message は JSON 制御イベント、binary message は Opus 音声packetです。

接続直後、サーバは次の順で text message を送ります。

1. `hello`: `protocol: 2` と `session_id`
2. `audio.config`: `8 kHz / mono / 60 ms`
3. `room.state`: 初期roomの参加者一覧

`hello` の後は、クライアントから `hello` を送り直して音声能力・役割などを通知できます。

### Client → Server events

| `type` | 主なフィールド | 用途 |
| --- | --- | --- |
| `hello` | `client_id`, `protocol`, `role`, `audio_formats`, `audio_config`, `bitrate`, `mixer`, `forward_to_logger` | クライアント能力・役割・音声設定を通知 |
| `audio.mute` | `muted` | ミュート状態を変更 |
| `audio.ptt` | `active` | Push-to-talk状態を変更 |
| `telemetry` | `source`, `timestamp`, `data` | 任意のJSONテレメトリを保存・配信 |
| `podium.meta` | `meta` | Podiumチャンネル定義を中継 |
| `podium.samples` | `samples` | Podiumサンプル行を中継 |

`hello.audio_formats` を省略した場合はサーバの音声形式を受信できるものとして扱います。空配列の場合は受信音声を無効にします。`role: "logger"` はロガーとして扱われ、logger同士の音声転送は行いません。`forward_to_logger: false` でもloggerへの音声転送を止められます。

音声送信は `audio.ptt` が active、かつ `audio.mute` が false のときだけ有効です。binary message は Opus packetで、サーバは受信packetをデコードしてから8 kHz mono / 60 ms単位で再エンコードし、同じroomの対象クライアントへfan-outします。受信側ごとのqueueは3 packet（約180 ms）で、遅い受信者では古いpacketがdropされます。

`bitrate` は `4000..32000` bps を受け付けます。`audio_config` は現在 `sample_rate: 8000`, `channels: 1`, `frame_ms: 60` のみ対応します。`mixer` は受信してログに残しますが、サーバは個別Opus packetのfan-outを行い、音声mixingはしません。

音声は binary WebSocket message の 8 kHz mono Opus packet として送受信します。text message は JSON の制御イベントに使います。音声は同じ room の送信元以外へ fan-out され、遅い受信者には bounded queue により古いpacketをdropします。

サーバからの制御イベントには `hello`、`audio.config`、`room.state`、`error` があります。`error` は `code` と `message` を持ちます。主な `code` は `invalid_control`、`invalid_room`、`unsupported_audio_config`、`invalid_bitrate`、`codec_init` です。

`room.state` の `participants` は `session_id`、`muted`、`ptt` を持ちます。room移動、mute、PTT、切断などでroom状態が変化すると、そのroomの参加者へ新しい `room.state` が配信されます。

### Telemetry / map API

ロガーは `/ws/audio/{room_id}` のtext messageで `telemetry` を送れます。`source` と `timestamp` は省略可能で、`data` は任意のJSON値です。

```json
{"type":"telemetry","source":"ecologger","timestamp":"2026-10-17T03:00:00Z","data":{"latitude":35.68,"longitude":139.76,"speed_kmh":42.1}}
```

受信データはSQLiteへ保存され、同じroomのRaw WebSocketクライアントへ即時配信されます。Socket.IOから送られた `telemetry` も同じSQLiteへ保存され、同じnamespaceのSocket.IOクライアントへ配信されます。Web UIは緯度/経度の一般的なキー（`lat` / `latitude` / `gps_lat` など）を検出してlogger位置としてmapへ表示します。UIはtelemetryのキー構成を固定せず、JSONをそのまま表示します。

map設定は `center_lat` / `center_lon` / `zoom` と、`point` / `line` の座標・色をroom単位で保存します。Socket.IOの `map.update` でも更新できるため、複数のWebクライアントから同じ設定を共有できます。

### Socket.IO

ブラウザ/PWAは Socket.IO の `/{room_id}` namespaceを使用します。接続時に `hello`（`protocol: 1`）、`audio.config`、`room.state`、必要なら `map.state` が送られます。主なクライアントイベントは `audio.mute`、`audio.ptt`、`audio.packet`、`map.update`、`telemetry` です。サーバからは `participant.joined`、`participant.updated`、`participant.left`、`telemetry`、`map.state` などが配信されます。

Raw WebSocketのprotocol versionは **2**、Socket.IO namespaceのprotocol versionは **1** です。これは別の経路のバージョン番号なので、クライアントは接続方式に応じて解釈してください。

### Audio routing

raw WebSocketのloggerはhelloで `{"role":"logger"}` を指定できます。loggerは他のloggerから音声を受け取らず、room内の他ユーザーからの音声だけを受け取ります。`audio_formats: []` なら受信音声を無効にできます。`forward_to_logger: false` でもloggerへの音声転送を停止できます。`audio_config` は現在 `8 kHz / mono / 60 ms` に固定し、`bitrate` は `4000..32000` bps を受け付けます。`mixer` はクライアント指定値を受信してログに残すだけです。ミックスを受けたいときは、下の「ロガー向けミックス」の形式を `audio_formats` に入れます。

### Phase 4.1 実装

ブラウザ/PWAのリアルタイム通信は `socketioxide` + `socket.io-client` を使用します。Redis adapterは使用せず、単一のサーバプロセス内でroom状態とイベントを共有します。クライアントは `/api/rooms` から事前定義されたroom一覧を取得し、入力されたroom keyを `/{room_id}` namespaceのSocket.IO auth payloadとして渡して接続します。room切替時はnamespace URLを変更して再接続します。切断時は自動再接続し、復帰後にroom状態とmap設定を再取得します。ロガー向け `/ws/audio/{room_id}` は従来のWebSocket互換経路として維持します。

### メトリクスのPodium中継（Phase 5の一部）

ロガー（EcoLogger）は既存の `/ws/audio/{room_id}`（Socket.IOではない方）でPodium形式の行を送り、
サーバがPodium Connectとの接続（認証・連番・生存確認・再接続）を受け持ちます。ストリーミングキーはサーバ側SQLiteのroom定義に置き、ロガーとの間では流しません（Podium Connect自体は暗号化なしのTCPのため）。

ロガー → サーバ（text message）:

```json
{"type":"hello","client_id":"ecologger-v2","protocol":2,"audio_formats":[]}
{"type":"podium.meta","meta":[{"nm":"Interval","ut":"ms","sr":10,"min":0,"max":0,"prec":0}, ...]}
{"type":"podium.samples","samples":[[12300,1791077786762,34.843112,...,63], ...]}
```

- `meta` はPodiumの `{"s":{"meta":[...]}}` の中身そのまま。チャンネル構成が変わったら送り直す
- `samples` の各行はPodiumの `d`（値をmeta順に並べ、最後にビットマスク）。1メッセージ最大50行（ロガーは5行ずつ）。`t` はサーバが振る
- hello の `audio_formats` はクライアントが再生できる音声形式の一覧（`audio.config` と同じ `sample_rate`・`channels`・`frame_ms`）。
  サーバの形式を含まないクライアントには通話音声を流さない。空の一覧なら音声を一切受けない。
- **ロガー向けミックス**：一覧に `{"sample_rate":8000,"channels":1,"frame_ms":120}` を含めると、同じroomで話している全員
  （ブラウザのSocket.IO通話も含む）を1本にまとめた音声だけを受ける（自分の声は除く）。120msごとに1パケット、
  既定6kbps（`ECORUN_LOGGER_AUDIO_BITRATE`）、誰も話していない間は何も送らない。ロガーの送信待ちが詰まったら
  その音声は捨て、テレメトリを遅らせない。`cargo run --bin logger_mix_test -- http://127.0.0.1:3000 <room> 10 <roomのキー>` で確認できる
  省略したクライアント（既存のロガー・ブラウザ）はこれまでどおり受け取る
- 中継はroomごとに1本。1つのroomに送るロガーは1台にする

サーバ → room内の全員（text message）:

```json
{"type":"podium.status","room_id":"main","state":"streaming","lt":44}
{"type":"error","code":"podium_not_configured","message":"..."}
```

`state` は `connecting` / `streaming` / `error` / `idle`（ロガーが60秒黙ったら切断）/ `waiting_meta`。
`lt` はPodiumの生存確認（約5秒ごと）で返る「最後に受け取ったサンプル番号」。

注意：新Podiumは**GNSS由来の `Utc` が入るまでストリームを表示しない**。測位待ちの判断はロガー側で行い、サーバは中身を変えずに中継する。

### データベース

SQLiteはサーバ起動時に `ECORUN_DB_PATH`（未設定なら `ecorun.sqlite3`）を開き、必要なテーブルを自動作成します。現在は専用のDB管理CLIやmigrationコマンドはなく、map設定はWeb UI/APIから、telemetryはWebSocket/Socket.IO経由でアプリケーションが自動的に読み書きします。

作成される主なテーブルは `map_configs`、`map_plots`、`telemetry` です。SQLiteはWAL modeを使用するため、ローカルでは `ecorun.sqlite3` に加えて `ecorun.sqlite3-wal` / `ecorun.sqlite3-shm` が生成されることがあります。これらは実行時データなのでGit管理対象外です。

手動で中身を確認・バックアップする場合は、SQLite対応のツールで `ECORUN_DB_PATH` のファイルを開いてください。アプリケーション停止後に操作するのが安全です。

偽Podiumを使った通し試験:

```bash
ECORUN_PODIUM_ENDPOINT=127.0.0.1:18080 cargo run
cargo run --bin podium_relay_test
```


### Phase 1 test client

サーバを起動した状態で、別ターミナルから実行します。

```bash
cargo run --bin phase1_test_client
```

別ホストへ接続する場合:

```bash
cargo run --bin phase1_test_client -- ws://192.168.1.10:3000/ws/audio/main
```

hello/config、初期room、8 kHz mono / 60 ms Opus、room内fan-outをまとめて確認できます。

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

## 現在の状態と残作業

Phase 2 の実機検証（SIM7070G / Tailscale）はまだ完了していません。Phase 3 のWeb/PWA実装は入っていますが、ブラウザの WebCodecs Opus を実機で確認する必要があります。

次に行う検証:

```bash
cd web
pnpm install
pnpm run build
```

その後、ブラウザ実機で音声送受信を確認し、さらに実ロガーで連続運用・瞬断・帯域・遅延を測定します。

詳細な移行計画とPhaseごとの完了条件は [`PLAN.md`](./PLAN.md) を参照してください。

