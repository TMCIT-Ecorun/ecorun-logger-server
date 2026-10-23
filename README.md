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

Vite の開発サーバが表示するURLを開き、画面上の **Server URL** に Rust サーバの `http://<server>:3000` を指定します。Web UIは Socket.IO の `/socket.io/` endpointを使用します。Web UI単体では、Rust側の静的ファイル配信は使いません。

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
2. `audio.config`: `8 kHz / mono / 60 ms`（初期値。`hello` で120ms受信を宣言したloggerには、続けて120ms版を通知）
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

音声送信は `audio.ptt` が active、かつ `audio.mute` が false のときだけ有効です。binary message は Opus packetで、サーバは受信packetをデコードしてから8 kHz mono / 60 ms単位で再エンコードし、同じroomの対象クライアントへfan-outします。受信側ごとのqueueは3 packet（通常約180 ms、120ms混合音声では約360ms）で、遅い受信者では新しい音声packetもdropされます。

`bitrate` は `4000..32000` bps を受け付けます。`audio_config` は `sample_rate: 8000`, `channels: 1`, `frame_ms: 60` または `120` に対応します。`role: "logger"` と `audio_formats` に8 kHz mono / 120 msを含むhelloがあれば、ブラウザで同時に話している音声をサーバで1本にmixし、約6 kbps / 120 msのOpusとしてloggerへ送信します。無音時は送信しません。`mixer: "mix"` または `true` も受け付けますが、混合音声の選択条件はroleと受信可能形式です。120msを宣言しない既存クライアント向けの60ms経路も残します。

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

raw WebSocketのloggerはhelloで `{"role":"logger","mixer":"mix","audio_formats":[{"sample_rate":8000,"channels":1,"frame_ms":120}]}` を指定できます。ブラウザ音声はサーバ側で1本にmixされ、約6 kbps / 120 msのOpusとして受信できます。`audio_formats: []` なら受信音声を無効にでき、`forward_to_logger: false` でも停止できます。以前の60ms個別fan-out経路も維持します。loggerからのテレメトリと音声送信は従来のRaw WebSocketです。


### Phase 4.1 実装

ブラウザ/PWAのリアルタイム通信は `socketioxide` + `socket.io-client` を使用します。Redis adapterは使用せず、単一のサーバプロセス内でroom状態とイベントを共有します。クライアントは `/api/rooms` から事前定義されたroom一覧を取得し、Room keyやauth payloadなしで `/{room_id}` namespaceに接続します。room切替時はnamespace URLを変更して再接続します。切断時は自動再接続し、復帰後にroom状態とmap設定を再取得します。ロガー向け `/ws/audio/{room_id}` は従来のWebSocket互換経路として維持します。

地図API（`GET/PUT /api/rooms/{room_id}/map`）とテレメトリ履歴API（`GET /api/rooms/{room_id}/telemetry`）にもBearerトークンは不要です。roomの存在チェックと入力値検証は引き続き行います。既存DBと管理ツールとの互換性のためroom keyの保存項目は残しますが、ブラウザやHTTP APIのアクセス認証には使用しません。ノード間の `/_cluster` namespaceの `ECORUN_CLUSTER_TOKEN`、Discord Botの `ECORUN_DISCORD_BOT_TOKEN`、Podiumなど外部サービスの認証は従来どおり必要です。

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

## 設定

`.env.example` を参照してください。現在の環境変数は以下です。

| 変数 | デフォルト | 用途 |
| --- | --- | --- |
| `ECORUN_BIND` | `0.0.0.0:3000` | HTTP/WebSocket待受アドレス |
| `RUST_LOG` | `ecorun_logger_server=info` | ログレベル |
| `ECORUN_SERVERS` | `[]` | Webクライアントが接続するLoggerノード一覧。JSON配列で `id` / `name` / `url` / `available` を指定 |
| `ECORUN_TELEMETRY_DIR` | `./data` | ノードごとのtelemetryファイルの保存先（`<dir>/<room>/<開始時刻>Z.jsonl`。空文字で保存しない） |
| `ECORUN_DB_PATH` | `ecorun.sqlite3` | Room認証・地図・telemetryのSQLite DB |
| `ECORUN_PODIUM_ENDPOINT` | `telemetry.podium.live:8080` | Podium接続先 |
| `ECORUN_PODIUM_SERIAL` | `C3PROTO01` | Podiumのデバイス識別子 |

`ECORUN_SERVERS` の例:

```json
[
  {"id":"logger-a","name":"Logger A","url":"https://logger-a.example/","available":true},
  {"id":"logger-b","name":"Logger B","url":"https://logger-b.example/","available":true}
]
```

## 現在の機能

- Rust + Tokio + Axum の HTTP/WebSocket サーバ
- `GET /healthz`
- `GET /ws/audio/{room_id}`
- 8 kHz mono / 60 ms Opus 音声のroom fan-outと、logger向け120ms/約6kbps混合音声
- Socket.IO による複数Loggerノードへの同時接続と自動再接続
- 外部Redisを使わず、各Webクライアントが全Loggerノードへ接続し、loggerが接続したノードを選ぶ冗長構成
- Logger telemetry の受信、ノードごとのJSON Lines記録（roomごと・セッションごと）、Socket.IOによるWeb GUI配信
- Web GUIで数値telemetryを折れ線グラフ表示
- Webクライアントごとの送信Opusビットレート設定（6〜32 kbps）
- bounded queue による slow receiver 対策
- room join / leave と participant 状態
- SIGINT / SIGTERM による graceful shutdown
- SvelteKit PWA（room UI / PTT / telemetry表示）
- WebCodecs Opus のブラウザ送受信経路
- `web/build` の Rust 静的配信

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

### 冗長構成

外部Redisなどの共有サービスを必須にせず、**同じroomへ参加するWebクライアントが設定済みの全LoggerノードへSocket.IOのWebSocket接続を張る**方式です。各ノードは自分に接続したクライアントへ音声とテレメトリを配信します。ブラウザはroomごとの `logger.status` に基づき、実際にloggerが接続しているノードから音声・位置情報を受信し、そのノードへマイク音声を送ります。選択中ノードが切断された場合は直ちに別ノードを選び、接続が維持されている場合は候補のlogger接続が3秒間安定してから切り替えます。マップの変更は両ノードへ書き込み、切断中だった予備にも再接続時に現行マップを送ります。

**loggerは主サーバーと予備のうち1台の `/ws/audio/{room_id}` へ接続します。** `podium.meta` と `podium.samples` は受信ノードでPodiumへ中継され、meta名をキーとする `telemetry` に変換されてSQLiteに保存・配信されます。ロガーが送った行そのものはJSON Linesファイルにも残ります（下の「Telemetryファイル」）。サーバー間で走行ログや音声を同期する仕組みはありません。logger側の主→予備→Podium直接という切り替えに合わせてブラウザが表示先を選びます。両方にloggerが接続しない場合はその状態を表示します。

2台構成の例:

```text
Logger ──▶ 主サーバー ───▶ Podium
  └ (障害時) ─▶ 予備サーバー ─▶ Podium
Browser ──Socket.IO──▶ 主サーバー
  └──────Socket.IO──▶ 予備サーバー
         (loggerのいる方を選択)
```

例えば2台を別ホストで動かす場合、両方の `ECORUN_SERVERS` に**双方のHTTPS URLを含む同一JSON**を設定します。両方のSQLiteにroom `race` と**同じroom key**を設定してください。WebブラウザからはどちらのURLを最初に開いても全ノードへ接続します。`ECORUN_SERVERS` が空の場合は現在開いているノードのみ使います。Tailscale FunnelのHTTPS URL間はSocket.IOのWebSocketで直接接続します。

ピットでは**主・予備の両URLをブックマーク**してください。主サーバーのページを開いたままならWebSocket接続を自動で切り替えられますが、主サーバー停止後のページ再読み込みではその主サーバーのHTML/JSを取得できません。予備のブックマークから開き直す必要があります。地図の描画途中・通話・現在の受信状況は切り替えで一度途切れる場合があります。走行ログは各ノード別に保存されるので、大会後に集約します。room keyはロガーのRaw WebSocket接続には現状使いません。

Socket.IOの標準in-memory adapter自体はプロセス間でイベントを共有しません。そのため、ここではRedis adapterを有効にする設定を削除し、ブラウザの多重接続を冗長化の境界として使っています。Socket.IOは `socketioxide` と `socket.io-client` の組み合わせです。

### 音声設定

Webクライアントの送信Opusはサンプルレート、フレーム長、ビットレートをクライアントごとに設定できます。ブラウザ同士は個別Opusを配信し、logger向けだけサーバでmixして低ビットレートに再エンコードします。

Logger/Pi向けの `/ws/audio/{room_id}` は既存互換経路として残り、8 kHz mono / 60 msと120 ms（loggerへの混合音声）を扱います。

### Telemetryファイル

各ノードは、ロガーの走行をroomごと・セッションごとのJSON Linesに残します（DBを開かずに読み返したり、追記を追いかけてGrafana等へ流したりするため）。`podium.meta` を受けるたびに新しいファイルになります。

```text
<ECORUN_TELEMETRY_DIR>/<room_id>/<YYYYMMDD-HHMMSS>Z.jsonl
{"recv_ms":1791077786762,"meta":[...]}
{"recv_ms":1791077787262,"samples":[[...],[...]]}
```

`recv_ms` はロガーがつながっているノードでの受信時刻（Unix ms）で、複製先のノードでも同じ値になります。行はロガーが送ったまま（`meta` 順の値＋bitmask）です。そこから名前を付けた `podium` の行は重複になるので書きません。それ以外のtelemetry（Socket.IOの `telemetry.ingest` 等）はイベントごと `<room_id>/events.ndjson` に追記します。

### Telemetry

Loggerは次のSocket.IOイベントを対象roomのnamespaceで送信できます。

```json
{
  "source": "ecologger",
  "timestamp": "2026-10-19T01:20:00+09:00",
  "data": {
    "speed_kmh": 12.4,
    "battery_v": 3.91,
    "temperature_c": 27.2
  }
}
```

イベント名は `telemetry.ingest` です。サーバは受信時刻を付加して保存し、`telemetry` イベントとしてWebクライアントへ送ります。GUIは数値フィールドを自動検出して折れ線グラフと直近イベントを表示します。

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


### Telemetry replication / browser failover

すべてのノードで同じ `ECORUN_SERVERS` を設定し、`ECORUN_NODE_ID` は各ノードの `id` に合わせて一意に設定します。`ECORUN_CLUSTER_TOKEN` は全ノードで共通の24文字以上の秘密文字列です。room ID と Socket.IO room key も全ノードで一致させます。

```env
ECORUN_NODE_ID=main
ECORUN_CLUSTER_TOKEN=replace-with-a-shared-secret-at-least-24-chars
ECORUN_SERVERS=[{"id":"main","name":"Primary","url":"https://main.example.org","available":true},{"id":"backup","name":"Backup","url":"https://backup.example.org","available":true}]
```

クラスタは Socket.IO の専用 namespace を使い、Redis なしで双方向にテレメトリ・地図・音声を同期します。Podium メタデータ・元のサンプル列・名前付きの計測値を各ノードの SQLite に保存します。接続断から復帰すると未受信テレメトリの履歴同期を試みます。`ECORUN_SERVERS=[]` に設定すると、そのノードはクラスタ同期に参加しません。

## Discord VC bridge (optional, Rust)

The Rust server embeds the Discord voice bot. Set `ECORUN_DISCORD_BOT_TOKEN`
from the Discord Developer Portal; invite the bot with **View Channel**,
**Connect**, and **Speak** permissions. A room is disabled by default: run
`cargo run -- admin`, select the room, press `v`, then enter its voice channel
ID. Clear the value to disconnect. Changes are polled every five seconds.

```sh
export ECORUN_DISCORD_BOT_TOKEN='YOUR_BOT_TOKEN'
export ECORUN_DISCORD_NODE_ID='main' # one designated node in a redundant cluster
cargo run --release
```

`ECORUN_DISCORD_NODE_ID` defaults to `ECORUN_NODE_ID`. Set the same
`ECORUN_DISCORD_NODE_ID` on all nodes, designating only one active node for
Discord voice. Discord permits a bot account in only one voice channel per
guild, so rooms in the same guild need separate bot accounts. The bridge
converts between 8 kHz mono Opus and Discord's 48 kHz stereo PCM. It sends
Discord speech to Logger/Pi and web participants and plays Logger and web
speech in Discord; it also forwards voice events across the existing cluster.
No Python environment or companion process is required. Runtime verification
requires a real bot token and voice channel.

Room audio reaches Discord as a mix: each talker has its own decoder, so two
people talking at once are heard together. Discord speech is low-passed at
3.4 kHz before it is decimated to 8 kHz (plain averaging folds everything
above 4 kHz back into the voice band as noise).

### `/ecologger` slash command

The bot registers one guild command on start (it shows up at once). Replies are
visible only to the caller. The standby bot of a dual-bot pair does not register
it, so the pit sees one `/ecologger`.

- `/ecologger join [channel] [room]` binds the room to a voice channel (the
  caller's channel when omitted) and joins it. `room` defaults to the room this
  guild is bound to, else `ECORUN_DISCORD_DEFAULT_ROOM`. Other rooms bound in the
  same guild are unbound, since a bot can be in one voice channel per guild.
  The binding is the room's `discord_channel_id`, so it survives a restart.
- `/ecologger leave` unbinds this guild's room (the bot stays out after a restart).
- `/ecologger status` shows the room, the voice channel, how many loggers are
  connected to this node, and whether Discord audio is being relayed now.
- `/ecologger audio [level] [max_gain] [lowcut] [bitrate] [volume]` reads or
  changes the logger's voice settings (driver's voice level, gain limit, low-cut,
  bitrate, earphone volume) through `logger.config` and shows the logger's
  answer. On LTE the answer takes up to several seconds.

### `logger.config`

A pass-through for settings on the logger in a room, over `/ws/audio/{room_id}`:
`{"type":"logger.config","data":{...}}` from a non-logger goes to the room's
loggers only, and a logger's (its answer with the values it now uses) goes to
everyone else in the room. Nothing is stored. Only loggers connected to the same
node receive it.
