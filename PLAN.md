# EcoRun Logger Server 移行計画

## 目的

`ecorun2026` の実験的な Python/FastAPI + Discord 音声中継を、Rust ベースの常駐サーバへ移行する。最終的には、Raspberry Pi / SIM7070G の低帯域音声、ブラウザ/PWA、通話、走行メトリクス、Podium テレメトリを同一サーバで扱い、Discord を音声経路から外す。

## 現状の確認

- 現行サーバ: `ecorun2026/server/server.py`
- 現行音声境界: WebSocket `/ws/audio`
- 音声: 8 kHz / mono / Opus / 60 ms（480 samples）
- Pi→server: Opus を受信し、48 kHz stereo PCM に展開して Discord へ渡す
- server→Pi: Discord 48 kHz stereo PCM を 8 kHz mono に落とし、60 ms Opus にして WS へ送る
- 既存クライアントは PTT 送信、8 kHz mono Opus、8 kbps 設定を使用
- 現行リポジトリ `ecorun2026` は `master...origin/master` で、今回の移行先とは別管理
- 移行先 `ecorun-logger-server` は空に近いGitリポジトリ
- 予定されていた `podium_connect_notes.md` / `hardware_share_20261004_podium.md` は今回のローカル環境では見つからなかったため、Podium の詳細は実装前に原資料で再確認する

## 基本方針

1. 既存 `/ws/audio` のPi向けプロトコルを最初の互換境界として維持する。
2. Discord は移行対象から外し、音声をサーバ内部の room/session に直接接続する。
3. Rust サーバは「音声」「テレメトリ」「セッション」「Web API」を分離したモジュール構成にする。
4. ブラウザは Svelte + PWA を第一候補とし、音声再生/送信は Web Audio API + WebSocket を基本にする。ブラウザの Opus 実装は最初の実機検証で確定する。
5. 多人数通話は、各参加者を個別ストリームとして受け、サーバで room 単位に fan-out する。必要になるまではサーバ側ミキシングを行わず、クライアント混合を優先する。
6. Podium は外部TCP接続の専用adapterとして隔離し、改行区切りJSONを内部の正規化イベントへ変換する。
7. Tailscale Funnel は公開入口として使えるが、認証・Origin・レート制限・最大接続数をアプリ側でも明示する。

## 想定構成

```text
                         +-----------------------+
                         |   Rust HTTP/WS API    |
                         | auth / rooms / health |
                         +-----------+-----------+
                                     |
                 +-------------------+-------------------+
                 |                   |                   |
          +------v------+     +------v------+     +------v------+
          | Audio Rooms |     | Telemetry   |     | Podium      |
          | fan-out     |     | aggregator  |     | TCP adapter |
          +------+------+
                 |
       +---------+---------+
       |                   |
 +-----v-----+       +-----v------+
 | Pi/SIM7070|       | Svelte/PWA |
 | /ws/audio |       | browser WS |
 +-----------+       +------------+

 Podium TCP :8080 --> Podium adapter --> normalized telemetry --> WS/API
```

## 技術候補

- Rust: stable toolchain
- Async/runtime: Tokio
- HTTP/WebSocket: Axum
- Serialization: Serde + serde_json
- Logging/observability: tracing + tracing-subscriber
- Configuration: environment + typed config
- Web: SvelteKit/PWA
- Persistence: 最初は不要な状態をインメモリ、履歴が必要になった時点でSQLite等を追加
- Opus: libopus を使うRust binding。8 kHz mono 60 ms を既存互換モードとしてテストする

## 実装フェーズ

### Phase 0: 仕様固定・互換テスト

- 現行Piクライアントから `/ws/audio` のbinary/text境界を採取
- 8 kHz mono Opus 60 ms の実データをfixture化
- WebSocket切断、再接続、欠損、遅延、無音時の期待動作を定義
- Podium の実資料/実機ログを回収し、TCPメッセージ形式と項目を確定
- 認証方式、room ID、権限モデルを決める

**完了条件:** プロトコル仕様とfixtureがGitに入り、Rust側の互換テストを書ける状態。

### Phase 1: Rustサーバ最小核

- `crates/server` または単一crateから開始
- `/healthz`、設定、tracing、Graceful shutdown
- WebSocket session管理
- `/ws/audio` の受信・送信・backpressure
- Opus codec adapter
- room/sessionの最小モデル

**完了条件:** DiscordなしでPi同士/テストクライアント間の音声往復が成立。

### Phase 2: Raspberry Pi互換運用

- 既存 `/ws/audio` クライアントを変更最小で接続
- Tailscale内接続→Funnel公開の順で検証
- 再接続、ネットワーク瞬断、CPU/メモリ、帯域を計測
- 8 kbps/60 msを基準値として遅延と品質を評価

**完了条件:** 実Piで連続運用でき、Discord依存なし。

### Phase 3: Web/PWA

- SvelteKitでログイン/接続画面
- room一覧・参加/退出・PTT
- telemetry表示
- Service Worker / installable PWA
- ブラウザ音声I/OとOpus経路を実機確認

**完了条件:** PC/スマホブラウザから同じroomで送受信できる。

### Phase 4: 多人数通話

- roomごとのparticipant registry
- participant単位の音声fan-out
- mute/PTT/接続状態
- 音量・遅延・jitter対策
- 必要ならサーバミキシングを追加

**完了条件:** 3〜5人規模で音声が安定し、1人の切断がroom全体を巻き込まない。

### Phase 4.1: Socket.IO化と分散構成対応

Phase 4で実装したブラウザ向けの独自WebSocket制御プロトコルを Socket.IO に置き換え、Rust側は `socketioxide`、ブラウザ側は公式 `socket.io-client` を使用する。複数のLoggerプロセスに接続が分散しても、同じroomの参加者状態・制御イベント・音声が共有される構成を目標とする。分散共有にはユーザー指定の `socketioxide-redis` を採用候補の第一選択とし、実装開始時に現在の `socketioxide` との互換性・提供機能を確認する。

- ブラウザは Socket.IO のイベント通信を使い、音声packetはイベントpayloadのbinaryデータとして送受信する。
- ブラウザの接続先は `io(origin + "/" + encodeURIComponent(roomId), { path: "/socket.io/" })` 相当とし、Socket.IO namespaceのパス部分をroom IDとして扱う。`/socket.io/` はSocket.IO transport endpointとして固定する。
- Loggerは接続namespaceからroom IDを決定し、接続時に対応する内部roomへ自動参加させる。クライアントから `room.join` を送ってroomを選択・変更する仕組みは廃止する。room切替は接続先namespace URLを変更して再接続する。
- `room.state`、参加者のmute/PTT状態、切断時の退出処理は、Socket.IO接続ライフサイクルとroomイベントに合わせて再定義する。
- 分散ノード間のroom参加者・制御イベント・音声配信を同期するadapterを導入する。`socketioxide-redis` のAPIと依存サービスの設定項目を実装開始時に確認する。単一プロセス内のroom状態だけで分散対応済みとはみなさない。
- 分散adapterには `socketioxide-redis` を使用し、Redis接続先、認証情報の外部設定、接続再試行、障害時のログ/health表示を設計する。crateのAPI・互換バージョン・Redis経由で伝播する対象（room状態、制御イベント、音声binary payload）を実装前に確認し、未対応のデータ経路を暗黙にローカル処理へフォールバックさせない。
- クライアントが接続先Loggerサーバを選択できるようにする。サーバ一覧はHTTP APIまたはSocket.IOイベントで取得可能にし、少なくとも識別子、接続URL、表示名、利用可否/状態を定義する。API/eventの方式、一覧の供給元（設定値または管理データ）、公開範囲と認証を実装前に確定する。秘密情報を一覧レスポンスへ含めない。
- 接続断時は `socket.io-client` の再接続機能を有効化し、指数バックオフ等の再試行間隔、最大待機時間、接続中/再接続中/失敗状態をUIに定義する。選択したサーバが復旧しない場合の別サーバへのフェイルオーバーは、同一Redis共有状態を前提に候補リストから順次試行するか、ユーザー選択を維持するかを決める。再接続後は認証・namespace/room参加・`room.state` とparticipant状態を再同期し、重複参加者や古い状態を残さない。
- Pi向け `/ws/audio/{room_id}` の既存WebSocketプロトコルは互換性境界として維持し、Socket.IO化の対象はブラウザ/PWA経路とする。
- Socket.IO接続パス、room URLの形式、イベント名とpayload、binary音声形式、エラー、再接続時の状態同期を仕様として固定する。

**完了条件:** ブラウザでサーバ一覧を取得・選択してURLでroomに参加でき、`room.join` を使わずに音声・参加者状態を送受信できる。Redis adapterを通じ複数Loggerプロセスに接続したクライアント同士でも同一room内で音声と状態が同期する。切断後の自動再接続と状態再同期が確認でき、選択サーバが復旧しない場合の動作が仕様どおりで、1接続の切断がroom全体を巻き込まない。

### Phase 5: メトリクス / Podium

- 内部telemetry schemaを定義
- EcoLogger由来JSONをadapterで正規化
- Podium TCP `telemetry.podium.live:8080` 接続adapterを実装
- reconnect/backoff、malformed JSON、時系列順序、source timestampを処理
- WebSocketでブラウザへpush

**完了条件:** EcoLoggerとPodium双方の値を同じUIで識別表示でき、切断復旧が確認できる。

2026-10-14 進捗：EcoLogger → `/ws/audio/{room_id}` → Podium Connect の中継を先行実装（`src/podium.rs`、README「Podium中継」）。
Podiumの接続仕様は次世代Podium（2026-10-04実測）に合わせた。内部telemetry schema・ブラウザ表示・ログ保存は未着手。

### Phase 6: 認証・公開・運用

- Tailscale Funnelを入口にしたHTTPS/WSS
- 認証トークンまたは短命session token
- Origin検証、接続/room上限、message size上限
- systemd等で自動起動
- structured logs / health / metrics
- secretsをGit外へ

**完了条件:** 再起動・切断・不正入力を含む運用テストを通過。

### Phase 7: 旧系撤去

- Discord音声中継を停止
- 旧Python serverをarchive扱い
- `/ws/audio` 互換層を必要期間維持後、廃止判断
- 移行手順とrollback手順をドキュメント化

## API/プロトコル案

### WebSocket

既存互換:

- `binary`: 8 kHz mono Opus audio packet
- `text`: JSON telemetry/control event

新規イベント例:

```json
{"type":"hello","protocol":2,"client_id":"..."}
{"type":"room.join","room_id":"main"}
{"type":"room.state","participants":[...]}
{"type":"telemetry","source":"podium","timestamp":"...","data":{...}}
{"type":"audio.config","sample_rate":8000,"channels":1,"frame_ms":60}
```

上記は現行の独自WebSocketプロトコルの例。Phase 4.1以降のブラウザ/PWA経路では Socket.IO のイベントに移行し、`room.join` は使用しない。roomは接続URLで指定して接続時に参加する。Pi向け `/ws/audio/{room_id}` は既存互換プロトコルを維持する。

音声とJSONを同一WSに載せる方式はPi互換用として残す。Phase 4.1以降のブラウザ経路はSocket.IOイベントで制御とbinary音声を扱う。

## 重要な設計判断

- **音声frame:** 既存互換の60 msを第一候補。ただしブラウザ経路では20/40/60 msを比較し、遅延を見て最終決定。
- **サーバ混合:** 初期は行わない。各参加者のstreamをfan-outし、クライアント側で扱う。
- **再送:** 音声は再送しない。古いpacketより現在の音声を優先し、bounded queue + drop policyを採用。
- **テレメトリ:** 最新値とsource timestampを保持し、UI向けにはサーバ受信時刻も付与。
- **永続化:** 通話の必須要件にしない。走行履歴保存が必要になった時点でDBを追加。
- **認証:** Tailscaleによるネットワーク境界だけを認証とみなさず、アプリケーション認証を入れる。

## リスク / 要検証

1. ブラウザで8 kHz mono 60 ms Opusを安定して扱えるか。
2. SIM7070G + Tailscale Funnel経路の実効遅延・切断挙動。
3. 60 ms packetとネットワークjitterの体感品質。
4. Podium実データ仕様が今回確認できていない。
5. 現行serverの単純なrepeat/decimationは音質面で最低限なので、Rust化の際にresampling品質を上げるか、互換優先にするかを実測で決める。
6. 多人数時のfan-out帯域とブラウザCPU負荷。

## 受け入れテストの骨子

- codec: fixture encode/decode、フレーム長、異常packet
- WS: connect/disconnect/reconnect、複数client、slow receiver
- audio: end-to-end latency、packet loss、jitter、PTT
- telemetry: schema validation、source ordering、reconnect
- Podium: TCP reconnect、malformed line、unknown field、burst
- security: unauthorized room、Origin、oversized frame、connection flood
- operations: systemd restart、Funnel再接続、ログ、health check

## Git運用

- 変更は `ecorun-logger-server` で管理する
- 一段落ごとに `agent-memo/` に判断・検証結果・次の作業を残す
- コミットは意味のある単位で作成し、pushはしない
- secretsやローカル環境ファイルはコミットしない

## 最初の実装順

1. Phase 0のfixture/仕様固定
2. Rustプロジェクト雛形 + codec/WSテスト
3. room fan-out
4. Pi実機互換
5. PWA
6. telemetry/Podium
7. 公開・認証・運用
8. Discord撤去
