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

### Phase 5: メトリクス / Podium

- 内部telemetry schemaを定義
- EcoLogger由来JSONをadapterで正規化
- Podium TCP `telemetry.podium.live:8080` 接続adapterを実装
- reconnect/backoff、malformed JSON、時系列順序、source timestampを処理
- WebSocketでブラウザへpush

**完了条件:** EcoLoggerとPodium双方の値を同じUIで識別表示でき、切断復旧が確認できる。

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

音声とJSONを同一WSに載せる方式はPi互換用として残し、新規Webクライアントでは必要に応じて制御WSと音声WSを分離できるよう内部設計する。

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
