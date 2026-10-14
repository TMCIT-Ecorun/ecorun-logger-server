# 2026-10-14 Phase 5（一部）Podium中継 実装メモ

依頼：EcoLoggerのテレメトリを既存の `/ws/audio/{room_id}`（Socket.IOではない方）で受け、Podium Connectへ流す。
ロガー側のPodium直接続をやめさせ、通信量が気になる場合は数件まとめて送る。

## 実装

- `src/podium.rs`：roomごとにPodium ConnectとのTCPセッションを1本持つ中継。
  - 認証・`meta` の送信・`t` の採番・`hb` の受信・切断時の再接続（1→30秒の指数バックオフ）をサーバ側で行う。
  - ストリーミングキーは環境変数 `ECORUN_PODIUM_KEYS`（room → key）だけに置く。ロガーとの間では流さない。
  - ロガーが60秒黙ったらPodiumを切断し、次のデータで再接続する。
  - Podiumが遅いときは新しいバッチを捨てる（遅延をためない）。`meta` は捨てない。
- `src/main.rs`：`podium.meta` / `podium.samples` の受信、hello の `receive_audio:false`（通話音声を流さない）を追加。
- `serde_json` に `preserve_order` を付けた。Podiumへ送るJSONのキー順を、実測で認証が通った公式アプリと同じにするため
  （無いとキーがアルファベット順に並び替わる）。

## Podium側の仕様（2026-10-04実測、次世代Podium）

- `telemetry.podium.live:8080`、暗号化なしTCP、1行JSON＋`\n`、ASCIIのみ。
- 認証 `{"cmd":{"auth":{"deviceId":KEY,"apiVer":1.1,"device":"RC_APP (RCPMK3)","ver":"2.19.0 (2.16.1)","sn":"C3PROTO01"}}}`。
  旧形式（`schemaVer:2`）は `lt` が返らない。キーが無効なときは返事なしで約1秒後に切断。
- **GNSS由来の `Utc` が入るまでストリームを表示しない**（2026-10-13にPodiumのDiscordで確認）。測位待ちはロガー側で扱う。

## 検証

- `cargo test`：検証関数の単体テスト2件。
- `cargo run --bin podium_relay_test`：偽Podiumに対し、認証（キー順含む）→meta→5行バッチが `t=0..4` で届く→`hb` が
  `podium.status` としてroomへ返る→不正なバッチが `podium_invalid` で拒否される、を確認。
- EcoCapture-Web（UNO Q側）の実コードからこのサーバ経由で偽Podiumへ40行送り、40行とも連番で届き、
  ロガー側に `podium.status`（`lt`）が返ることを確認。
- **実Podium・実機（UNO Q、SIM7070G、Tailscale Funnel）では未確認。**

## 次の作業

- サーバを動かす環境で `ECORUN_PODIUM_KEYS` を設定し、UNO QからWi-Fi→LTEの順で実Podiumへ通す。
- サーバ側のログ保存（テレメトリ＋音声。音声は保存しないか1ファイルにまとめる）。
- 中継の受け口に認証が無い（Phase 6）。roomのURLを知っていれば誰でもそのroomのキーでPodiumへ送れる。
