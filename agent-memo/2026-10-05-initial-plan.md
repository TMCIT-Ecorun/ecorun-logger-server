# 2026-10-05 初期計画メモ

## 確認したこと

- 移行先 `/home/maruo/ドキュメント/programs/ecorun-logger-server` は空に近いGitリポジトリだった。
- 現行実装は `/home/maruo/デスクトップ/ecorun2026/server/server.py`。
- 現行 `/ws/audio` はbinary音声を受け、8 kHz mono Opus / 60 msを扱う。
- 現行サーバはDiscord VCを音声ハブとして使い、8 kHz mono ↔ 48 kHz stereoの変換を行っている。
- クライアントはSIM7070G経由のWebSocket、PTT、8 kbps Opusを使う。
- `ecorun2026` 側は `master...origin/master`、直近コミットは `5146878 Initial commit`。
- 事前に想定していた `podium_connect_notes.md` と `hardware_share_20261004_podium.md` は今回のローカル環境では見つからなかった。
- `2026-logger-frontpanel` というパスも今回のリポジトリ内には存在せず、現物仕様は別場所/別repoの可能性がある。

## 判断

- 最初の互換境界として `/ws/audio` を維持する。
- Discordは新アーキテクチャの音声経路から外す。
- Rust + Tokio + Axum + Serde + tracingを基本候補にする。
- Web/PWAはSvelteKitを第一候補。
- 多人数音声はまずサーバfan-out、サーバミキシングは後回し。
- Podiumは専用adapterに隔離し、実資料確認後にschemaを固定する。
- Tailscale Funnelは公開入口とし、アプリ側認証・Origin/サイズ/接続制限を別途設ける。

## 次にやること

- 現行Pi/フロントパネルの実際のprotocol仕様とtelemetry JSONを回収する。
- PodiumのTCP実データを採取してfixture化する。
- Rust crate雛形とcodec/WS互換テストを作る。
- その実装が一段落したら、このメモを更新してコミットする。
