# Dev Container

このプロジェクトは、RustサーバとSvelte/Vite Webクライアントを同じDev Containerで開発できます。

## 起動

VS Code / GitHub Codespacesなどで `.devcontainer/devcontainer.json` を使ってコンテナを作成してください。

初回作成時に `web` の依存関係を `pnpm install` します。

## サーバを起動

WebをビルドしてからRustサーバを起動します。

```bash
cd web
pnpm build
cd ..
cargo run
```

ブラウザから `http://localhost:3000`、ヘルスチェックは `http://localhost:3000/healthz` です。

## Web開発

Webだけをホットリロードする場合:

```bash
cd web
pnpm dev --host 0.0.0.0
```

Viteの開発サーバは `5173` 番ポートで公開されます。

## 推奨する確認

```bash
cargo check
cd web && pnpm build
```
