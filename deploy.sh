#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> 1/3 安装依赖..."
cd "$SCRIPT_DIR"
npx pnpm install --frozen-lockfile

echo "==> 2/3 获取前端产物并构建..."
npx pnpm run build

echo "==> 3/3 部署到 Cloudflare Workers..."
npx wrangler deploy

echo ""
echo "✅ 部署完成! https://pan.939826.xyz"
