# Docker 基本コマンド

## コンテナ操作

### 起動・停止
```bash
# コンテナ起動
docker-compose up

# イメージを作成してコンテナ起動
docker-compose up --build

# コンテナ停止
docker-compose down

# コンテナ停止 + データごと削除
docker-compose down -v
```

### 状態確認
```bash
# 実行中のコンテナ確認
docker ps

# 全てのコンテナ確認
docker ps -a

# ログ確認
docker-compose logs
docker-compose logs -f  # リアルタイム表示
```

## ボリューム管理

### 確認・削除
```bash
# ボリューム一覧
docker volume ls

# 特定のボリューム削除
docker volume rm <volume名>

```