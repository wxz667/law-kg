#!/bin/bash
# 法律知识图谱Web应用一键部署脚本
# 使用方法: sudo bash deploy.sh

set -e

echo "🚀 开始部署法律知识图谱Web应用..."

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 检查是否为root
if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}请使用root权限运行此脚本${NC}"
  exit 1
fi

# 1. 安装Docker
echo -e "${YELLOW}[1/7] 安装Docker...${NC}"
if ! command -v docker &> /dev/null; then
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  rm get-docker.sh
fi

# 2. 安装Docker Compose
echo -e "${YELLOW}[2/7] 安装Docker Compose...${NC}"
if ! command -v docker compose &> /dev/null; then
  apt update
  apt install -y docker-compose-plugin
fi

# 3. 启动数据库服务
echo -e "${YELLOW}[3/7] 启动数据库服务...${NC}"
docker compose up -d

# 4. 等待服务启动
echo -e "${YELLOW}[4/7] 等待服务启动（60秒）...${NC}"
sleep 60

# 5. 验证服务
echo -e "${YELLOW}[5/7] 验证服务状态...${NC}"
echo "数据库服务状态:"
docker compose ps

# 6. 安装Node.js
echo -e "${YELLOW}[6/7] 安装Node.js...${NC}"
if ! command -v node &> /dev/null; then
  curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
  apt install -y nodejs
fi

# 7. 构建前端
echo -e "${YELLOW}[7/7] 构建前端应用...${NC}"
cd frontend
npm install
npm run build
cd ..

echo -e "${GREEN}✅ 部署完成！${NC}"
echo ""
echo "📋 服务访问地址:"
echo "  - 前端: http://$(curl -s ifconfig.me):5173"
echo "  - 后端API: http://$(curl -s ifconfig.me):8000/docs"
echo "  - Neo4j: http://$(curl -s ifconfig.me):7474"
echo "  - Elasticsearch: http://$(curl -s ifconfig.me):9200"
echo ""
echo "⚠️  请记得配置防火墙和Nginx反向代理！"
