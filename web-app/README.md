# 法律知识图谱 Web 应用

基于法律图谱的知识库与文书管理系统，提供法条检索、智能推荐、文书编辑等功能。

## 📋 目录结构

```
web-app/
├── backend/           # 后端 API 服务
│   └── api/kg-api/    # FastAPI 应用
├── frontend/          # 前端 React 应用
├── infra/             # 基础设施配置
│   ├── neo4j/         # Neo4j 图数据库
│   ├── postgres/      # PostgreSQL 关系数据库
│   └── elasticsearch/ # Elasticsearch 搜索引擎
├── data/              # 数据目录
├── compose.yaml       # Docker Compose 配置
└── .env.example       # 环境变量示例
```

## 🛠️ 技术栈

### 后端
- **FastAPI** - Python Web 框架
- **PostgreSQL** - 用户数据和文档存储
- **Neo4j** - 法律知识图谱存储
- **Elasticsearch** - 法条全文检索
- **Pydantic** - 数据验证
- **SQLAlchemy** - ORM 框架
- **JWT** - 身份认证

### 前端
- **React 18** - UI 框架
- **TypeScript** - 类型安全
- **Vite** - 构建工具
- **Tailwind CSS** - 样式框架
- **Tiptap** - 富文本编辑器
- **React Router** - 路由管理
- **Zustand** - 状态管理
- **Axios** - HTTP 客户端
- **Lucide React** - 图标库

## 🚀 快速开始

### 前置要求

- **Node.js** >= 18
- **Python** >= 3.10
- **Docker & Docker Compose** (可选，用于数据库服务)

### 安装依赖

#### 1. 安装基础设施（推荐）

```bash
# 使用 Docker Compose 启动数据库服务
cd web-app
docker compose up -d
```

这将启动：
- PostgreSQL (端口 5432)
- Neo4j (端口 7687)
- Elasticsearch (端口 9200)

#### 2. 配置环境变量

```bash
# 复制示例配置文件
cp .env.example .env
```

编辑 `.env` 文件，填入实际的配置：

```ini
# 数据库配置
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=law_kg

# Neo4j 配置
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password

# Elasticsearch 配置
ELASTICSEARCH_HOST=http://localhost:9200

# AI 推荐配置（可选）
AI_PROVIDER=qwen
QWEN_API_KEY=your_api_key
QWEN_MODEL=qwen-plus
QWEN_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1

# JWT 配置
SECRET_KEY=your-secret-key
```

### 启动后端

```bash
# 进入后端目录
cd backend/api/kg-api

# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# 安装依赖
pip install -e .

# 启动服务
PYTHONPATH=src uvicorn kg_api.main:app --reload --host 0.0.0.0 --port 8000
```

后端服务将运行在 `http://localhost:8000`

### 启动前端

```bash
# 进入前端目录
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev
```

前端应用将运行在 `http://localhost:5173`

## 📖 API 文档

启动后端服务后，访问：
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## 🔧 开发命令

### 后端

```bash
# 安装依赖
pip install -e .

# 开发模式运行
PYTHONPATH=src uvicorn kg_api.main:app --reload

# 生产模式运行
PYTHONPATH=src uvicorn kg_api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### 前端

```bash
# 安装依赖
npm install

# 开发模式
npm run dev

# 构建生产版本
npm run build

# 代码检查
npm run lint

# 预览生产版本
npm run preview
```

## 📦 主要功能

### 文书管理
- 创建/编辑/删除法律文书
- 富文本编辑器支持
- 文书类型分类（通用文书、起诉状、判决书、意见书）
- 状态管理（草稿、已发布）
- 搜索和筛选

### 知识图谱
- 法条搜索和浏览
- 图谱可视化
- 关系查询
- 智能推荐法条

### 法条引用
- 智能法条推荐（基于 AI）
- 法条搜索
- 一键插入法条到文书
- 自动格式化法条引用

### 用户系统
- 用户注册/登录
- JWT 认证
- 个人中心
- 系统数据概览

## 🗄️ 数据库初始化

### PostgreSQL 表结构

```sql
-- 用户表
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    display_name VARCHAR(100),
    avatar_url VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 用户文书表
CREATE TABLE user_documents (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title VARCHAR(200) NOT NULL,
    content TEXT,
    doc_type VARCHAR(50) DEFAULT '通用文书',
    status VARCHAR(50) DEFAULT 'draft',
    provisions JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Neo4j 图谱节点

- **DocumentNode** - 法律文档节点
- **ProvisionNode** - 法条节点
- **Relation** - 关系节点

## 🤖 AI 推荐功能

系统支持接入通义千问（Qwen）大模型，提供智能法条推荐：

1. 在 `.env` 中配置 AI 提供商信息
2. 系统根据文书内容自动分析并推荐相关法条
3. 支持多种插入模式（光标处插入、文末附件）

## 🐳 Docker 部署

```bash
# 构建并启动所有服务
docker compose up -d

# 查看服务状态
docker compose ps

# 查看日志
docker compose logs -f

# 停止服务
docker compose down
```

## 📝 环境变量说明

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `POSTGRES_HOST` | PostgreSQL 主机 | localhost |
| `POSTGRES_PORT` | PostgreSQL 端口 | 5432 |
| `POSTGRES_USER` | PostgreSQL 用户名 | postgres |
| `POSTGRES_PASSWORD` | PostgreSQL 密码 | - |
| `POSTGRES_DB` | PostgreSQL 数据库名 | law_kg |
| `NEO4J_URI` | Neo4j 连接 URI | bolt://localhost:7687 |
| `NEO4J_USER` | Neo4j 用户名 | neo4j |
| `NEO4J_PASSWORD` | Neo4j 密码 | - |
| `ELASTICSEARCH_HOST` | Elasticsearch 地址 | http://localhost:9200 |
| `AI_PROVIDER` | AI 提供商 | qwen |
| `QWEN_API_KEY` | 通义千问 API Key | - |
| `SECRET_KEY` | JWT 密钥 | - |

## 🐛 常见问题

### 后端启动失败
- 检查 Python 版本是否 >= 3.10
- 确保设置了 `PYTHONPATH=src`
- 检查数据库服务是否正常运行
- 验证 `.env` 配置是否正确

### 前端启动失败
- 检查 Node.js 版本是否 >= 18
- 删除 `node_modules` 后重新执行 `npm install`
- 检查端口 5173 是否被占用

### 数据库连接失败
- 确认 Docker 容器是否正常运行：`docker compose ps`
- 检查数据库连接配置
- 查看日志：`docker compose logs`
