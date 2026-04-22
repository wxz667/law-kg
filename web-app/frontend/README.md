# 法律知识图谱前端 (kg-web)

法律知识图谱系统的 Web 前端应用，提供法条浏览、法律知识图谱可视化、文书编写、智能推荐等功能。

## 技术栈

- **前端框架**: React 18 + TypeScript
- **构建工具**: Vite 5
- **样式方案**: TailwindCSS 4
- **状态管理**: Zustand
- **路由**: React Router 7
- **HTTP 客户端**: Axios
- **富文本编辑器**: TipTap 3 (支持表格)
- **图谱可视化**: @antv/g6 5
- **UI 图标**: lucide-react
- **表单管理**: react-hook-form + zod
- **日期处理**: date-fns

## 环境要求

- Node.js 18+
- npm 9+ 或 pnpm 8+
- 后端服务运行在 `http://localhost:8000`

## 快速开始

### 1. 安装依赖

```bash
cd frontend

# 使用 npm
npm install

# 或使用 pnpm
pnpm install
```

### 2. 配置环境变量

复制示例配置文件：

```bash
cp .env.example .env
```

编辑 `.env` 文件，配置 API 地址：

```env
# 后端 API 地址
VITE_API_BASE_URL=http://localhost:8000
```

### 3. 启动开发服务器

```bash
npm run dev
```

应用将在 `http://localhost:5173` 启动。

支持热更新 (HMR)，修改代码后自动刷新。

### 4. 构建生产版本

```bash
npm run build
```

构建产物将输出到 `dist/` 目录。

### 5. 预览生产构建

```bash
npm run preview
```

## 项目结构

```
frontend/
├── src/
│   ├── main.tsx                # React 应用入口
│   ├── App.tsx                 # 主组件 (路由配置)
│   ├── App.css                 # 全局样式
│   ├── index.css               # Tailwind CSS 导入
│   ├── components/
│   │   ├── ui/                 # 通用 UI 组件
│   │   │   ├── Modal.tsx       # 模态框
│   │   │   └── Toast.tsx       # 通知组件
│   │   └── layout/             # 布局组件
│   │       └── Sidebar.tsx     # 侧边栏
│   ├── features/
│   │   └── provision/          # 法条功能模块
│   │       └── components/
│   │           ├── KnowledgeGraph.tsx      # 单条法条图谱
│   │           ├── LawKnowledgeGraph.tsx   # 法律知识体系图谱
│   │           ├── ProvisionSearch.tsx     # 法条搜索
│   │           └── ProvisionTree.tsx       # 法条树形结构
│   ├── pages/
│   │   ├── Home.tsx            # 首页
│   │   ├── Search.tsx          # 全局搜索页
│   │   ├── Provisions/
│   │   │   ├── ProvisionsPage.tsx     # 法条数据库页
│   │   │   └── LawDetail.tsx          # 法律详情页
│   │   ├── Documents/
│   │   │   ├── DocumentList.tsx       # 文书列表
│   │   │   └── DocumentEditor.tsx     # 文书编辑器
│   │   └── Profile.tsx         # 个人中心
│   ├── services/
│   │   ├── index.ts            # API 服务封装
│   │   └── api.ts              # API 类型定义
│   ├── stores/
│   │   ├── auth-store.ts       # 认证状态
│   │   ├── document-store.ts   # 文档状态
│   │   └── provision-store.ts  # 法条状态
│   ├── hooks/
│   │   └── use-auth.ts         # 认证相关 Hooks
│   ├── lib/
│   │   └── api-client.ts       # HTTP 客户端配置
│   └── types/
│       └── api.ts              # API 类型定义
├── public/                     # 静态资源
│   └── vite.svg
├── .env                        # 环境变量 (不提交到 Git)
├── .env.example                # 环境变量示例
├── package.json                # 项目配置
├── tsconfig.json               # TypeScript 配置
├── vite.config.ts              # Vite 配置
├── tailwind.config.js          # Tailwind CSS 配置
└── postcss.config.js           # PostCSS 配置
```

## 功能模块

### 1. 法条数据库 (`/provisions`)

- **树形层级结构浏览**
  - 展示法律的章节、条款层级结构
  - 支持展开/折叠
  - 高亮当前选中节点

- **法条详情查看**
  - 显示法条完整内容
  - 展示法条层级信息
  - 支持复制法条内容

- **知识图谱可视化**
  - 单条法条关联关系图谱 (KnowledgeGraph)
  - 法律知识体系图谱 (LawKnowledgeGraph)
  - 使用 @antv/g6 渲染
  - 支持节点拖拽、缩放
  - 展示法条之间的引用关系

- **法条搜索**
  - 关键词搜索
  - 支持分页
  - 实时搜索结果

- **法条收藏**
  - 收藏常用法条
  - 快速访问收藏列表

### 2. 文书编写 (`/documents`)

- **文书管理**
  - 文书列表展示
  - 创建/编辑/删除文书
  - 自动保存状态

- **富文本编辑器**
  - 基于 TipTap 3
  - 支持表格编辑
  - 支持标题、列表、引用等格式
  - 实时预览

- **智能法条引用助手**
  - **法条搜索面板**
    - 实时搜索法条
    - 分页展示结果
    - 一键插入到文书
  
  - **法条推荐面板**
    - 基于文档内容智能推荐
    - 推荐相关法条
    - 一键插入到文书

- **法条插入模式**
  - **光标处插入**: 在当前光标位置插入法条
  - **文末附件**: 在文档末尾作为附件插入（带分隔线）

### 3. 全局搜索 (`/search`)

- 全文检索
- 多字段筛选
- 搜索结果高亮
- 分页展示

### 4. 用户系统

- **认证功能**
  - 用户注册
  - 用户登录
  - JWT Token 认证
  - 自动刷新 Token

- **个人中心**
  - 查看用户信息
  - 编辑个人资料
  - 查看搜索历史
  - 系统数据统计

## API 集成

### 配置后端地址

在 `.env` 中设置：

```env
VITE_API_BASE_URL=http://localhost:8000
```

### 使用 API 客户端

```typescript
import { apiClient, documentApi, provisionApi } from '@/lib/api-client';

// 获取文书列表
const documents = await documentApi.list(apiClient);

// 创建文书
const doc = await documentApi.create(apiClient, { 
  title: '新文书',
  content: '<p>文书内容</p>'
});

// 插入法条
await documentApi.insertProvision(apiClient, docId, {
  provision_id: 'article:xxx',
  mode: 'cursor'  // 'cursor': 光标处插入 | 'append': 文末附件
});

// 获取法条推荐
const recommendations = await recommendationApi.get(apiClient, docId);

// 搜索法条
const results = await provisionApi.search(apiClient, {
  q: '关键词',
  page: 1,
  page_size: 10
});
```

### API 响应处理

```typescript
try {
  const data = await documentApi.get(apiClient, docId);
  console.log('文书数据:', data);
} catch (error: any) {
  console.error('请求失败:', error.response?.data?.detail || error.message);
  // 显示错误提示
}
```

## 富文本编辑器

使用 TipTap 3 编辑器，支持 HTML 格式内容。

### 插入法条

```typescript
// 法条插入使用 HTML 格式
const htmlSnippet = `<br><br>【引用法条】XXX法 第一条<br>法条内容...<br>`;
editor.commands.insertContent(htmlSnippet);

// 文末附件模式（带分隔线）
const appendixSnippet = `
  <br><br>
  <hr style="border-top: 2px dashed #ccc; margin: 20px 0;">
  <br>【引用法条】XXX法 第一条<br>
  <hr style="border-top: 2px dashed #ccc; margin: 20px 0;">
  <br>法条内容...<br>
`;
editor.commands.insertContent(appendixSnippet);
```

### 编辑器配置

```typescript
import { useEditor, EditorContent } from '@tiptap/react';
import StarterKit from '@tiptap/starter-kit';
import { Table, TableCell, TableHeader, TableRow } from '@tiptap/extension-table';

const editor = useEditor({
  extensions: [
    StarterKit,
    Table.configure({ resizable: true }),
    TableRow,
    TableHeader,
    TableCell,
  ],
  content: '<p>初始内容</p>',
  editorProps: {
    attributes: {
      class: 'prose prose-sm max-w-none px-8 py-10',
    },
  },
});
```

## 知识图谱可视化

使用 @antv/g6 5 进行图谱渲染。

### 基本用法

```typescript
import { Graph } from '@antv/g6';

const graph = new Graph({
  container: containerRef.current,
  autoFit: 'view',
  data: {
    nodes: [
      { id: 'node-1', label: '法条A', x: 100, y: 100 },
      { id: 'node-2', label: '法条B', x: 200, y: 200 },
    ],
    edges: [
      { source: 'node-1', target: 'node-2', label: '引用' },
    ],
  },
  layout: { type: 'preset' },
});

graph.render();
```

### 图谱数据格式

```typescript
interface GraphData {
  nodes: Array<{
    id: string;
    label: string;
    level: string;
    type: string;
    content?: string;
    x?: number;
    y?: number;
  }>;
  edges: Array<{
    source: string;
    target: string;
    label: string;
  }>;
}
```

## 状态管理

使用 Zustand 进行状态管理。

### 创建 Store

```typescript
import { create } from 'zustand';

interface AuthStore {
  token: string | null;
  user: User | null;
  setToken: (token: string) => void;
  logout: () => void;
}

export const useAuthStore = create<AuthStore>((set) => ({
  token: null,
  user: null,
  setToken: (token) => set({ token }),
  logout: () => set({ token: null, user: null }),
}));
```

### 使用 Store

```typescript
import { useAuthStore } from '@/stores/auth-store';

function UserProfile() {
  const { user, logout } = useAuthStore();
  
  return (
    <div>
      <p>欢迎, {user?.username}</p>
      <button onClick={logout}>退出登录</button>
    </div>
  );
}
```

## 开发指南

### 添加新页面

1. 在 `src/pages/` 创建页面组件
2. 在 `src/App.tsx` 中配置路由
3. 在侧边栏导航菜单中添加链接

示例：

```typescript
// src/App.tsx
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { NewPage } from './pages/NewPage';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/new" element={<NewPage />} />
      </Routes>
    </BrowserRouter>
  );
}
```

### 添加新的 API 接口

1. 在 `src/types/api.ts` 定义类型
2. 在 `src/services/index.ts` 添加 API 方法
3. 在组件中使用 `apiClient` 调用

示例：

```typescript
// src/types/api.ts
export interface NewApiOut {
  id: string;
  name: string;
}

// src/services/index.ts
export const newApi = {
  list: async (client: ApiClient): Promise<NewApiOut[]> => {
    return client.get<NewApiOut[]>('/new-api');
  },
};
```

### 代码规范

- ESLint 检查：`npm run lint`
- TypeScript 类型检查：`npm run build`
- 遵循 React Hooks 最佳实践
- 组件使用函数式组件 + Hooks
- 使用 Tailwind CSS 进行样式编写

## 常见问题

### 前端启动失败

1. **端口被占用**: 修改 `vite.config.ts` 中的 `server.port`
2. **Node 版本过低**: 确保 Node.js >= 18
3. **依赖安装失败**: 删除 `node_modules` 和 `package-lock.json` 后重新 `npm install`

### API 请求失败

1. **后端未启动**: 确保后端服务运行在 `http://localhost:8000`
2. **CORS 错误**: 检查后端 `.env` 中的 `CORS_ORIGINS` 配置
3. **401 错误**: 需要登录，检查 Token 是否有效
4. **网络错误**: 检查 `.env` 中的 `VITE_API_BASE_URL` 配置

### 图谱不显示

1. **后端返回空数据**: 检查 Neo4j 中是否有对应数据
2. **ID 不匹配**: 确保树形结构和图谱使用相同的 ID 格式
3. **G6 渲染问题**: 检查浏览器控制台错误信息
4. **容器尺寸为 0**: 确保图谱容器有明确的宽高

### 法条插入格式错误

确保后端返回的是 HTML 格式（`<br>` 标签），而不是纯文本（`\n`）。

### 编辑器内容丢失

- 检查是否正确保存了内容
- 确保 `editor.commands.setContent()` 被正确调用
- 检查浏览器控制台的错误信息

## 构建和部署

### 生产构建

```bash
npm run build
```

构建产物输出到 `dist/` 目录。

### 部署

将 `dist/` 目录部署到静态文件服务器（如 Nginx、Apache、Vercel、Netlify 等）。

Nginx 配置示例：

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    root /var/www/kg-web/dist;
    index index.html;
    
    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

## 浏览器支持

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## 后端依赖

本项目依赖 [kg-api](../backend/api/kg-api) 后端服务，启动前请确保后端服务已运行。

## 许可证

本项目为毕业项目，仅供学习研究使用。
