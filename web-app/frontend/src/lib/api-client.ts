import { ApiClient } from '../services/api';

/**
 * 从环境变量读取 API 配置
 */
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

/**
 * 创建全局 API 客户端实例
 */
export const apiClient = new ApiClient({
    baseURL: API_BASE_URL,
    timeout: 60000,  // 增加到60秒，以支持大文件加载
});

/**
 * 导出 API 服务
 */
export * from '../services/index';
