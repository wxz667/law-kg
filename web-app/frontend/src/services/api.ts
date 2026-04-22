import { ApiError } from '@/types/api';

/**
 * API 客户端配置
 */
interface ApiClientConfig {
    baseURL: string;
    timeout?: number;
}

/**
 * 请求配置选项
 */
interface RequestOptions extends RequestInit {
    params?: Record<string, string | number | undefined>;
}

/**
 * API 客户端类
 * 封装 fetch 请求，提供统一的错误处理和类型安全
 */
export class ApiClient {
    private baseURL: string;
    private timeout: number;

    constructor(config: ApiClientConfig) {
        this.baseURL = config.baseURL;
        this.timeout = config.timeout ?? 30000;
    }

    /**
     * 构建完整的 URL
     */
    private buildURL(endpoint: string, params?: Record<string, string | number | undefined>): string {
        const url = new URL(endpoint, this.baseURL);

        if (params) {
            Object.entries(params).forEach(([key, value]) => {
                if (value !== undefined) {
                    url.searchParams.append(key, String(value));
                }
            });
        }

        return url.toString();
    }

    /**
     * 处理响应
     */
    private async handleResponse<T>(response: Response): Promise<T> {
        // 处理超时
        const timeoutPromise = new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error('Request timeout')), this.timeout);
        });

        const responsePromise = (async () => {
            if (!response.ok) {
                let message = `HTTP ${response.status}: ${response.statusText}`;
                let code: string | undefined;

                try {
                    const errorData = await response.json();
                    // FastAPI 422 错误的详情在 detail 字段中
                    if (errorData.detail) {
                        if (Array.isArray(errorData.detail)) {
                            // Pydantic 验证错误，格式化显示
                            message = errorData.detail.map((d: any) =>
                                `${d.loc?.join('.')}: ${d.msg}`
                            ).join('; ');
                        } else {
                            message = typeof errorData.detail === 'string' ? errorData.detail : JSON.stringify(errorData.detail);
                        }
                    } else {
                        message = errorData.message || message;
                    }
                    code = errorData.code;
                } catch {
                    // 如果解析失败，使用默认消息
                }

                // 如果是 401 未授权错误，清除本地存储并跳转到登录页
                if (response.status === 401) {
                    localStorage.removeItem('access_token');
                    localStorage.removeItem('user_id');
                    localStorage.removeItem('username');

                    // 避免在登录页重复跳转
                    if (!window.location.pathname.includes('/login')) {
                        window.location.href = '/login';
                    }
                }

                throw new ApiError(response.status, message, code);
            }

            // 处理空响应
            const text = await response.text();
            if (!text) {
                return null as T;
            }

            try {
                return JSON.parse(text) as T;
            } catch {
                return text as T;
            }
        })();

        return Promise.race([timeoutPromise, responsePromise]);
    }

    /**
     * 获取认证头
     */
    private getAuthHeaders(): Record<string, string> {
        const token = localStorage.getItem('access_token');
        return token ? { 'Authorization': `Bearer ${token}` } : {};
    }

    /**
     * GET 请求
     */
    async get<T>(endpoint: string, options?: RequestOptions): Promise<T> {
        const url = this.buildURL(endpoint, options?.params);

        // 使用 AbortController 实现超时
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const response = await fetch(url, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    ...this.getAuthHeaders(),
                    ...options?.headers,
                },
                signal: controller.signal,
                ...options,
            });

            clearTimeout(timeoutId);
            return this.handleResponse<T>(response);
        } catch (error) {
            clearTimeout(timeoutId);
            if (error instanceof DOMException && error.name === 'AbortError') {
                throw new Error('Request timeout');
            }
            throw error;
        }
    }

    /**
     * POST 请求
     */
    async post<T>(endpoint: string, data?: unknown, options?: RequestOptions): Promise<T> {
        const url = this.buildURL(endpoint, options?.params);

        // 使用 AbortController 实现超时
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    ...this.getAuthHeaders(),
                    ...options?.headers,
                },
                body: data ? JSON.stringify(data) : undefined,
                signal: controller.signal,
            });

            clearTimeout(timeoutId);
            return this.handleResponse<T>(response);
        } catch (error) {
            clearTimeout(timeoutId);
            if (error instanceof DOMException && error.name === 'AbortError') {
                throw new Error('Request timeout');
            }
            throw error;
        }
    }

    /**
     * PUT 请求
     */
    async put<T>(endpoint: string, data?: unknown, options?: RequestOptions): Promise<T> {
        const url = this.buildURL(endpoint, options?.params);

        // 使用 AbortController 实现超时
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const response = await fetch(url, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    ...this.getAuthHeaders(),
                    ...options?.headers,
                },
                body: data ? JSON.stringify(data) : undefined,
                signal: controller.signal,
            });

            clearTimeout(timeoutId);
            return this.handleResponse<T>(response);
        } catch (error) {
            clearTimeout(timeoutId);
            if (error instanceof DOMException && error.name === 'AbortError') {
                throw new Error('Request timeout');
            }
            throw error;
        }
    }

    /**
     * DELETE 请求
     */
    async delete<T>(endpoint: string, options?: RequestOptions): Promise<T> {
        const url = this.buildURL(endpoint, options?.params);

        // 使用 AbortController 实现超时
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const response = await fetch(url, {
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json',
                    ...this.getAuthHeaders(),
                    ...options?.headers,
                },
                signal: controller.signal,
                ...options,
            });

            clearTimeout(timeoutId);
            return this.handleResponse<T>(response);
        } catch (error) {
            clearTimeout(timeoutId);
            if (error instanceof DOMException && error.name === 'AbortError') {
                throw new Error('Request timeout');
            }
            throw error;
        }
    }
}
