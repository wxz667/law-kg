import { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { Scale, Lock, Mail, ArrowRight } from 'lucide-react';
import { apiClient } from '@/lib/api-client';

export default function Login() {
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const navigate = useNavigate();

    const handleLogin = async (e: React.FormEvent) => {
        e.preventDefault();
        try {
            const response = await apiClient.post<{ access_token: string; user_id: number; username: string }>('/auth/login', {
                username: email,  // Can be username or email
                password,
            });

            // 保存 Token
            localStorage.setItem('access_token', response.access_token);
            localStorage.setItem('user_id', response.user_id.toString());
            localStorage.setItem('username', response.username);

            alert('登录成功');
            navigate('/');
        } catch (error) {
            console.error('登录失败:', error);
            alert('登录失败，请检查用户名和密码');
        }
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-slate-50 px-4">
            <div className="max-w-md w-full bg-white rounded-[2.5rem] shadow-2xl shadow-slate-200/50 border border-slate-100 p-12 relative overflow-hidden">
                {/* 装饰背景 */}
                <div className="absolute -top-20 -right-20 w-64 h-64 bg-blue-50 rounded-full blur-3xl opacity-60 pointer-events-none"></div>
                <div className="absolute -bottom-20 -left-20 w-64 h-64 bg-emerald-50 rounded-full blur-3xl opacity-60 pointer-events-none"></div>

                <div className="relative z-10">
                    <div className="text-center mb-12">
                        <div className="inline-flex items-center justify-center w-20 h-20 bg-slate-900 text-white rounded-3xl mb-6 shadow-xl shadow-slate-900/20 transform rotate-3 hover:rotate-6 transition-transform duration-500">
                            <Scale className="w-10 h-10" />
                        </div>
                        <h1 className="text-3xl font-black text-slate-900 tracking-tight mb-3">欢迎回来</h1>
                        <p className="text-slate-500 font-medium">登录法律知识图谱系统</p>
                    </div>

                    <form onSubmit={handleLogin} className="space-y-6">
                        <div className="space-y-2">
                            <label className="text-sm font-bold text-slate-700 ml-2">电子邮箱</label>
                            <div className="relative group">
                                <Mail className="w-5 h-5 text-slate-400 absolute left-5 top-1/2 transform -translate-y-1/2 group-focus-within:text-blue-500 transition-colors" />
                                <input
                                    type="email"
                                    value={email}
                                    onChange={(e) => setEmail(e.target.value)}
                                    className="w-full pl-14 pr-6 py-4 bg-slate-50 border-2 border-slate-100 rounded-2xl focus:border-blue-500 focus:bg-white focus:ring-4 focus:ring-blue-500/10 transition-all outline-none text-slate-700 font-medium placeholder:text-slate-400"
                                    placeholder="name@example.com"
                                    required
                                />
                            </div>
                        </div>

                        <div className="space-y-2">
                            <div className="flex items-center justify-between ml-2">
                                <label className="text-sm font-bold text-slate-700">密码</label>
                                <a href="#" className="text-xs font-bold text-blue-600 hover:text-blue-700 transition-colors">忘记密码？</a>
                            </div>
                            <div className="relative group">
                                <Lock className="w-5 h-5 text-slate-400 absolute left-5 top-1/2 transform -translate-y-1/2 group-focus-within:text-blue-500 transition-colors" />
                                <input
                                    type="password"
                                    value={password}
                                    onChange={(e) => setPassword(e.target.value)}
                                    className="w-full pl-14 pr-6 py-4 bg-slate-50 border-2 border-slate-100 rounded-2xl focus:border-blue-500 focus:bg-white focus:ring-4 focus:ring-blue-500/10 transition-all outline-none text-slate-700 font-medium placeholder:text-slate-400"
                                    placeholder="••••••••"
                                    required
                                />
                            </div>
                        </div>

                        <button
                            type="submit"
                            className="w-full py-4 bg-slate-900 text-white rounded-2xl font-bold text-lg hover:bg-slate-800 transition-all duration-300 shadow-xl shadow-slate-900/20 flex items-center justify-center gap-3 group mt-8"
                        >
                            <span>立即登录</span>
                            <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                        </button>
                    </form>

                    <div className="mt-10 text-center">
                        <p className="text-slate-500 font-medium">
                            还没有账号？{' '}
                            <Link to="/register" className="text-blue-600 font-bold hover:text-blue-700 transition-colors">
                                立即注册
                            </Link>
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
}
