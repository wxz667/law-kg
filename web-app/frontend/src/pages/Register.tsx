import { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { Scale, Lock, Mail, User, ArrowRight } from 'lucide-react';
import { apiClient } from '@/lib/api-client';

export default function Register() {
    const [username, setUsername] = useState('');
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const navigate = useNavigate();

    const handleRegister = async (e: React.FormEvent) => {
        e.preventDefault();
        try {
            await apiClient.post('/auth/register', {
                username,
                email,
                password,
                display_name: username,
            });

            alert('注册成功，请登录');
            navigate('/login');
        } catch (error: any) {
            console.error('注册失败:', error);
            let message = '请检查输入信息';
            if (error?.status === 400 || error?.status === 422) {
                message = error.message || '输入信息格式错误';
            } else if (error?.status === 409 || error?.status === 500) {
                message = error.message || '该邮箱已被注册或服务器错误';
            }
            alert(`注册失败: ${message}`);
        }
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-slate-50 px-4 py-12">
            <div className="max-w-md w-full bg-white rounded-[2.5rem] shadow-2xl shadow-slate-200/50 border border-slate-100 p-12 relative overflow-hidden">
                {/* 装饰背景 */}
                <div className="absolute -top-20 -right-20 w-64 h-64 bg-violet-50 rounded-full blur-3xl opacity-60 pointer-events-none"></div>
                <div className="absolute -bottom-20 -left-20 w-64 h-64 bg-blue-50 rounded-full blur-3xl opacity-60 pointer-events-none"></div>

                <div className="relative z-10">
                    <div className="text-center mb-10">
                        <div className="inline-flex items-center justify-center w-20 h-20 bg-blue-600 text-white rounded-3xl mb-6 shadow-xl shadow-blue-600/20 transform -rotate-3 hover:-rotate-6 transition-transform duration-500">
                            <Scale className="w-10 h-10" />
                        </div>
                        <h1 className="text-3xl font-black text-slate-900 tracking-tight mb-3">创建账号</h1>
                        <p className="text-slate-500 font-medium">开启您的法律知识探索之旅</p>
                    </div>

                    <form onSubmit={handleRegister} className="space-y-5">
                        <div className="space-y-2">
                            <label className="text-sm font-bold text-slate-700 ml-2">用户名</label>
                            <div className="relative group">
                                <User className="w-5 h-5 text-slate-400 absolute left-5 top-1/2 transform -translate-y-1/2 group-focus-within:text-blue-500 transition-colors" />
                                <input
                                    type="text"
                                    value={username}
                                    onChange={(e) => setUsername(e.target.value)}
                                    className="w-full pl-14 pr-6 py-4 bg-slate-50 border-2 border-slate-100 rounded-2xl focus:border-blue-500 focus:bg-white focus:ring-4 focus:ring-blue-500/10 transition-all outline-none text-slate-700 font-medium placeholder:text-slate-400"
                                    placeholder="您的称呼"
                                    required
                                />
                            </div>
                        </div>

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
                            <label className="text-sm font-bold text-slate-700 ml-2">设置密码</label>
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
                            className="w-full py-4 bg-blue-600 text-white rounded-2xl font-bold text-lg hover:bg-blue-700 transition-all duration-300 shadow-xl shadow-blue-600/20 flex items-center justify-center gap-3 group mt-8"
                        >
                            <span>立即注册</span>
                            <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                        </button>
                    </form>

                    <div className="mt-8 text-center">
                        <p className="text-slate-500 font-medium">
                            已有账号？{' '}
                            <Link to="/login" className="text-blue-600 font-bold hover:text-blue-700 transition-colors">
                                返回登录
                            </Link>
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
}
