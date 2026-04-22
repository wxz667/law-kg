import { useState, useEffect } from 'react';
import { User, Mail, Settings, LogOut, Database, Network, BookOpen, Clock } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { apiClient } from '@/lib/api-client';

interface UserProfile {
    id: number;
    username: string;
    email: string;
    display_name: string | null;
    avatar_url: string | null;
    created_at: string;
    updated_at: string;
}

export default function Profile() {
    const navigate = useNavigate();
    const [user, setUser] = useState<UserProfile | null>(null);
    const [loading, setLoading] = useState(true);
    const [stats, setStats] = useState({
        total_laws: 0,
        total_nodes: 0,
        total_provisions: 0,
        last_updated: '',
    });

    useEffect(() => {
        checkAuthAndLoadData();
    }, []);

    const checkAuthAndLoadData = async () => {
        const token = localStorage.getItem('access_token');
        if (!token) {
            alert('请先登录');
            navigate('/login');
            return;
        }

        try {
            await loadUserProfile();
            await loadStats();
        } catch (error) {
            console.error('加载用户信息失败:', error);
            localStorage.removeItem('access_token');
            localStorage.removeItem('user_id');
            localStorage.removeItem('username');
            alert('登录已过期，请重新登录');
            navigate('/login');
        } finally {
            setLoading(false);
        }
    };

    const loadUserProfile = async () => {
        const response = await apiClient.get<UserProfile>('/auth/me');
        setUser(response);
    };

    const loadStats = async () => {
        try {
            // 获取系统统计数据
            const data = await apiClient.get<any>('/system/stats');

            setStats({
                total_laws: data?.total_laws || 0,
                total_nodes: data?.total_nodes || 0,
                total_provisions: data?.total_provisions || 0,
                last_updated: data?.last_updated || '',
            });
        } catch (error) {
            console.error('加载系统统计数据失败:', error);
            setStats({
                total_laws: 0,
                total_nodes: 0,
                total_provisions: 0,
                last_updated: '',
            });
        }
    };

    const handleLogout = () => {
        // 清除本地存储的 token
        localStorage.removeItem('access_token');
        localStorage.removeItem('user_id');
        localStorage.removeItem('username');
        navigate('/login');
    };

    if (loading) {
        return (
            <div className="min-h-screen bg-slate-50 flex items-center justify-center">
                <div className="text-center">
                    <div className="w-16 h-16 border-4 border-indigo-600 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
                    <p className="text-slate-600 font-medium">加载中...</p>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-slate-50">
            {/* Header */}
            <div className="border-b border-slate-200 bg-white/80 backdrop-blur-md sticky top-0 z-50">
                <div className="max-w-6xl mx-auto px-8 py-8">
                    <div className="flex items-center gap-4 mb-2">
                        <div className="p-2 bg-indigo-600 rounded-lg shadow-lg shadow-indigo-600/20">
                            <Settings className="w-5 h-5 text-white" />
                        </div>
                        <h1 className="text-4xl font-black text-slate-900 tracking-tight">个人中心</h1>
                    </div>
                    <p className="text-slate-500 font-medium">管理您的个人资料、偏好设置与系统安全</p>
                </div>
            </div>

            {/* Main Content */}
            <div className="max-w-6xl mx-auto px-8 py-12 space-y-12">
                {/* Profile Card */}
                <div className="bg-white rounded-[3rem] p-12 border border-slate-200 shadow-2xl shadow-slate-200/50 relative overflow-hidden">
                    {/* Background decoration */}
                    <div className="absolute top-0 right-0 w-96 h-96 bg-indigo-50 rounded-full blur-3xl -mr-48 -mt-48 opacity-50" />

                    <div className="relative z-10 flex flex-col md:flex-row items-center md:items-start gap-12">
                        <div className="relative group">
                            <div className="w-32 h-32 rounded-[2.5rem] bg-gradient-to-tr from-indigo-600 to-blue-600 flex items-center justify-center shadow-2xl shadow-indigo-500/30 group-hover:scale-105 transition-transform duration-500">
                                <User className="w-16 h-16 text-white" />
                            </div>
                            <div className="absolute -bottom-2 -right-2 w-10 h-10 bg-emerald-500 rounded-2xl border-4 border-white flex items-center justify-center shadow-lg">
                                <div className="w-2 h-2 bg-white rounded-full animate-pulse" />
                            </div>
                        </div>

                        <div className="flex-1 text-center md:text-left">
                            <div className="flex flex-col md:flex-row md:items-center gap-4 mb-8">
                                <h2 className="text-4xl font-black text-slate-900 tracking-tight">
                                    {user?.display_name || user?.username || '未知用户'}
                                </h2>
                                <span className="inline-flex items-center px-4 py-1.5 bg-indigo-50 text-indigo-700 text-xs font-black uppercase tracking-widest rounded-full border border-indigo-100">
                                    注册用户
                                </span>
                            </div>

                            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                <div className="bg-slate-50 rounded-2xl p-6 border border-slate-100 group hover:bg-white hover:border-blue-500/30 hover:shadow-xl transition-all duration-300">
                                    <div className="flex items-center gap-3 mb-3">
                                        <User className="w-4 h-4 text-slate-400" />
                                        <label className="text-xs font-black text-slate-400 uppercase tracking-widest">用户名</label>
                                    </div>
                                    <p className="text-lg font-bold text-slate-700">{user?.username}</p>
                                </div>
                                <div className="bg-slate-50 rounded-2xl p-6 border border-slate-100 group hover:bg-white hover:border-blue-500/30 hover:shadow-xl transition-all duration-300">
                                    <div className="flex items-center gap-3 mb-3">
                                        <Mail className="w-4 h-4 text-slate-400" />
                                        <label className="text-xs font-black text-slate-400 uppercase tracking-widest">电子邮箱</label>
                                    </div>
                                    <p className="text-lg font-bold text-slate-700">{user?.email}</p>
                                </div>
                            </div>
                        </div>

                        <button onClick={handleLogout} className="flex items-center gap-2 px-6 py-3 bg-slate-900 text-white rounded-2xl font-bold hover:bg-red-600 transition-all shadow-xl shadow-slate-900/10">
                            <LogOut className="w-5 h-5" />
                            <span>安全退出</span>
                        </button>
                    </div>
                </div>

                {/* Stats Section */}
                <div>
                    <div className="flex items-center justify-between mb-8 px-4">
                        <h2 className="text-2xl font-black text-slate-900 tracking-tight">系统数据概览</h2>
                        <span className="text-sm font-bold text-slate-400 bg-white px-4 py-2 rounded-full border border-slate-100">
                            {stats.last_updated ? `更新于 ${new Date(stats.last_updated).toLocaleString('zh-CN')}` : '加载中...'}
                        </span>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                        {[
                            { label: '收录法律', value: stats.total_laws.toString(), icon: BookOpen, color: 'blue', suffix: '部' },
                            { label: '图谱节点', value: stats.total_nodes.toString(), icon: Network, color: 'emerald', suffix: '个' },
                            { label: '法条总量', value: stats.total_provisions.toString(), icon: Database, color: 'violet', suffix: '条' },
                            { label: '系统状态', value: '在线', icon: Clock, color: 'amber', suffix: '' },
                        ].map((stat, index) => (
                            <div key={index} className="group bg-white rounded-[2.5rem] p-8 border border-slate-200 hover:shadow-2xl hover:shadow-blue-500/10 transition-all duration-500 hover:-translate-y-2 relative overflow-hidden">
                                <div className={`absolute -right-10 -bottom-10 w-32 h-32 bg-${stat.color}-50 rounded-full blur-3xl opacity-0 group-hover:opacity-100 transition-opacity duration-500`} />
                                <div className={`inline-flex w-16 h-16 rounded-2xl bg-${stat.color}-50 text-${stat.color}-600 items-center justify-center mb-6 shadow-inner relative z-10 group-hover:scale-110 transition-transform duration-500`}>
                                    <stat.icon className="w-8 h-8" />
                                </div>
                                <div className="flex items-baseline gap-2 mb-2 relative z-10">
                                    <div className="text-4xl font-black text-slate-900">{stat.value}</div>
                                    {stat.suffix && <div className="text-sm font-bold text-slate-400">{stat.suffix}</div>}
                                </div>
                                <div className="text-slate-500 font-bold uppercase tracking-widest text-xs relative z-10">{stat.label}</div>
                            </div>
                        ))}
                    </div>
                </div>


            </div>
        </div>
    );
}
