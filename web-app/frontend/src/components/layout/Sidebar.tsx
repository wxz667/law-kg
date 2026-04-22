import { NavLink } from 'react-router-dom';
import { ChevronLeft, ChevronRight, Book, FileText, User, Home, Search } from 'lucide-react';
import { useLayoutStore } from '@/stores/layoutStore';

const menuItems = [
    { path: '/', icon: Home, label: '首页' },
    { path: '/provisions', icon: Book, label: '法条数据库' },
    { path: '/documents', icon: FileText, label: '文书管理' },
    { path: '/search', icon: Search, label: '高级搜索' },
    { path: '/profile', icon: User, label: '个人中心' },
];

export function Sidebar() {
    const { sidebarCollapsed, setSidebarCollapsed } = useLayoutStore();

    // 获取当前登录用户信息
    const username = localStorage.getItem('username') || '未登录';
    const userInitial = username.charAt(0).toUpperCase();

    return (
        <div
            className={`flex flex-col bg-slate-900 border-r border-slate-800 transition-all duration-300 ease-in-out ${sidebarCollapsed ? 'w-20' : 'w-64'
                } h-screen sticky top-0 shadow-xl z-50`}
        >
            {/* 顶部标题栏 */}
            <div className="flex items-center justify-between px-6 py-8 border-b border-slate-800/50">
                {!sidebarCollapsed && (
                    <div className="flex items-center gap-3">
                        <div className="w-8 h-8 bg-blue-500 rounded-lg flex items-center justify-center shadow-lg shadow-blue-500/20">
                            <Book className="w-5 h-5 text-white" />
                        </div>
                        <h1 className="text-xl font-bold text-white tracking-tight truncate">
                            法律图谱
                        </h1>
                    </div>
                )}
                <button
                    onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
                    className="p-2 rounded-xl bg-slate-800 text-slate-400 hover:text-white hover:bg-slate-700 transition-all duration-200 shadow-inner flex-shrink-0"
                    title={sidebarCollapsed ? '展开菜单' : '收起菜单'}
                >
                    {sidebarCollapsed ? (
                        <ChevronRight className="w-5 h-5" />
                    ) : (
                        <ChevronLeft className="w-5 h-5" />
                    )}
                </button>
            </div>

            {/* 导航菜单 */}
            <nav className="flex-1 px-4 py-8 space-y-2 overflow-y-auto custom-scrollbar">
                {menuItems.map((item) => (
                    <NavLink
                        key={item.path}
                        to={item.path}
                        className={({ isActive }) =>
                            `group flex items-center gap-4 px-4 py-3.5 rounded-xl transition-all duration-300 ${isActive
                                ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20'
                                : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800'
                            }`
                        }
                        title={sidebarCollapsed ? item.label : undefined}
                    >
                        <item.icon className={`w-5 h-5 flex-shrink-0 transition-transform duration-300 group-hover:scale-110 ${sidebarCollapsed ? 'mx-auto' : ''
                            }`} />
                        {!sidebarCollapsed && (
                            <span className="font-semibold text-[15px] tracking-wide">{item.label}</span>
                        )}
                    </NavLink>
                ))}
            </nav>

            {/* 底部信息栏 */}
            {!sidebarCollapsed && (
                <div className="px-6 py-6 border-t border-slate-800/50 bg-slate-900/50">
                    <div className="flex items-center gap-3 px-4 py-3 bg-slate-800/50 rounded-xl border border-slate-700/50">
                        <div className="w-8 h-8 rounded-full bg-gradient-to-tr from-blue-500 to-indigo-500 flex items-center justify-center text-white font-bold text-xs shadow-inner">
                            {userInitial}
                        </div>
                        <div className="flex-1 min-w-0">
                            <p className="text-sm font-bold text-slate-100 truncate">{username}</p>
                            <p className="text-xs text-slate-500 truncate">在线</p>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
