import { Book, FileText, Search, Network, ArrowRight } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

export default function Home() {
    const navigate = useNavigate();

    const features = [
        {
            icon: Book,
            title: '法条数据库',
            description: '浏览完整的法律条文数据库，支持条、款、项、目的层级结构',
            path: '/provisions',
            color: 'blue',
        },
        {
            icon: FileText,
            title: '文书管理',
            description: '创建和管理法律文书，支持插入法条引用',
            path: '/documents',
            color: 'emerald',
        },
        {
            icon: Search,
            title: '高级搜索',
            description: '使用 Elasticsearch 进行全文检索，支持关键词高亮和智能建议',
            path: '/search',
            color: 'violet',
            hidden: true, // 暂时隐藏
        },
        {
            icon: Network,
            title: '知识图谱',
            description: '可视化展示法条之间的引用、解释、修订等关系',
            path: '/provisions',
            color: 'amber',
        },
    ];

    return (
        <div className="min-h-screen bg-slate-50">
            {/* Hero Section */}
            <div className="relative overflow-hidden bg-white border-b border-slate-200">
                {/* Decorative background elements */}
                <div className="absolute top-0 left-0 w-full h-full overflow-hidden pointer-events-none">
                    <div className="absolute -top-[10%] -left-[10%] w-[40%] h-[40%] bg-blue-50 rounded-full blur-3xl opacity-50" />
                    <div className="absolute top-[20%] -right-[5%] w-[30%] h-[30%] bg-indigo-50 rounded-full blur-3xl opacity-50" />
                </div>

                <div className="relative max-w-7xl mx-auto px-8 py-24 lg:py-32">
                    <div className="text-center max-w-4xl mx-auto">
                        <div className="inline-flex items-center gap-2 px-4 py-2 bg-blue-50 rounded-full text-blue-700 text-sm font-semibold mb-8 border border-blue-100 shadow-sm">
                            <span className="relative flex h-2 w-2">
                                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
                                <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500"></span>
                            </span>
                            v2.0 Legal Knowledge Graph System
                        </div>

                        <h1 className="text-5xl lg:text-7xl font-extrabold text-slate-900 mb-8 tracking-tight leading-[1.1]">
                            构建法律知识的
                            <span className="block bg-gradient-to-r from-blue-600 via-indigo-600 to-violet-600 bg-clip-text text-transparent">
                                数字化桥梁
                            </span>
                        </h1>

                        <p className="text-xl text-slate-600 leading-relaxed mb-12 max-w-2xl mx-auto">
                            专业的法律条文知识图谱系统，深度整合法条数据、文书管理与智能搜索，
                            为法律专业人士打造的一站式智慧工作平台。
                        </p>

                        <div className="flex flex-col sm:flex-row gap-5 justify-center items-center">
                            <button
                                onClick={() => navigate('/provisions')}
                                className="group inline-flex items-center justify-center gap-3 px-10 py-4 bg-slate-900 text-white rounded-2xl font-bold hover:bg-slate-800 transition-all duration-300 shadow-xl shadow-slate-900/20 hover:-translate-y-1"
                            >
                                立即体验
                                <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                            </button>
                        </div>

                        <div className="mt-16 flex items-center justify-center gap-8 grayscale opacity-50">
                            {/* Simple placeholders for "Trusted by" or partners */}
                            <div className="text-sm font-bold tracking-widest text-slate-400 uppercase">Trusted by Legal Professionals</div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Features Grid */}
            <div className="max-w-7xl mx-auto px-8 py-24">
                <div className="mb-16">
                    <div className="max-w-2xl">
                        <h2 className="text-4xl font-black text-slate-900 mb-4 tracking-tight">核心能力</h2>
                        <p className="text-slate-500 text-lg">全方位的数字化法律服务工具，重新定义法律工作流</p>
                    </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    {features.filter(f => !f.hidden).map((feature, index) => (
                        <div
                            key={index}
                            onClick={() => navigate(feature.path)}
                            className="group cursor-pointer bg-white rounded-[2rem] p-10 border border-slate-100 shadow-sm hover:shadow-2xl hover:shadow-blue-500/10 transition-all duration-500 hover:-translate-y-2 relative overflow-hidden"
                        >
                            {/* Hover background decoration */}
                            <div className={`absolute -right-10 -bottom-10 w-40 h-40 bg-${feature.color}-50 rounded-full opacity-0 group-hover:opacity-100 transition-opacity duration-500 blur-3xl`} />

                            <div className="flex flex-col sm:flex-row items-start gap-8 relative z-10">
                                {/* Icon */}
                                <div className={`p-5 rounded-2xl bg-${feature.color}-50 text-${feature.color}-600 group-hover:bg-${feature.color}-600 group-hover:text-white transition-all duration-500 shadow-inner`}>
                                    <feature.icon className="w-10 h-10" />
                                </div>

                                {/* Content */}
                                <div className="flex-1">
                                    <h3 className="text-2xl font-bold text-slate-900 mb-4 group-hover:text-blue-600 transition-colors">
                                        {feature.title}
                                    </h3>
                                    <p className="text-slate-500 leading-relaxed mb-6 text-lg">
                                        {feature.description}
                                    </p>
                                    <div className="flex items-center gap-3 text-slate-400 group-hover:text-blue-600 transition-all font-bold">
                                        <span className="text-sm uppercase tracking-wider">Explore Now</span>
                                        <div className="w-8 h-[2px] bg-slate-200 group-hover:w-12 group-hover:bg-blue-600 transition-all" />
                                    </div>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {/* Quick Start Guide */}
            <div className="max-w-7xl mx-auto px-6 py-20 mb-20">
                <div className="bg-gradient-to-br from-gray-50 to-gray-100/50 rounded-3xl p-10 border border-gray-200">
                    <div className="text-center mb-12">
                        <h2 className="text-3xl font-bold text-gray-900 mb-4">快速开始</h2>
                        <p className="text-gray-600 text-lg">四步开启高效法律检索之旅</p>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
                        {[
                            { step: 1, title: '浏览法条', desc: '在左侧导航栏选择"法条数据库"', color: 'blue' },
                            { step: 2, title: '管理文书', desc: '点击"文书管理"创建新的法律文书', color: 'emerald' },
                            { step: 3, title: '个人中心', desc: '通过"个人中心"查看您的收藏和设置', color: 'amber' },
                        ].map((item) => (
                            <div key={item.step} className="relative">
                                <div className="bg-white rounded-2xl p-6 shadow-sm border border-gray-100">
                                    <div className={`inline-flex items-center justify-center w-12 h-12 rounded-2xl bg-${item.color}-100 text-${item.color}-600 font-bold text-lg mb-4`}>
                                        {item.step}
                                    </div>
                                    <h3 className="font-bold text-gray-900 mb-2">{item.title}</h3>
                                    <p className="text-sm text-gray-600 leading-relaxed">{item.desc}</p>
                                </div>
                                {item.step < 3 && (
                                    <div className="hidden lg:block absolute top-1/2 -right-4 transform -translate-y-1/2 z-10">
                                        <ArrowRight className="w-5 h-5 text-gray-300" />
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {/* Footer */}
            <div className="border-t border-gray-100">
                <div className="max-w-7xl mx-auto px-6 py-12">
                    <div className="flex flex-col md:flex-row justify-between items-center gap-4">
                        <div className="flex items-center gap-3">
                            <div className="w-10 h-10 bg-gradient-to-br from-blue-600 to-violet-600 rounded-xl flex items-center justify-center">
                                <Network className="w-6 h-6 text-white" />
                            </div>
                            <div>
                                <div className="font-bold text-gray-900">Legal KG</div>
                                <div className="text-sm text-gray-500">Knowledge Graph System</div>
                            </div>
                        </div>
                        <p className="text-sm text-gray-500">
                            © 2026 Legal Knowledge Graph. Crafted with precision.
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
}
