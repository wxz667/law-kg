import { Search as SearchIcon, Tag, Filter, CheckCircle2, ArrowRight, Sparkles } from 'lucide-react';

export default function Search() {
    return (
        <div className="min-h-screen bg-slate-50">
            {/* Header */}
            <div className="border-b border-slate-200 bg-white/80 backdrop-blur-md sticky top-0 z-50">
                <div className="max-w-6xl mx-auto px-8 py-8">
                    <div className="flex items-center gap-4 mb-2">
                        <div className="p-2 bg-blue-600 rounded-lg shadow-lg shadow-blue-600/20">
                            <Sparkles className="w-5 h-5 text-white" />
                        </div>
                        <h1 className="text-4xl font-black text-slate-900 tracking-tight">智能搜索</h1>
                    </div>
                    <p className="text-slate-500 font-medium">利用 AI 和知识图谱技术，精准定位法律条文与关联信息</p>
                </div>
            </div>

            {/* Main Content */}
            <div className="max-w-6xl mx-auto px-8 py-12">
                {/* Search Box Section */}
                <div className="bg-white rounded-[3rem] p-12 border border-slate-200 shadow-2xl shadow-slate-200/50 mb-12 relative overflow-hidden">
                    {/* Background decoration */}
                    <div className="absolute top-0 right-0 w-64 h-64 bg-blue-50 rounded-full blur-3xl -mr-32 -mt-32 opacity-50" />
                    
                    <div className="relative z-10">
                        <div className="text-center mb-12">
                            <h2 className="text-3xl font-black text-slate-900 mb-4 tracking-tight">全文语义检索</h2>
                            <p className="text-slate-500 text-lg font-medium">支持自然语言查询，智能理解法律术语与上下文关系</p>
                        </div>

                        <div className="relative max-w-4xl mx-auto mb-10">
                            <input
                                type="text"
                                placeholder="输入您想查询的法律问题或关键词..."
                                className="w-full px-8 py-6 pl-16 bg-slate-50 border-2 border-slate-100 rounded-3xl focus:border-blue-500 focus:ring-8 focus:ring-blue-500/5 focus:bg-white transition-all outline-none text-xl shadow-inner font-medium"
                            />
                            <SearchIcon className="w-8 h-8 text-slate-400 absolute left-6 top-1/2 transform -translate-y-1/2" />
                            <button className="absolute right-3 top-1/2 transform -translate-y-1/2 px-10 py-4 bg-slate-900 text-white rounded-2xl hover:bg-slate-800 transition-all duration-300 font-bold shadow-xl shadow-slate-900/20 flex items-center gap-2">
                                <span>立即搜索</span>
                                <ArrowRight className="w-5 h-5" />
                            </button>
                        </div>

                        {/* Hot Tags */}
                        <div className="flex flex-wrap items-center justify-center gap-4">
                            <div className="flex items-center gap-2 text-sm font-bold text-slate-400 uppercase tracking-widest mr-2">
                                <Tag className="w-4 h-4" />
                                热门检索
                            </div>
                            {['民法典总则', '违约责任认定', '知识产权侵权', '劳动合同解除'].map((tag, index) => (
                                <button
                                    key={index}
                                    className="px-6 py-2.5 bg-white border border-slate-100 rounded-2xl text-sm font-bold text-slate-600 hover:border-blue-500 hover:text-blue-600 hover:bg-blue-50 transition-all duration-300 shadow-sm hover:shadow-md"
                                >
                                    {tag}
                                </button>
                            ))}
                        </div>
                    </div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-10">
                    {/* Left Side: Advanced Filters */}
                    <div className="lg:col-span-1 space-y-8">
                        <div className="bg-white rounded-[2.5rem] p-10 border border-slate-200 shadow-sm">
                            <div className="flex items-center gap-3 mb-8">
                                <div className="p-2 bg-slate-100 rounded-lg">
                                    <Filter className="w-5 h-5 text-slate-600" />
                                </div>
                                <h2 className="text-xl font-black text-slate-900 tracking-tight">高级筛选</h2>
                            </div>
                            
                            <div className="space-y-8">
                                {[
                                    { label: '搜索范围', options: ['全部资源', '法律条文', '裁判文书', '法律实体'] },
                                    { label: '法律部门', options: ['所有部门', '民商事', '刑事法律', '行政法规', '经济法'] },
                                    { label: '效力级别', options: ['全部级别', '法律', '行政法规', '地方性法规', '司法解释'] },
                                ].map((filter, index) => (
                                    <div key={index}>
                                        <label className="block text-sm font-black text-slate-400 uppercase tracking-widest mb-4">
                                            {filter.label}
                                        </label>
                                        <select className="w-full px-6 py-4 bg-slate-50 border border-slate-100 rounded-2xl focus:ring-4 focus:ring-blue-500/5 focus:border-blue-500 transition-all outline-none font-bold text-slate-700 cursor-pointer hover:bg-slate-100/50">
                                            {filter.options.map((option, idx) => (
                                                <option key={idx} value={option}>{option}</option>
                                            ))}
                                        </select>
                                    </div>
                                ))}
                            </div>

                            <button className="w-full mt-10 py-4 border-2 border-slate-100 text-slate-600 rounded-2xl font-bold hover:bg-slate-50 transition-all flex items-center justify-center gap-2">
                                重置筛选
                            </button>
                        </div>
                    </div>

                    {/* Right Side: Results / Empty State */}
                    <div className="lg:col-span-2">
                        <div className="bg-slate-100/50 rounded-[2.5rem] border-4 border-dashed border-slate-200 p-16 text-center h-full flex flex-col items-center justify-center">
                            <div className="w-32 h-32 bg-white rounded-full flex items-center justify-center shadow-2xl mb-10 relative">
                                <SearchIcon className="w-12 h-12 text-slate-200" />
                                <div className="absolute -right-2 -top-2 w-10 h-10 bg-blue-500 rounded-2xl flex items-center justify-center shadow-lg animate-bounce">
                                    <Sparkles className="w-5 h-5 text-white" />
                                </div>
                            </div>
                            <h3 className="text-3xl font-black text-slate-900 mb-4 tracking-tight">准备好开始检索了吗？</h3>
                            <p className="text-slate-500 text-lg font-medium mb-12 max-w-md mx-auto">
                                在上方输入关键词，系统将为您实时呈现来自知识图谱的深度关联结果
                            </p>
                            <div className="flex flex-wrap items-center justify-center gap-8">
                                {[
                                    { label: '全文语义理解', color: 'text-blue-600' },
                                    { label: '关联知识图谱', color: 'text-indigo-600' },
                                    { label: '精准效力筛选', color: 'text-violet-600' },
                                ].map((feature, index) => (
                                    <div key={index} className={`flex items-center gap-3 ${feature.color}`}>
                                        <CheckCircle2 className="w-6 h-6" />
                                        <span className="font-black text-sm uppercase tracking-wider">{feature.label}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
