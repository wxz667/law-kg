import { useState } from 'react';
import { Search, History, X } from 'lucide-react';
import { Input, Button, Card } from '@/components/ui';
import { useProvisionSearch } from '../hooks/useProvisionSearch';
import { SearchResults } from './SearchResults';

interface ProvisionSearchProps {
    onResultSelect?: (result: any) => void;
}

export function ProvisionSearch({ onResultSelect }: ProvisionSearchProps) {
    const [query, setQuery] = useState('');
    const [field, setField] = useState<'name' | 'text'>('name');
    const [searchHistory, setSearchHistory] = useState<string[]>([]);
    const { results, total, search } = useProvisionSearch();

    const handleSearch = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!query.trim()) return;

        // 添加到搜索历史
        setSearchHistory(prev => {
            const newHistory = [query, ...prev.filter(h => h !== query)].slice(0, 10);
            return newHistory;
        });

        await search({ q: query, field });
    };

    const clearHistory = () => {
        setSearchHistory([]);
    };

    const selectHistoryItem = (item: string) => {
        setQuery(item);
        search({ q: item, field });
    };

    return (
        <div className="p-6 bg-white">
            {/* 搜索框 */}
            <form onSubmit={handleSearch} className="mb-6">
                <div className="flex gap-2">
                    <Input
                        placeholder="请输入搜索关键词"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        leftAddon={<Search className="w-4 h-4" />}
                        className="flex-1"
                    />
                    <select
                        value={field}
                        onChange={(e) => setField(e.target.value as 'name' | 'text')}
                        className="border border-gray-300 rounded-md px-3 py-2 text-sm"
                    >
                        <option value="name">名称</option>
                        <option value="text">内容</option>
                    </select>
                    <Button type="submit" disabled={!query.trim()}>
                        搜索
                    </Button>
                </div>
            </form>

            {/* 搜索历史 */}
            {searchHistory.length > 0 && !results.length && (
                <Card className="mb-6">
                    <div className="flex items-center justify-between mb-3 px-4 py-2 border-b">
                        <div className="flex items-center gap-2 text-gray-600">
                            <History className="w-4 h-4" />
                            <span className="text-sm font-medium">搜索历史</span>
                        </div>
                        <button
                            onClick={clearHistory}
                            className="text-gray-400 hover:text-gray-600"
                        >
                            <X className="w-4 h-4" />
                        </button>
                    </div>
                    <div className="px-4 py-2 space-y-2">
                        {searchHistory.map((item, index) => (
                            <button
                                key={index}
                                onClick={() => selectHistoryItem(item)}
                                className="block w-full text-left px-3 py-2 text-sm text-gray-700 hover:bg-gray-100 rounded"
                            >
                                {item}
                            </button>
                        ))}
                    </div>
                </Card>
            )}

            {/* 搜索结果 */}
            {results.length > 0 && (
                <div>
                    <div className="mb-4 text-sm text-gray-600">
                        找到 <span className="font-medium text-blue-600">{total}</span> 条结果
                    </div>
                    <SearchResults results={results} onSelect={onResultSelect} />
                </div>
            )}
        </div>
    );
}
