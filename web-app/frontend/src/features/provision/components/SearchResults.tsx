import type { TreeNode } from '../types';

interface SearchResultsProps {
    results: TreeNode[];
    onSelect?: (result: TreeNode) => void;
}

export function SearchResults({ results, onSelect }: SearchResultsProps) {
    return (
        <div className="space-y-4">
            {results.map((result) => (
                <div
                    key={result.id}
                    className="border rounded-lg p-4 hover:shadow-md transition-shadow cursor-pointer"
                    onClick={() => onSelect && onSelect(result)}
                >
                    <div className="flex items-start justify-between mb-2">
                        <div>
                            <h3 className="font-semibold text-lg text-gray-900">
                                {result.name}
                            </h3>
                            {result.properties?.documentName && (
                                <p className="text-sm text-gray-500 mt-1">
                                    {result.properties.documentName}
                                </p>
                            )}
                        </div>
                        <span className="px-2 py-1 bg-blue-100 text-blue-800 text-xs rounded">
                            {result.level}
                        </span>
                    </div>

                    {result.properties?.content && (
                        <p className="text-gray-600 text-sm line-clamp-3">
                            {result.properties.content}
                        </p>
                    )}
                </div>
            ))}
        </div>
    );
}
