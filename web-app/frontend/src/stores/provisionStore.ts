import { create } from 'zustand';

export interface TreeNode {
    id: string;
    title: string;
    content?: string;
    children?: TreeNode[];
    level?: number | string;  // 支持数字或字符串（document, chapter, article等）
    nodeType?: 'DocumentNode' | 'TocNode' | 'ProvisionNode';  // 节点类型
}

interface ProvisionState {
    currentProvision: TreeNode | null;
    setCurrentProvision: (node: TreeNode | null) => void;
    favorites: string[];
    toggleFavorite: (id: string) => void;
}

export const useProvisionStore = create<ProvisionState>((set, get) => ({
    currentProvision: null,
    setCurrentProvision: (node) => set({ currentProvision: node }),
    favorites: [],
    toggleFavorite: (id) => {
        const favorites = get().favorites;
        if (favorites.includes(id)) {
            set({ favorites: favorites.filter(fav => fav !== id) });
        } else {
            set({ favorites: [...favorites, id] });
        }
    },
}));
