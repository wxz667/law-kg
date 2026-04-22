import { create } from 'zustand';

export interface DocumentOut {
    id: string;
    title: string;
    content: string;
    type?: string;
    status?: string;
    created_at: string;
    updated_at: string;
    provisions?: string[];
}

interface DocumentState {
    currentDocument: DocumentOut | null;
    setCurrentDocument: (doc: DocumentOut | null) => void;
    documents: DocumentOut[];
    setDocuments: (docs: DocumentOut[]) => void;
    addDocument: (doc: DocumentOut) => void;
    updateDocument: (doc: DocumentOut) => void;
    removeDocument: (id: string) => void;
}

export const useDocumentStore = create<DocumentState>((set) => ({
    currentDocument: null,
    setCurrentDocument: (doc) => set({ currentDocument: doc }),
    documents: [],
    setDocuments: (docs) => set({ documents: docs }),
    addDocument: (doc) => set((state) => ({
        documents: [...state.documents, doc]
    })),
    updateDocument: (doc) => set((state) => ({
        documents: state.documents.map(d => d.id === doc.id ? doc : d),
        currentDocument: state.currentDocument?.id === doc.id ? doc : state.currentDocument
    })),
    removeDocument: (id) => set((state) => ({
        documents: state.documents.filter(d => d.id !== id),
        currentDocument: state.currentDocument?.id === id ? null : state.currentDocument
    })),
}));
