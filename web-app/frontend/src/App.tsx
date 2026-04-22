import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import { Sidebar } from "@/components/layout/Sidebar";
import { ProtectedRoute } from "@/components/ProtectedRoute";
import Home from "./pages/Home";
import Search from "./pages/Search";
import { Provisions } from "./pages/Provisions/Provisions";
import { ProvisionDetail } from "./pages/Provisions/ProvisionDetail";
import { DocumentsList } from "./pages/Documents/DocumentsList";
import { DocumentEditor } from "./pages/Documents/DocumentEditor";
import Profile from "./pages/Profile/Profile";
import Login from "./pages/Login";
import Register from "./pages/Register";

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />
        <Route path="/*" element={
          <div className="flex h-screen bg-gray-50 overflow-hidden">
            {/* 侧边栏 - 固定左侧 */}
            <div className="flex-shrink-0">
              <Sidebar />
            </div>

            {/* 主内容区 - 占据剩余空间 */}
            <main className="flex-1 overflow-auto">
              <div className="min-h-full">
                <Routes>
                  <Route path="/" element={<ProtectedRoute><Home /></ProtectedRoute>} />
                  <Route path="/provisions" element={<ProtectedRoute><Provisions /></ProtectedRoute>} />
                  <Route path="/provisions/:id" element={<ProtectedRoute><ProvisionDetail /></ProtectedRoute>} />
                  <Route path="/documents" element={<ProtectedRoute><DocumentsList /></ProtectedRoute>} />
                  <Route path="/documents/new" element={<ProtectedRoute><DocumentEditor /></ProtectedRoute>} />
                  <Route path="/documents/edit/:id" element={<ProtectedRoute><DocumentEditor /></ProtectedRoute>} />
                  <Route path="/documents/:id" element={<ProtectedRoute><DocumentEditor /></ProtectedRoute>} />
                  <Route path="/search" element={<ProtectedRoute><Search /></ProtectedRoute>} />
                  <Route path="/profile" element={<ProtectedRoute><Profile /></ProtectedRoute>} />
                </Routes>
              </div>
            </main>
          </div>
        } />
      </Routes>
    </Router>
  );
}

export default App
