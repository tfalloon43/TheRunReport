import { useEffect, useState } from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";

import Navbar from "./components/Navbar";
import HomePage from "./pages/HomePage";
import ChartsPage from "./pages/ChartsPage";
import ContactPage from "./pages/ContactPage";
import AuthPage from "./pages/AuthPage";
import AboutPage from "./pages/AboutPage";
import AuthContext from "./AuthContext";
import { supabase } from "./supabaseClient";

import "./App.css";

export default function App() {
  const [session, setSession] = useState(null);
  const [authLoading, setAuthLoading] = useState(true);

  useEffect(() => {
    let mounted = true;

    async function loadSession() {
      if (!supabase) {
        if (mounted) setAuthLoading(false);
        return;
      }
      const { data } = await supabase.auth.getSession();
      if (mounted) {
        setSession(data.session);
        setAuthLoading(false);
      }
    }

    loadSession();

    if (!supabase) return () => {};
    const { data: listener } = supabase.auth.onAuthStateChange((_event, newSession) => {
      if (mounted) setSession(newSession);
    });

    return () => {
      mounted = false;
      listener?.subscription?.unsubscribe();
    };
  }, []);

  return (
    <AuthContext.Provider value={{ session, loading: authLoading }}>
      <Router>
        <div className="app-container">
          <Navbar />

          {/* Main Page Content */}
          <div className="main-content">
            <Routes>
              <Route path="/" element={<HomePage />} />
              <Route path="/charts" element={<ChartsPage />} />
              <Route path="/contact" element={<ContactPage />} />
              <Route path="/login" element={<AuthPage />} />
              <Route path="/about" element={<AboutPage />} />
            </Routes>
          </div>
        </div>
      </Router>
    </AuthContext.Provider>
  );
}
