import { BrowserRouter as Router, Routes, Route } from "react-router-dom";

import Navbar from "./components/Navbar";
import HomePage from "./pages/HomePage";
import ChartsPage from "./pages/ChartsPage";
import ContactPage from "./pages/ContactPage";

import "./App.css";

export default function App() {
  return (
    <Router>
      <div className="app-container">
        <Navbar />

        {/* Main Page Content */}
        <div className="main-content">
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/charts" element={<ChartsPage />} />
            <Route path="/contact" element={<ContactPage />} />
          </Routes>
        </div>
      </div>
    </Router>
  );
}