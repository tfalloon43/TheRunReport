import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";

export default function Navbar() {
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);

  useEffect(() => {
    function handleClickOutside(event) {
      if (!menuRef.current) return;
      if (!menuRef.current.contains(event.target)) {
        setMenuOpen(false);
      }
    }

    function handleKeyDown(event) {
      if (event.key === "Escape") {
        setMenuOpen(false);
      }
    }

    if (menuOpen) {
      document.addEventListener("mousedown", handleClickOutside);
      document.addEventListener("keydown", handleKeyDown);
    }

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [menuOpen]);

  return (
    <div className="navbar">
      <div className="nav-title">
        <Link to="/" className="nav-title-link">
          <img className="nav-logo" src="/logo.png" alt="The Run Report logo" />
          <span>TheRunReport</span>
        </Link>
      </div>

      <div className="nav-links">
        <Link to="/">Home</Link>
        <Link to="/charts">Charts</Link>
        <Link to="/about">About Us</Link>
        <Link to="/contact">Contact Us</Link>
      </div>

      <div className="nav-actions">
        <Link className="login-button" to="/login">
          <span className="login-icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" role="presentation">
              <path
                d="M5 12h12M13 6l6 6-6 6"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </span>
          <span className="login-label">SIGN IN</span>
        </Link>
        <div className="nav-menu-wrap" ref={menuRef}>
          <button
            type="button"
            className="nav-menu-button"
            aria-label="Open menu"
            aria-expanded={menuOpen}
            onClick={() => setMenuOpen((open) => !open)}
          >
            <span className="menu-icon" aria-hidden="true">
              <span className="menu-icon-row" />
              <span className="menu-icon-row" />
              <span className="menu-icon-row" />
            </span>
          </button>
          {menuOpen && (
            <div className="nav-mobile-menu">
              <Link to="/" onClick={() => setMenuOpen(false)}>
                Home
              </Link>
              <Link to="/charts" onClick={() => setMenuOpen(false)}>
                Charts
              </Link>
              <Link to="/about" onClick={() => setMenuOpen(false)}>
                About Us
              </Link>
              <Link to="/contact" onClick={() => setMenuOpen(false)}>
                Contact Us
              </Link>
              <Link to="/login" onClick={() => setMenuOpen(false)}>
                Sign In
              </Link>
            </div>
          )}
        </div>
      </div>

    </div>
  );
}
