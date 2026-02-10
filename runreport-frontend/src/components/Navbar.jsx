import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "../AuthContext";
import { supabase } from "../supabaseClient";

export default function Navbar() {
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);
  const { session } = useAuth();
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const userMenuRef = useRef(null);

  const displayName = session?.user?.user_metadata?.full_name
    || session?.user?.user_metadata?.name
    || session?.user?.email?.split("@")[0];

  async function handleSignOut() {
    if (!supabase) return;
    await supabase.auth.signOut();
    setMenuOpen(false);
    setUserMenuOpen(false);
  }

  useEffect(() => {
    function handleClickOutside(event) {
      if (menuRef.current && !menuRef.current.contains(event.target)) {
        setMenuOpen(false);
      }
      if (userMenuRef.current && !userMenuRef.current.contains(event.target)) {
        setUserMenuOpen(false);
      }
    }

    function handleKeyDown(event) {
      if (event.key === "Escape") {
        setMenuOpen(false);
        setUserMenuOpen(false);
      }
    }

    if (menuOpen || userMenuOpen) {
      document.addEventListener("mousedown", handleClickOutside);
      document.addEventListener("keydown", handleKeyDown);
    }

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [menuOpen, userMenuOpen]);

  return (
    <div className="navbar">
      <div className="nav-title">
        <Link to="/" className="nav-title-link">
          <img className="nav-logo" src="/logo.png" alt="The Run Report logo" />
          <span>TheRunReport</span>
        </Link>
      </div>

      <div className="nav-links">
        <Link to="/charts">Charts</Link>
        <Link to="/about">About Us</Link>
        <Link to="/contact">Contact Us</Link>
      </div>

      <div className="nav-actions">
        {session ? (
          <div className="nav-user-menu" ref={userMenuRef}>
            <button
              type="button"
              className="nav-greeting-button"
              aria-expanded={userMenuOpen}
              onClick={() => setUserMenuOpen((open) => !open)}
            >
              Hello, {displayName || "there"}
            </button>
            {userMenuOpen && (
              <div className="nav-user-dropdown">
                <button type="button" onClick={handleSignOut}>
                  Sign Out
                </button>
              </div>
            )}
          </div>
        ) : (
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
            <span className="login-label">Sign in</span>
          </Link>
        )}
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
              {session ? (
                <div className="nav-mobile-greeting">
                  Hello, {displayName || "there"}
                </div>
              ) : (
                <Link to="/login" onClick={() => setMenuOpen(false)}>
                  Sign in / Create account
                </Link>
              )}
              <Link to="/charts" onClick={() => setMenuOpen(false)}>
                Charts
              </Link>
              <Link to="/about" onClick={() => setMenuOpen(false)}>
                About Us
              </Link>
              <Link to="/contact" onClick={() => setMenuOpen(false)}>
                Contact Us
              </Link>
              {session && (
                <button
                  type="button"
                  className="nav-mobile-signout"
                  onClick={handleSignOut}
                >
                  Sign Out
                </button>
              )}
            </div>
          )}
        </div>
      </div>

    </div>
  );
}
