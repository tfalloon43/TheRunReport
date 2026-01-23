import { Link } from "react-router-dom";

export default function Navbar() {
  return (
    <div className="navbar">
      <div className="nav-title">
        <Link to="/">The Run Report</Link>
      </div>

      <div className="nav-links">
        <Link to="/">Home</Link>
        <Link to="/charts">Charts</Link>
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
      </div>
    </div>
  );
}
