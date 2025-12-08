import { Link } from "react-router-dom";

export default function Navbar() {
  return (
    <div className="navbar">
      <div className="nav-title">
        <Link to="/" style={{ color: "white", textDecoration: "none" }}>
          The Run Report
        </Link>
      </div>

      <div className="nav-links">
        <Link to="/">Home</Link>
        <Link to="/charts">Charts</Link>
        <Link to="/contact">Contact Us</Link>
      </div>
    </div>
  );
}