import { Link } from "react-router-dom";

export default function Footer() {
  return (
    <footer className="site-footer">
      <div className="footer-links">
        <Link to="/terms">Terms and Conditions</Link>
        <Link to="/refund-policy">Refund Policy</Link>
        <Link to="/privacy-policy">Privacy Policy</Link>
        <Link to="/unsubscribe">Unsubscribe</Link>
      </div>
      <div className="footer-meta">© {new Date().getFullYear()} TheRunReport</div>
    </footer>
  );
}
