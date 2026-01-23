import { Link } from "react-router-dom";

export default function AuthPage() {
  return (
    <div className="page-container auth-page">
      <section className="auth-hero">
        <h1>Welcome back</h1>
        <p>
          Sign in to save runs, track favorites, and keep your fishing plans in
          one place.
        </p>
      </section>

      <section className="auth-grid">
        <div className="auth-card">
          <h2>Sign in</h2>
          <form className="auth-form" onSubmit={(event) => event.preventDefault()}>
            <label className="form-field">
              <span className="field-title">Email address</span>
              <input type="email" name="email" autoComplete="email" />
            </label>
            <label className="form-field">
              <span className="field-title">Password</span>
              <input type="password" name="password" autoComplete="current-password" />
            </label>
            <button className="cta-button" type="submit">
              Sign in
            </button>
            <p className="auth-helper">
              Forgot your password? <button type="button">Reset it</button>
            </p>
          </form>
        </div>

        <div className="auth-card secondary">
          <h2>New here?</h2>
          <p>
            Create an account to get early access features and personalized run
            alerts.
          </p>
          <ul>
            <li>Save favorite rivers and species</li>
            <li>Get notified when runs spike</li>
            <li>Keep notes for each trip</li>
          </ul>
          <Link className="cta-button outline" to="/signup">
            Create account
          </Link>
        </div>
      </section>
    </div>
  );
}
