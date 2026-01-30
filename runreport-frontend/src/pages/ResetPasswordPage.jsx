import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { supabase, isSupabaseConfigured } from "../supabaseClient";
import { useAuth } from "../AuthContext";

export default function ResetPasswordPage() {
  const navigate = useNavigate();
  const { session } = useAuth();
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [authMessage, setAuthMessage] = useState("");
  const [authError, setAuthError] = useState("");
  const [busy, setBusy] = useState(false);

  async function handleUpdatePassword(event) {
    event.preventDefault();
    setAuthError("");
    setAuthMessage("");

    if (!supabase) return;
    if (!password || !confirmPassword) {
      setAuthError("Enter and confirm your new password.");
      return;
    }
    if (password !== confirmPassword) {
      setAuthError("Passwords do not match.");
      return;
    }

    setBusy(true);
    const { error } = await supabase.auth.updateUser({ password });
    setBusy(false);

    if (error) {
      setAuthError(error.message);
      return;
    }

    setAuthMessage("Password updated. You can sign in with your new password.");
    setPassword("");
    setConfirmPassword("");
  }

  return (
    <div className="page-container auth-page reset-page">
      <section className="auth-hero">
        <h1>Reset your password</h1>
        <p>Choose a new password for your account.</p>
        {!isSupabaseConfigured && (
          <p className="auth-helper">Supabase is not configured yet.</p>
        )}
        {!session && (
          <p className="auth-helper">
            Your reset link may be expired. Request a new one from the sign in page.
          </p>
        )}
      </section>

      <section className="auth-grid">
        <div className="auth-card">
          <h2>Set a new password</h2>
          <form className="auth-form" onSubmit={handleUpdatePassword}>
            <label className="form-field">
              <span className="field-title">New password</span>
              <input
                type="password"
                name="new-password"
                autoComplete="new-password"
                placeholder="New password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                required
              />
            </label>
            <label className="form-field">
              <span className="field-title">Confirm password</span>
              <input
                type="password"
                name="confirm-password"
                autoComplete="new-password"
                placeholder="Confirm password"
                value={confirmPassword}
                onChange={(event) => setConfirmPassword(event.target.value)}
                required
              />
            </label>
            <button className="cta-button" type="submit" disabled={busy}>
              {busy ? "Updating..." : "Update password"}
            </button>
          </form>
          <div className="auth-helper" style={{ marginTop: "12px" }}>
            <button
              type="button"
              onClick={() => navigate("/login")}
              className="reset-link-button"
            >
              Back to sign in
            </button>
          </div>
        </div>
      </section>

      {(authError || authMessage) && (
        <section className="auth-feedback">
          {authError && <p className="auth-error">{authError}</p>}
          {authMessage && <p className="auth-message">{authMessage}</p>}
          {authMessage && (
            <p className="auth-helper">
              <Link to="/login">Go to sign in</Link>
            </p>
          )}
        </section>
      )}
    </div>
  );
}
