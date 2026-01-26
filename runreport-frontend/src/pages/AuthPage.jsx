import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { supabase, isSupabaseConfigured } from "../supabaseClient";
import { useAuth } from "../AuthContext";

export default function AuthPage() {
  const navigate = useNavigate();
  const { session } = useAuth();
  const [signInEmail, setSignInEmail] = useState("");
  const [signInPassword, setSignInPassword] = useState("");
  const [signUpName, setSignUpName] = useState("");
  const [signUpEmail, setSignUpEmail] = useState("");
  const [signUpPassword, setSignUpPassword] = useState("");
  const [authMessage, setAuthMessage] = useState("");
  const [authError, setAuthError] = useState("");
  const [busy, setBusy] = useState(false);

  async function handleSignIn(event) {
    event.preventDefault();
    setAuthError("");
    setAuthMessage("");
    if (!supabase) return;
    setBusy(true);
    const { error } = await supabase.auth.signInWithPassword({
      email: signInEmail,
      password: signInPassword,
    });
    setBusy(false);
    if (error) {
      setAuthError(error.message);
      return;
    }
    navigate("/charts");
  }

  async function handleSignUp(event) {
    event.preventDefault();
    setAuthError("");
    setAuthMessage("");
    if (!supabase) return;
    setBusy(true);
    const { error } = await supabase.auth.signUp({
      email: signUpEmail,
      password: signUpPassword,
      options: {
        data: {
          full_name: signUpName,
        },
      },
    });
    setBusy(false);
    if (error) {
      setAuthError(error.message);
      return;
    }
    setAuthMessage("Check your email to confirm your account.");
  }

  async function handleResetPassword() {
    setAuthError("");
    setAuthMessage("");
    if (!supabase) return;
    if (!signInEmail) {
      setAuthError("Enter your email first so we know where to send the reset.");
      return;
    }
    setBusy(true);
    const { error } = await supabase.auth.resetPasswordForEmail(signInEmail, {
      redirectTo: window.location.origin + "/login",
    });
    setBusy(false);
    if (error) {
      setAuthError(error.message);
      return;
    }
    setAuthMessage("Password reset email sent.");
  }

  async function handleSignOut() {
    setAuthError("");
    setAuthMessage("");
    if (!supabase) return;
    setBusy(true);
    const { error } = await supabase.auth.signOut();
    setBusy(false);
    if (error) {
      setAuthError(error.message);
    }
  }

  return (
    <div className="page-container auth-page">
      <section className="auth-hero">
        <h1>Welcome back</h1>
        <p>
          Sign in to save runs, track favorites, and keep your fishing plans in
          one place.
        </p>
        {!isSupabaseConfigured && (
          <p className="auth-helper">Supabase is not configured yet.</p>
        )}
        {session && (
          <div className="auth-signed-in">
            <p className="auth-helper">
              You are signed in as {session.user?.email}.
            </p>
            <button
              className="cta-button outline"
              type="button"
              onClick={handleSignOut}
            >
              Sign out
            </button>
          </div>
        )}
      </section>

      <section className="auth-grid">
        <div className="auth-card">
          <h2>Sign in</h2>
          <form className="auth-form" onSubmit={handleSignIn}>
            <label className="form-field">
              <span className="field-title">Email address</span>
              <input
                type="email"
                name="email"
                autoComplete="email"
                value={signInEmail}
                onChange={(event) => setSignInEmail(event.target.value)}
                required
              />
            </label>
            <label className="form-field">
              <span className="field-title">Password</span>
              <input
                type="password"
                name="password"
                autoComplete="current-password"
                value={signInPassword}
                onChange={(event) => setSignInPassword(event.target.value)}
                required
              />
            </label>
            <button className="cta-button" type="submit">
              {busy ? "Signing in..." : "Sign in"}
            </button>
            <p className="auth-helper">
              Forgot your password?{" "}
              <button type="button" onClick={handleResetPassword}>
                Reset it
              </button>
            </p>
          </form>
        </div>

        <div className="auth-card secondary">
          <h2>New here?</h2>
          <p>
            Create an account to get early access features and personalized run
            alerts.
          </p>
          <form className="auth-form" onSubmit={handleSignUp}>
            <label className="form-field">
              <span className="field-title">Name</span>
              <input
                type="text"
                name="signup-name"
                autoComplete="name"
                placeholder="Name"
                value={signUpName}
                onChange={(event) => setSignUpName(event.target.value)}
                required
              />
            </label>
            <label className="form-field">
              <span className="field-title">Email address</span>
              <input
                type="email"
                name="signup-email"
                autoComplete="email"
                placeholder="Email"
                value={signUpEmail}
                onChange={(event) => setSignUpEmail(event.target.value)}
                required
              />
            </label>
            <label className="form-field">
              <span className="field-title">Password</span>
              <input
                type="password"
                name="signup-password"
                autoComplete="new-password"
                placeholder="Password"
                value={signUpPassword}
                onChange={(event) => setSignUpPassword(event.target.value)}
                required
              />
            </label>
            <button className="cta-button" type="submit">
              {busy ? "Creating..." : "Create account"}
            </button>
          </form>
        </div>
      </section>

      {(authError || authMessage) && (
        <section className="auth-feedback">
          {authError && <p className="auth-error">{authError}</p>}
          {authMessage && <p className="auth-message">{authMessage}</p>}
          {session && (
            <button
              className="cta-button outline"
              type="button"
              onClick={handleSignOut}
            >
              Sign out
            </button>
          )}
        </section>
      )}
    </div>
  );
}
