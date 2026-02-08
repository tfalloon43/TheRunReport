import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { supabase, isSupabaseConfigured } from "../supabaseClient";
import { useAuth } from "../AuthContext";
import { initPaddle } from "../utils/paddle";

export default function AuthPage() {
  const navigate = useNavigate();
  const { session } = useAuth();
  const appUrl = import.meta.env.VITE_APP_URL || window.location.origin;
  const [signInEmail, setSignInEmail] = useState("");
  const [signInPassword, setSignInPassword] = useState("");
  const [flowStep, setFlowStep] = useState("email");
  const [authMessage, setAuthMessage] = useState("");
  const [authError, setAuthError] = useState("");
  const [busy, setBusy] = useState(false);
  const [billingError, setBillingError] = useState("");
  const [billingBusy, setBillingBusy] = useState(false);
  const [subscriptionStatus, setSubscriptionStatus] = useState(null);
  const [subscriptionLoading, setSubscriptionLoading] = useState(false);
  const [pendingEmail, setPendingEmail] = useState("");
  const [pendingUserId, setPendingUserId] = useState("");
  const isActiveSubscriber =
    subscriptionStatus === "active"
    || subscriptionStatus === "trialing"
    || subscriptionStatus === "complete";
  const isStatusActive = (status) =>
    ["active", "trialing", "complete"].includes((status || "").toLowerCase());

  async function handleResetPassword() {
    setAuthError("");
    setAuthMessage("");
    if (!supabase) return;
    const email = (signInEmail || "").trim().toLowerCase();
    if (!email) {
      setAuthError("Enter your email first so we know where to send the reset.");
      return;
    }
    setBusy(true);
    const { data: gateData, error: gateError } = await supabase.functions.invoke(
      "password-reset-gate",
      {
        body: { email },
      }
    );
    if (gateError || !gateData?.ok) {
      setAuthError("This email is not associated with an active subscriber.");
      setBusy(false);
      return;
    }
    setBusy(true);
    const { error } = await supabase.auth.resetPasswordForEmail(email, {
      redirectTo: window.location.origin + "/reset-password",
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

  async function handleStartSubscription() {
    setBillingError("");
    const email = pendingEmail || signInEmail;
    if (!email) return;

    const priceId = import.meta.env.VITE_PADDLE_PRICE_ID;
    if (!priceId) {
      setBillingError("Missing Paddle price id.");
      return;
    }

    try {
      setBillingBusy(true);
      const paddle = await initPaddle();
      console.log("Paddle checkout URLs", {
        success_url: `${appUrl}/login`,
        close_url: `${appUrl}/login`,
      });
      paddle.Checkout.open({
        items: [{ priceId, quantity: 1 }],
        customer: {
          email,
        },
        customData: {
          email,
          user_id: pendingUserId || null,
        },
        settings: {
          successUrl: `${appUrl}/login`,
          closeUrl: `${appUrl}/login`,
        },
      });
    } catch (error) {
      setBillingError(error.message || "Unable to start checkout.");
    } finally {
      setBillingBusy(false);
    }
  }

  async function handleEmailContinue(event) {
    event.preventDefault();
    setAuthError("");
    setAuthMessage("");
    setBillingError("");
    if (!supabase) return;
    const email = (signInEmail || "").trim().toLowerCase();
    if (!email) {
      setAuthError("Enter your email first to continue.");
      return;
    }
    setBusy(true);
    const { data, error } = await supabase.functions.invoke(
      "subscription-lookup",
      { body: { email } }
    );
    setBusy(false);
    if (error) {
      setAuthError("We could not look up that email. Please try again.");
      return;
    }
    if (data?.found) {
      setPendingEmail(email);
      setPendingUserId(data.user_id || "");
      if (isStatusActive(data.status)) {
        setFlowStep("password");
      } else {
        setFlowStep("payment");
        setAuthMessage(
          "Email linked to an unsubscribed account. Continue to payment."
        );
      }
    } else {
      setPendingEmail(email);
      setPendingUserId("");
      setFlowStep("payment");
    }
  }

  async function handlePasswordSignIn(event) {
    event.preventDefault();
    setAuthError("");
    setAuthMessage("");
    if (!supabase) return;
    setBusy(true);
    const { error } = await supabase.auth.signInWithPassword({
      email: pendingEmail,
      password: signInPassword,
    });
    setBusy(false);
    if (error) {
      setAuthError(error.message);
      return;
    }
  }

  async function loadSubscriptionStatus() {
    if (!supabase || !session?.user?.id) {
      setSubscriptionStatus(null);
      return;
    }
    setSubscriptionLoading(true);
    const { data } = await supabase
      .from("paddle_subscriptions")
      .select("status")
      .eq("user_id", session.user.id)
      .maybeSingle();
    setSubscriptionStatus(data?.status ? data.status.toLowerCase() : null);
    setSubscriptionLoading(false);
  }

  useEffect(() => {
    loadSubscriptionStatus();
  }, [session?.user?.id]);

  useEffect(() => {
    if (!session || subscriptionLoading) return;
    if (isActiveSubscriber) {
      navigate("/charts");
      return;
    }
    if (session?.user?.email) {
      setFlowStep("payment");
      setPendingEmail(session.user.email);
      setAuthMessage(
        "Payment required. Please update your payment method to continue."
      );
    }
  }, [session, subscriptionLoading, subscriptionStatus, navigate, isActiveSubscriber]);

  return (
    <div className="page-container auth-page">
      <section className="auth-hero">
        <h1>Sign in or create account</h1>
        <p>Enter your email to continue.</p>
        {!isSupabaseConfigured && (
          <p className="auth-helper">Supabase is not configured yet.</p>
        )}
      </section>

      <section className="auth-grid">
        <div className="auth-card">
          {flowStep === "email" && (
            <form className="auth-form" onSubmit={handleEmailContinue}>
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
              <button className="cta-button" type="submit">
                {busy ? "Checking..." : "Continue"}
              </button>
            </form>
          )}

          {flowStep === "password" && (
            <form className="auth-form" onSubmit={handlePasswordSignIn}>
              <label className="form-field">
                <span className="field-title">Email address</span>
                <input
                  type="email"
                  name="email"
                  autoComplete="email"
                  value={pendingEmail}
                  onChange={(event) => setPendingEmail(event.target.value)}
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
                <span className="auth-text-link" onClick={handleResetPassword}>
                  Forgot your password?
                </span>
              </p>
            </form>
          )}

          {flowStep === "payment" && (
            <div className="auth-form">
              <p>
                Continue to payment to activate your subscription. We’ll email a
                link to set your password after checkout.
              </p>
              <button
                className="cta-button"
                type="button"
                onClick={handleStartSubscription}
                disabled={billingBusy}
              >
                {billingBusy ? "Opening checkout..." : "Continue to payment"}
              </button>
            </div>
          )}
        </div>
      </section>

      {(authError || authMessage) && (
        <section className="auth-feedback">
          {authError && <p className="auth-error">{authError}</p>}
          {authMessage && <p className="auth-message">{authMessage}</p>}
        </section>
      )}

      {billingError && (
        <section className="auth-feedback">
          <p className="auth-error">{billingError}</p>
        </section>
      )}
    </div>
  );
}
