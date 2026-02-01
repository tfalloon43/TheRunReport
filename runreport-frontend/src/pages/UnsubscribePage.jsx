export default function UnsubscribePage() {
  const portalUrl = import.meta.env.VITE_PADDLE_PORTAL_URL;

  return (
    <div className="page-container legal-page">
      <h1>Unsubscribe</h1>
      <p>
        You can manage or cancel your subscription through the Paddle customer
        portal.
      </p>
      {portalUrl ? (
        <a className="cta-button" href={portalUrl} target="_blank" rel="noreferrer">
          Manage subscription
        </a>
      ) : (
        <p>Customer portal URL is not configured yet.</p>
      )}
    </div>
  );
}
