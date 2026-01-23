export default function ContactPage() {
  return (
    <div className="page-container contact-page">
      <section className="contact-hero">
        <h1>Get in touch</h1>
        <p className="contact-lede">
          Have a question, spotted an issue, or want to suggest a feature? We
          read everything and use feedback to make TheRunReport better.
        </p>
      </section>

      <section className="contact-section contact-options">
        <h2>Contact options</h2>
        <div className="contact-grid">
          <div className="contact-card">
            <h3>Email</h3>
            <p className="contact-detail">support@therunreport.app</p>
            <p>
              If you prefer email, you can reach us directly here. We typically
              respond within 1-2 business days.
            </p>
          </div>
          <div className="contact-card">
            <h3>Response time</h3>
            <p>
              We are a small, focused project, not a call center. Every message
              is read, but response times may be slower during peak fishing
              seasons.
            </p>
          </div>
          <div className="contact-card">
            <h3>Location</h3>
            <p>Washington State, built by a local angler.</p>
            <p className="contact-note">
              Phone support is not available yet, email is the fastest way to
              reach us.
            </p>
          </div>
        </div>
      </section>

      <section className="contact-section contact-form-section">
        <div className="contact-form-header">
          <h2>Send a message</h2>
          <p>
            Use the form below and we will get back to you as soon as possible.
          </p>
          <p className="contact-note">
            If it is urgent or the form is not working, email us directly at
            support@therunreport.app.
          </p>
        </div>

        <form className="contact-form" onSubmit={(event) => event.preventDefault()}>
          <label className="form-field">
            <span className="field-title">Name</span>
            <span className="field-helper">So we know who we're talking to.</span>
            <input type="text" name="name" autoComplete="name" />
          </label>

          <label className="form-field">
            <span className="field-title">Email address</span>
            <span className="field-helper">Where we should reply.</span>
            <input type="email" name="email" autoComplete="email" />
          </label>

          <label className="form-field">
            <span className="field-title">Issue / Message</span>
            <span className="field-helper">
              Tell us what's going on -- the more detail, the better.
            </span>
            <textarea name="message" rows="6" />
            <span className="field-example">
              Examples: data looks off, chart request, bug report, feature idea,
              general question.
            </span>
          </label>

          <button className="cta-button" type="submit">
            Send message
          </button>
        </form>
      </section>

      <section className="contact-section contact-about">
        <div>
          <h2>About TheRunReport</h2>
          <p>
            TheRunReport is built by a Washington angler who wanted faster,
            clearer access to fish run data without digging through agency sites
            and spreadsheets.
          </p>
          <p>If something looks wrong or confusing, we genuinely want to know.</p>
        </div>
        <p className="contact-thanks">Thanks for helping improve TheRunReport.</p>
      </section>
    </div>
  );
}
