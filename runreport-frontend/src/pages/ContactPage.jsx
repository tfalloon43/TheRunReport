export default function ContactPage() {
  return (
    <div className="page-container contact-page">
      <section className="contact-hero">
        <h1>Contact Us</h1>
        <p className="contact-lede">
          Have a question, spotted an issue, or want to suggest a feature? Feedback is a gift.
          If something looks wrong or confusing, we genuinely want to know.
          Thanks for helping us improve!
        </p>
        <p className="contact-lede">
          Get TheRunReport free for life if you bring us fish count data sources with weekly updates!
        </p>
      </section>

      <section className="contact-section contact-form-section">

        <form className="contact-form" onSubmit={(event) => event.preventDefault()}>
          <label className="form-field">
            <span className="field-title">Name</span>
            <input type="text" name="name" autoComplete="name" />
          </label>

          <label className="form-field">
            <span className="field-title">Email address</span>
            <input type="email" name="email" autoComplete="email" />
          </label>

          <label className="form-field">
            <span className="field-title">Issue / Message</span>
            <span className="field-helper">
              Tell us what's going on -- the more detail, the better.
            </span>
            <textarea name="message" rows="6" />
            <span className="field-example">
              If your issue is best described with pictures please email 
              us directly at support@therunreport.app.
            </span>
          </label>

          <button className="cta-button" type="submit">
            Send message
          </button>
        </form>
      </section>
    </div>
  );
}
