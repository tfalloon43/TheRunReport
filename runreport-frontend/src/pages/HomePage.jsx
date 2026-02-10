import { Link } from "react-router-dom";

export default function HomePage() {
  return (
    <div className="page-container home-page">
      <section className="home-hero">
        <div className="hero-grid">
          <div className="hero-copy">
            <h1>CATCH FISH TODAY</h1>
            <p className="hero-detail">
              TheRunReport gives Washington fish run data.<br />
              Easy-to-read charts help you catch more fish.
            </p>
            <div className="cta-row">
              <Link className="cta-button" to="/login">
                Join TheRunReport
              </Link>
            </div>
          </div>
        </div>
      </section>

      <section className="home-section benefits-section">
        <div className="benefit-grid">
          <div className="benefit-card">
            <h3>Hatchery Escapement Reports Visualized</h3>
            <div className="benefit-card-body">
              <ul className="benefit-list">
                <li>Salmon, Steelhead, Cutthroat</li>
                <li>49 different river systems across WA</li>
                <li>Choose the fishy water</li>
              </ul>
              <img
                className="benefit-card-image"
                src="/chart.jpeg"
                alt="Sample run chart"
              />
            </div>
          </div>
          <div className="benefit-card">
            <h3>Check river flows in one click</h3>
            <div className="benefit-card-body">
              <ul className="benefit-list">
                <li>CFS and stage data from 69 stations</li>
                <li>See 7 day and 30 day trends, time the drop</li>
              </ul>
              <img
                className="benefit-card-image"
                src="/Flow.jpeg"
                alt="River flow chart"
              />
            </div>
          </div>
          <div className="benefit-card">
            <h3>Regular updates, low price</h3>
            <div className="benefit-card-body">
              <ul className="benefit-list">
                <li>Flows update hourly</li>
                <li>Dam counts update daily</li>
                <li>Hatchery counts update weekly</li>
                <li>$5/month</li>
              </ul>
              <img
                className="benefit-card-image"
                src="/AboutUs2.jpeg"
                alt="Run report data sources"
              />
            </div>
          </div>
        </div>
      </section>


      <section className="home-section closing-cta">
        <h2>Ready to catch more fish?</h2>
        <Link className="cta-button" to="/login">
          Join TheRunReport
        </Link>
        <p className="cta-footnote">
          The only place Washington anglers can see fish counts and river
          conditions together, clearly, in one place.
        </p>
      </section>
    </div>
  );
}
