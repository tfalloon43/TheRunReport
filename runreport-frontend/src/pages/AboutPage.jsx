export default function AboutPage() {
  return (
    <div className="page-container about-page">
      <section className="about-row about-row-title">
        <h1>About TheRunReport</h1>
      </section>

      <section className="about-row about-row-split">
        <div className="about-media">
          <img className="about-hero-image" src="/AboutUs.jpeg" alt="About TheRunReport" />
        </div>
        <div className="about-card">
          <h2>How It Started</h2>
          <p>
            I grew up fly fishing for trout in Colorado. Cold water, small
            rivers, skittish fish. When I moved to Washington, I had never
            heard the word "steelhead". I was still fishing for trout when I saw my
            first steelhead. A few months later I caught one. It
            changed me.
          </p>
          <p>
            Steelhead fishing rewired how I thought about preparing to fish.
             It was not just about showing up anymore. It was about when
            you showed up. That obsession sent me down a rabbit hole of
            collecting data, trying to understand the conditions that produced
            those electric days.
          </p>
        </div>
      </section>

      <section className="about-row about-row-split reverse">
        <div className="about-card">
          <h2>The Problem I Kept Running Into</h2>
          <p>The data was out there, but it was hard to use.</p>
          <p>
            Run counts lived in PDFs. Everything was backward-looking and
            buried in tables. If you worked a normal 9-5, it was almost
            impossible to know where to fish on any given Saturday.
          </p>
          <p>
            The Run Report started as a personal tool to answer one simple
            question before heading out the door:
          </p>
          <p>"Is this the best river to fish today?"</p>
        </div>
        <div className="about-media">
          <img className="about-hero-image" src="/AboutUs2.jpeg" alt="Run report data source" />
        </div>
      </section>

      <section className="about-row about-row-split">
        <div className="about-media">
          <img className="about-hero-image" src="/AboutUs3.jpeg" alt="Future of TheRunReport" />
        </div>
        <div className="about-card">
          <h2>Where We are Headed</h2>
          <p>
            The goal of The Run Report is simple: help anglers make better
            decisions with less guesswork.
          </p>
          <p>But long-term, I want it to be more than just a website.
            As The Run Report grows, I hope to use it as a platform to:</p>
          <ul className="about-list">
            <li>Sponsor river cleanups</li>
            <li>Support habitat restoration efforts</li>
            <li>Advocate for more effective hatchery operations</li>
          </ul>
          <p>
            Current hatchery systems are often expensive and inefficient, and I
            believe we can do better with smarter data, better feedback loops,
            and more accountability. Healthy rivers and sustainable runs matter,
            not just for fishing today, but for fishing forever.
          </p>
          <p>
            The Run Report is built by an angler, for anglers, with respect for
            the rivers that make it all possible.
          </p>
          <p>
            - Thomas F
            <br />
            Founder
          </p>
        </div>
      </section>
    </div>
  );
}
