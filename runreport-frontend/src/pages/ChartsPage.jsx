// src/pages/ChartsPage.jsx
import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { supabase, isSupabaseConfigured } from "../supabaseClient";
import { useAuth } from "../AuthContext";
import { initPaddle } from "../utils/paddle";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Label,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

function toDayOfYear(dateString) {
  if (!dateString) return null;
  const normalized = dateString.replace("/", "-");
  const parts = normalized.split("-");
  if (parts.length !== 2) return null;
  const month = Number(parts[0]);
  const day = Number(parts[1]);
  if (!month || !day) return null;
  const year = 2020;
  const date = new Date(Date.UTC(year, month - 1, day));
  const start = new Date(Date.UTC(year, 0, 1));
  const diffDays = Math.round((date - start) / 86400000);
  return diffDays + 1;
}

const monthTicks = [
  { label: "01/01", day: 1 },
  { label: "02/01", day: 32 },
  { label: "03/01", day: 61 },
  { label: "04/01", day: 92 },
  { label: "05/01", day: 122 },
  { label: "06/01", day: 153 },
  { label: "07/01", day: 183 },
  { label: "08/01", day: 214 },
  { label: "09/01", day: 245 },
  { label: "10/01", day: 275 },
  { label: "11/01", day: 306 },
  { label: "12/01", day: 336 },
  { label: "12/31", day: 366 },
];

function ChartsPage() {
  // ------------------------------------------------------------
  // STATE
  // ------------------------------------------------------------
  const { session } = useAuth();
  const navigate = useNavigate();
  const appUrl = import.meta.env.VITE_APP_URL || window.location.origin;
  const [view, setView] = useState("Fish"); // "Fish" or "Flow"

  const [rivers, setRivers] = useState([]);
  const [selectedRiver, setSelectedRiver] = useState("");

  const [dams, setDams] = useState([]);
  const [selectedDam, setSelectedDam] = useState("");

  const [species, setSpecies] = useState([]);
  const [selectedSpecies, setSelectedSpecies] = useState("");

  const [flowSites, setFlowSites] = useState([]);
  const [selectedFlowSite, setSelectedFlowSite] = useState("");

  const [flowWindow, setFlowWindow] = useState("30d");
  const [showFlowCfs, setShowFlowCfs] = useState(true);
  const [showFlowStage, setShowFlowStage] = useState(false);

  const [fishChartData, setFishChartData] = useState([]);
  const [flowChartData, setFlowChartData] = useState([]);

  const [loading, setLoading] = useState(false);
  const [subscriptionStatus, setSubscriptionStatus] = useState(null);
  const [subscriptionLoading, setSubscriptionLoading] = useState(false);
  const [billingError, setBillingError] = useState("");
  // ------------------------------------------------------------
  // LOAD RIVERS ONCE
  // ------------------------------------------------------------
  useEffect(() => {
    async function loadRivers() {
      if (!supabase) return;
      console.log("Loading rivers...");

      const { data, error } = await supabase
        .from("Rivers")
        .select("river")
        .order("river", { ascending: true });

      if (error) {
        console.error("Rivers fetch error:", error);
        return;
      }

      const riverList = (data || [])
        .map((row) => row.river)
        .filter(Boolean);
      console.log("Final river list:", riverList);
      setRivers(riverList);
    }

    loadRivers();
  }, []);

  // ------------------------------------------------------------
  // WHEN RIVER CHANGES → LOAD DAMS (COLUMBIA / SNAKE ONLY)
  // ------------------------------------------------------------
  useEffect(() => {
    async function loadDams() {
      if (!supabase) return;
      if (!selectedRiver) {
        setDams([]);
        setSelectedDam("");
        return;
      }

      if (selectedRiver === "Columbia River" || selectedRiver === "Snake River") {
        const damSet = new Set();
        const pageSize = 1000;
        let offset = 0;
        let hasMore = true;

        while (hasMore) {
          const { data, error } = await supabase
            .from("Columbia_FishCounts")
            .select("dam_name")
            .eq("river", selectedRiver)
            .range(offset, offset + pageSize - 1);

          if (error) {
            console.error("Error loading dams:", error);
            setDams([]);
            setSelectedDam("");
            return;
          }

          (data || []).forEach((row) => {
            if (row.dam_name) damSet.add(row.dam_name);
          });

          if (!data || data.length < pageSize) {
            hasMore = false;
          } else {
            offset += pageSize;
          }
        }

        const uniqueDams = [...damSet].sort();
        setDams(uniqueDams);
        setSelectedDam(uniqueDams[0] || "");
      } else {
        setDams([]);
        setSelectedDam("");
      }
    }

    loadDams();
  }, [selectedRiver]);

  // ------------------------------------------------------------
  // WHEN RIVER OR DAM CHANGES → LOAD SPECIES (FIXED TO Species_Plot)
  // ------------------------------------------------------------
  useEffect(() => {
    async function loadSpecies() {
      if (!supabase) return;
      if (!selectedRiver) {
        setSpecies([]);
        setSelectedSpecies("");
        return;
      }

      let fromTable;
      let query;

      // Columbia + Snake use columbia_data
      if (selectedRiver === "Columbia River" || selectedRiver === "Snake River") {
        fromTable = "Columbia_FishCounts";
        query = supabase
          .from(fromTable)
          .select("Species_Plot")
          .eq("river", selectedRiver);

        if (selectedDam) {
          query = query.eq("dam_name", selectedDam);
        }
      } else {
        // All other rivers use pdf_data
        fromTable = "EscapementReport_PlotData";
        query = supabase
          .from(fromTable)
          .select("Species_Plot")
          .eq("river", selectedRiver);
      }

      const { data, error } = await query;

      if (error) {
        console.error("Error loading species from", fromTable, ":", error);
        setSpecies([]);
        setSelectedSpecies("");
        return;
      }

      const uniqueSpecies = [
        ...new Set((data || []).map((row) => row.Species_Plot)),
      ].sort();

      console.log("Loaded species for", selectedRiver, ":", uniqueSpecies);

      setSpecies(uniqueSpecies);
      setSelectedSpecies((prev) =>
        uniqueSpecies.includes(prev) ? prev : uniqueSpecies[0] || ""
      );
    }

    loadSpecies();
  }, [selectedRiver, selectedDam]);

  // ------------------------------------------------------------
  // WHEN VIEW OR RIVER CHANGES → LOAD FLOW SITES
  // ------------------------------------------------------------
  useEffect(() => {
    async function loadFlowSites() {
      if (!supabase) return;
      if (view !== "Flow" || !selectedRiver) {
        setFlowSites([]);
        setSelectedFlowSite("");
        return;
      }

      const { data: usgs } = await supabase
        .from("USGS_flows")
        .select("site_name, river");

      const { data: noaa } = await supabase
        .from("NOAA_flows")
        .select("site_name, river");

      const combined = [...(usgs || []), ...(noaa || [])].filter(
        (r) => r.river === selectedRiver
      );

      const unique = [...new Set(combined.map((r) => r.site_name))].sort();
      setFlowSites(unique);
      setSelectedFlowSite(unique[0] || "");
    }

    loadFlowSites();
  }, [view, selectedRiver]);

  // ------------------------------------------------------------
  // LOAD FISH CHART DATA (USES Species_Plot)
  // ------------------------------------------------------------
  async function loadFishChart() {
    if (!supabase) return;
    if (!selectedRiver || !selectedSpecies) return;
    setLoading(true);

    try {
      // Columbia + Snake: columbia_data
      if (selectedRiver === "Columbia River" || selectedRiver === "Snake River") {
        const { data, error } = await supabase
          .from("Columbia_FishCounts")
          .select(
            "Dates, Daily_Count_Current_Year, Daily_Count_Last_Year, Ten_Year_Average_Daily_Count, dam_name, Species_Plot"
          )
          .eq("river", selectedRiver)
          .eq("dam_name", selectedDam)
          .eq("Species_Plot", selectedSpecies);

        if (error) {
          console.error("Error loading fish chart (columbia_data):", error);
          setFishChartData([]);
          return;
        }

        const points = (data || []).map((row) => ({
          date: row.Dates, // e.g. "01/01"
          dayOfYear: toDayOfYear(row.Dates),
          current: row.Daily_Count_Current_Year,
          last: row.Daily_Count_Last_Year,
          ten: row.Ten_Year_Average_Daily_Count,
        }));

        setFishChartData(points);
      } else {
        // Other rivers: pdf_data
        const { data, error } = await supabase
          .from("EscapementReport_PlotData")
          .select("MM-DD, current_year, previous_year, 10_year, Species_Plot")
          .eq("river", selectedRiver)
          .eq("Species_Plot", selectedSpecies);

        if (error) {
          console.error("Error loading fish chart (pdf_data):", error);
          setFishChartData([]);
          return;
        }

        const points = (data || []).map((row) => ({
          date: row["MM-DD"],
          dayOfYear: toDayOfYear(row["MM-DD"]),
          current: row.current_year,
          last: row.previous_year,
          ten: row["10_year"],
        }));

        setFishChartData(points);
      }
    } finally {
      setLoading(false);
    }
  }

  // ------------------------------------------------------------
  // LOAD FLOW CHART DATA
  // ------------------------------------------------------------
  async function loadFlowChart() {
    if (!supabase) return;
    if (!selectedRiver || !selectedFlowSite) return;
    setLoading(true);

    try {
      async function fetchAllFlows(table) {
        const pageSize = 1000;
        let offset = 0;
        let allRows = [];
        let hasMore = true;

        while (hasMore) {
          const { data, error } = await supabase
            .from(table)
            .select("timestamp, window, site_name, river, flow_cfs, stage_ft")
            .eq("river", selectedRiver)
            .eq("site_name", selectedFlowSite)
            .eq("window", flowWindow)
            .order("timestamp", { ascending: true })
            .range(offset, offset + pageSize - 1);

          if (error) {
            console.error(`${table} flow fetch error:`, error);
            break;
          }

          allRows = allRows.concat(data || []);

          if (!data || data.length < pageSize) {
            hasMore = false;
          } else {
            offset += pageSize;
          }
        }

        return allRows;
      }

      const [usgs, noaa] = await Promise.all([
        fetchAllFlows("USGS_flows"),
        fetchAllFlows("NOAA_flows"),
      ]);

      const combined = [...(usgs || []), ...(noaa || [])];

      const points = combined
        .map((row) => ({
          timestamp: row.timestamp,
          timestampMs: new Date(row.timestamp).getTime(),
          flow: row.flow_cfs,
          stage: row.stage_ft,
        }))
        .sort(
          (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
        );

      setFlowChartData(points);
    } finally {
      setLoading(false);
    }
  }

  // ------------------------------------------------------------
  // HANDLER FOR BUTTON
  // ------------------------------------------------------------
  useEffect(() => {
    if (view === "Fish") {
      loadFishChart();
    } else if (view === "Flow") {
      loadFlowChart();
    }
  }, [view, selectedRiver, selectedSpecies, selectedDam, selectedFlowSite, flowWindow]);

  // ------------------------------------------------------------
  // RENDER
  // ------------------------------------------------------------

  const containerStyle = {
    minHeight: "100vh",
    padding: "32px 24px 40px",
    color: "rgb(var(--color-text))",
    fontFamily: "var(--font-base)",
    background: "rgb(var(--color-bg))",
  };

  const panelStyle = {
    background: "rgba(var(--color-surface), 0.9)",
    border: "1px solid rgba(var(--color-text), 0.08)",
    borderRadius: "16px",
    padding: "16px",
  };
  const chartPanelStyle = {
    ...panelStyle,
    position: "relative",
  };
  const controlPanelStyle = {
    ...panelStyle,
    minHeight: "170px",
  };
  const placeholderChartStyle = {
    width: "100%",
    height: 400,
    borderRadius: "14px",
    boxShadow: "inset 0 0 0 1px rgba(var(--color-text), 0.08)",
    background:
      "linear-gradient(180deg, rgba(var(--color-text), 0.05), rgba(var(--color-text), 0.01))," +
      "repeating-linear-gradient(90deg, rgba(var(--color-text), 0.08) 0, rgba(var(--color-text), 0.08) 1px, transparent 1px, transparent 72px)," +
      "repeating-linear-gradient(0deg, rgba(var(--color-text), 0.06) 0, rgba(var(--color-text), 0.06) 1px, transparent 1px, transparent 48px)",
  };
  const hasFishData = fishChartData && fishChartData.length > 0;
  const hasFlowValues = (flowChartData || []).some((row) => {
    if (showFlowCfs && Number.isFinite(row.flow)) return true;
    if (showFlowStage && Number.isFinite(row.stage)) return true;
    return false;
  });
  const hasActiveSubscription =
    subscriptionStatus === "active"
    || subscriptionStatus === "trialing"
    || subscriptionStatus === "complete";
  const isLocked = !session || !hasActiveSubscription || subscriptionLoading;

  useEffect(() => {
    let mounted = true;
    async function loadSubscription() {
      if (!supabase || !session) {
        if (mounted) {
          setSubscriptionStatus(null);
          setSubscriptionLoading(false);
        }
        return;
      }
      setSubscriptionLoading(true);
      const { data, error } = await supabase
        .from("paddle_subscriptions")
        .select("status")
        .eq("user_id", session.user.id)
        .maybeSingle();

      if (mounted) {
        if (error) {
          console.error("Subscription lookup error:", error);
        }
        setSubscriptionStatus(data?.status ?? null);
        setSubscriptionLoading(false);
      }
    }

    loadSubscription();
    return () => {
      mounted = false;
    };
  }, [session]);

  useEffect(() => {
    if (!session || hasActiveSubscription || subscriptionLoading) return;
    let attempts = 0;
    const maxAttempts = 15;
    const interval = setInterval(async () => {
      attempts += 1;
      const { data } = await supabase
        .from("paddle_subscriptions")
        .select("status")
        .eq("user_id", session.user.id)
        .maybeSingle();
      const nextStatus = data?.status?.toLowerCase() || null;
      if (nextStatus) {
        setSubscriptionStatus(nextStatus);
      }
      if (
        nextStatus === "active" ||
        nextStatus === "trialing" ||
        nextStatus === "complete" ||
        attempts >= maxAttempts
      ) {
        clearInterval(interval);
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [session, hasActiveSubscription, subscriptionLoading]);

  async function handleStartSubscription() {
    setBillingError("");
    const priceId = import.meta.env.VITE_PADDLE_PRICE_ID;
    if (!priceId) {
      setBillingError("Missing Paddle price id.");
      return;
    }
    try {
      const paddle = await initPaddle();
      console.log("Paddle checkout URLs", {
        success_url: `${appUrl}/charts`,
        close_url: `${appUrl}/charts`,
      });
      paddle.Checkout.open({
        items: [{ priceId, quantity: 1 }],
        customer: {
          email: session?.user?.email,
        },
        customData: {
          user_id: session?.user?.id,
        },
        settings: {
          successUrl: `${appUrl}/charts`,
          closeUrl: `${appUrl}/charts`,
        },
      });
    } catch (error) {
      setBillingError(error.message || "Unable to start checkout.");
    }
  }

  return (
    <div style={containerStyle}>
      {!isSupabaseConfigured && (
        <div style={{ marginBottom: "16px", opacity: 0.8 }}>
          Charts are unavailable until Supabase environment variables are set.
        </div>
      )}
      <div className="chart-controls" />

      <div style={{ marginTop: "24px", ...chartPanelStyle }}>
        <div
          style={{
            marginBottom: "16px",
            ...panelStyle,
            border: "none",
            background: "transparent",
            padding: 0,
          }}
        >
          <div style={{ display: "flex", gap: "16px", alignItems: "center" }}>
            <div style={{ flex: 1 }}>
              <div className="toggle-pill" role="group" aria-label="Fish or flow">
                <button
                  type="button"
                  className={`toggle-pill-button ${view === "Fish" ? "is-active" : ""}`}
                  onClick={() => setView("Fish")}
                  disabled={isLocked}
                >
                  Fish Counts
                </button>
                <button
                  type="button"
                  className={`toggle-pill-button ${view === "Flow" ? "is-active" : ""}`}
                  onClick={() => setView("Flow")}
                  disabled={isLocked}
                >
                  River Flow
                </button>
              </div>
            </div>
            <div style={{ flex: 1 }}>
              <select
                className="chart-select"
                value={selectedRiver}
                onChange={(e) => setSelectedRiver(e.target.value)}
                disabled={isLocked}
              >
                <option value="">Select river...</option>
                {rivers.map((r) => (
                  <option key={r} value={r}>
                    {r}
                  </option>
                ))}
              </select>
              {view === "Fish" &&
                (selectedRiver === "Columbia River" ||
                  selectedRiver === "Snake River") && (
                  <div style={{ marginTop: "12px" }}>
                    <select
                      className="chart-select"
                      value={selectedDam}
                      onChange={(e) => setSelectedDam(e.target.value)}
                      disabled={isLocked}
                    >
                      {dams.map((d) => (
                        <option key={d} value={d}>
                          {d}
                        </option>
                      ))}
                    </select>
                  </div>
                )}
            </div>
            <div style={{ flex: 1 }}>
              {view === "Fish" && (
                <select
                  className="chart-select"
                  value={selectedSpecies}
                  onChange={(e) => setSelectedSpecies(e.target.value)}
                  disabled={isLocked}
                >
                  <option value="">Select species...</option>
                  {species.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              )}
              {view === "Flow" && (
                <select
                  className="chart-select"
                  value={selectedFlowSite}
                  onChange={(e) => setSelectedFlowSite(e.target.value)}
                  disabled={isLocked}
                >
                  <option value="">Select station...</option>
                  {flowSites.map((site) => (
                    <option key={site} value={site}>
                      {site}
                    </option>
                  ))}
                </select>
              )}
            </div>
          </div>
        </div>
        {(!selectedRiver || (loading && !(hasFishData || hasFlowValues))) && (
          <div style={placeholderChartStyle} />
        )}
        {selectedRiver && view === "Fish" && hasFishData && (
          <FishChart data={fishChartData} selectedRiver={selectedRiver} />
        )}
        {selectedRiver && view === "Flow" && hasFlowValues && (
          <FlowChart
            data={flowChartData}
            showCfs={showFlowCfs}
            showStage={showFlowStage}
            hasData={hasFlowValues}
          />
        )}
        {selectedRiver &&
          !loading &&
          ((view === "Fish" && (!fishChartData || fishChartData.length === 0)) ||
            (view === "Flow" && !hasFlowValues)) && (
            <div style={{ ...placeholderChartStyle, position: "relative" }}>
              <div
                style={{
                  position: "absolute",
                  inset: 0,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  color: "rgba(var(--color-text), 0.7)",
                  fontSize: "0.95rem",
                }}
              >
                No data available.
              </div>
            </div>
          )}
        {loading && selectedRiver && (
          <div style={{ position: "absolute", bottom: 16, right: 16, opacity: 0.7 }}>
            Loading...
          </div>
        )}
        {selectedRiver && !view && !loading && (
          <div style={{ opacity: 0.7 }}>
            Select a river and a data type to render the chart.
          </div>
        )}
        {selectedRiver && view === "Fish" && !loading && (
          <div
            style={{
              marginTop: "16px",
              ...panelStyle,
              border: "none",
              background: "transparent",
              padding: 0,
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "center",
                gap: "18px",
                color: "rgba(var(--color-text), 0.7)",
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                <span
                  aria-hidden="true"
                  style={{
                    width: "18px",
                    height: "2px",
                    background: "rgb(var(--chart-line-1))",
                    display: "inline-block",
                    borderRadius: "999px",
                  }}
                />
                <span>Current Year</span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                <span
                  aria-hidden="true"
                  style={{
                    width: "18px",
                    height: "2px",
                    background: "rgb(var(--chart-line-2))",
                    display: "inline-block",
                    borderRadius: "999px",
                  }}
                />
                <span>Last Year</span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                <span
                  aria-hidden="true"
                  style={{
                    width: "18px",
                    height: "2px",
                    background: "rgb(var(--chart-line-3))",
                    display: "inline-block",
                    borderRadius: "999px",
                  }}
                />
                <span>10-Year Avg</span>
              </div>
            </div>
          </div>
        )}
        {selectedRiver && view === "Flow" && (
          <div
            style={{
              marginTop: "16px",
              ...panelStyle,
              border: "none",
              background: "transparent",
              padding: 0,
            }}
          >
            <div style={{ display: "flex", justifyContent: "center", gap: "16px" }}>
              <div className="toggle-pill" role="group" aria-label="Flow metric">
                <button
                  type="button"
                  className={`toggle-pill-button ${showFlowStage ? "is-active" : ""}`}
                  onClick={() => {
                    setShowFlowStage(true);
                    setShowFlowCfs(false);
                  }}
                  disabled={isLocked}
                >
                  Stage (ft)
                </button>
                <button
                  type="button"
                  className={`toggle-pill-button ${showFlowCfs ? "is-active" : ""}`}
                  onClick={() => {
                    setShowFlowCfs(true);
                    setShowFlowStage(false);
                  }}
                  disabled={isLocked}
                >
                  CFS
                </button>
              </div>
              <div className="toggle-pill" role="group" aria-label="Flow window">
                <button
                  type="button"
                  className={`toggle-pill-button ${flowWindow === "30d" ? "is-active" : ""}`}
                  onClick={() => setFlowWindow("30d")}
                  disabled={isLocked}
                >
                  30 days
                </button>
                <button
                  type="button"
                  className={`toggle-pill-button ${flowWindow === "7d" ? "is-active" : ""}`}
                  onClick={() => setFlowWindow("7d")}
                  disabled={isLocked}
                >
                  7 days
                </button>
              </div>
            </div>
          </div>
        )}
        {isLocked && (
          <div className="chart-lock-overlay" role="status">
            {!session ? (
              <button
                type="button"
                className="chart-lock-button"
                onClick={() => navigate("/login")}
              >
                Log in to see run info
              </button>
            ) : subscriptionLoading ? (
              <div className="chart-lock-message">Checking subscription...</div>
            ) : (
              <div className="chart-lock-stack">
                <button
                  type="button"
                  className="chart-lock-button"
                  onClick={handleStartSubscription}
                >
                  Subscribe to see run info
                </button>
                {billingError && (
                  <div className="chart-lock-error">{billingError}</div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function FlowChart({ data, showCfs, showStage, hasData }) {
  if (!hasData) return null;
  if (!showCfs && !showStage) return <div>Select a flow metric.</div>;
  const flowLabel = showCfs ? "Flow (cfs)" : "Stage (ft)";
  const flowColor = showCfs
    ? "rgb(var(--chart-line-1))"
    : "rgb(var(--chart-line-2))";

  function niceNum(range, round) {
    const exponent = Math.floor(Math.log10(range));
    const fraction = range / Math.pow(10, exponent);
    let niceFraction;

    if (round) {
      if (fraction < 1.5) niceFraction = 1;
      else if (fraction < 3) niceFraction = 2;
      else if (fraction < 7) niceFraction = 5;
      else niceFraction = 10;
    } else {
      if (fraction <= 1) niceFraction = 1;
      else if (fraction <= 2) niceFraction = 2;
      else if (fraction <= 5) niceFraction = 5;
      else niceFraction = 10;
    }

    return niceFraction * Math.pow(10, exponent);
  }

  function buildNiceTicks(values, targetCount) {
    const min = Math.min(...values);
    const max = Math.max(...values);
    if (!Number.isFinite(min) || !Number.isFinite(max)) return null;

    if (min === max) {
      const step = min === 0 ? 1 : Math.abs(min) * 0.1;
      const ticks = Array.from(
        { length: targetCount },
        (_, i) => min - step + step * i
      );
      return { ticks, domain: [ticks[0], ticks[ticks.length - 1]] };
    }

    const range = max - min;
    const tickSpacing = niceNum(range / (targetCount - 1), true);
    const niceMin = Math.floor(min / tickSpacing) * tickSpacing;
    const niceMax = Math.ceil(max / tickSpacing) * tickSpacing;
    const tickCount =
      Math.round((niceMax - niceMin) / tickSpacing) + 1;

    const ticks = Array.from({ length: tickCount }, (_, i) =>
      Number((niceMin + i * tickSpacing).toFixed(6))
    );

    return { ticks, domain: [ticks[0], ticks[ticks.length - 1]] };
  }

  const metricValues = data
    .flatMap((row) => {
      const values = [];
      if (showCfs) values.push(row.flow);
      if (showStage) values.push(row.stage);
      return values;
    })
    .filter((value) => Number.isFinite(value));

  if (metricValues.length === 0) return null;

  const yAxisConfig = buildNiceTicks(metricValues, 6);

  return (
    <div style={{ width: "100%", height: 400 }}>
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid stroke="rgba(var(--color-text), 0.15)" />

          <XAxis
            dataKey="timestampMs"
            type="number"
            domain={["dataMin", "dataMax"]}
            stroke="rgba(var(--color-text), 0.7)"
            tick={{ fill: "rgba(var(--color-text), 0.7)" }}
            tickFormatter={(value) =>
              new Date(value).toISOString().slice(5, 10)
            }
          />
          <YAxis
            stroke="rgba(var(--color-text), 0.7)"
            tick={{ fill: "rgba(var(--color-text), 0.7)" }}
            domain={yAxisConfig?.domain}
            ticks={yAxisConfig?.ticks}
            label={{
              value: flowLabel,
              angle: -90,
              position: "insideLeft",
              fill: "rgba(var(--color-text), 0.7)",
              offset: 0,
              textAnchor: "middle",
              dominantBaseline: "middle",
            }}
          />

          <Tooltip
            content={({ active, payload }) => {
              if (!active || !payload || payload.length === 0) return null;
              const byKey = Object.fromEntries(
                payload.map((entry) => [entry.dataKey, entry])
              );
              const order = ["flow", "stage"];
              const ts = payload[0]?.payload?.timestamp;
              const tsLabel = ts ? new Date(ts).toLocaleString() : "";
              return (
                <div
                  style={{
                    background: "rgb(var(--color-bg))",
                    border: "1px solid rgba(var(--color-text), 0.2)",
                    padding: "8px 10px",
                  }}
                >
                  {order
                    .map((key) => byKey[key])
                    .filter(Boolean)
                    .map((entry) => (
                      <div key={entry.dataKey} style={{ color: entry.color }}>
                        {entry.name}: {entry.value}
                      </div>
                    ))}
                  {tsLabel && (
                    <div style={{ color: "rgb(var(--color-text))", marginTop: 6 }}>
                      {tsLabel}
                    </div>
                  )}
                </div>
              );
            }}
          />

          {showCfs && (
            <Line
              type="monotone"
              dataKey="flow"
              stroke="rgb(var(--chart-line-1))"
              strokeWidth={3}
              dot={false}
              name="Flow (cfs)"
            />
          )}

          {showStage && (
            <Line
              type="monotone"
              dataKey="stage"
              stroke="rgb(var(--chart-line-2))"
              strokeWidth={2}
              dot={false}
              name="Stage (ft)"
            />
          )}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

function FishChart({ data, selectedRiver }) {
  if (!data || data.length === 0) return null;
  const isColumbiaOrSnake =
    selectedRiver === "Columbia River" || selectedRiver === "Snake River";
  const yAxisLabel = isColumbiaOrSnake ? "Fish per day" : "Fish per week";
  const lineColors = {
    current: "rgb(var(--chart-line-1))",
    last: "rgb(var(--chart-line-2))",
    ten: "rgb(var(--chart-line-3))",
  };
  const legendItems = [
    { value: "Current Year", id: "current", color: lineColors.current },
    { value: "Last Year", id: "last", color: lineColors.last },
    { value: "10-Year Avg", id: "ten", color: lineColors.ten },
  ];

  return (
    <div style={{ width: "100%", height: 400 }}>
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid stroke="rgba(var(--color-text), 0.15)" />

          <XAxis
            dataKey="dayOfYear"
            type="number"
            stroke="rgba(var(--color-text), 0.7)"
            tick={{ fill: "rgba(var(--color-text), 0.7)" }}
            ticks={monthTicks.map((tick) => tick.day)}
            domain={[1, 366]}
            tickFormatter={(value) => {
              const match = monthTicks.find((tick) => tick.day === value);
              return match ? match.label : "";
            }}
          />
          <YAxis
            stroke="rgba(var(--color-text), 0.7)"
            tick={{ fill: "rgba(var(--color-text), 0.7)" }}
            label={{
              value: yAxisLabel,
              angle: -90,
              position: "insideLeft",
              fill: "rgba(var(--color-text), 0.7)",
              offset: 0,
              textAnchor: "middle",
              dominantBaseline: "middle",
            }}
          />

          <Tooltip
            content={({ active, payload }) => {
              if (!active || !payload || payload.length === 0) return null;
              const byKey = Object.fromEntries(
                payload.map((entry) => [entry.dataKey, entry])
              );
              const dateLabel = payload[0]?.payload?.date || "";
              const order = ["current", "last", "ten"];
              return (
                <div
                  style={{
                    background: "rgb(var(--color-bg))",
                    border: "1px solid rgba(var(--color-text), 0.2)",
                    padding: "8px 10px",
                  }}
                >
                  <div style={{ color: "rgb(var(--color-text))", marginBottom: 6 }}>
                    {dateLabel}
                  </div>
                  {order
                    .map((key) => byKey[key])
                    .filter(Boolean)
                    .map((entry) => (
                      <div key={entry.dataKey} style={{ color: entry.color }}>
                        {entry.name}: {entry.value}
                      </div>
                    ))}
                </div>
              );
            }}
          />

          <Legend content={() => null} />

          <Line
            type="monotone"
            dataKey="ten"
            stroke={lineColors.ten}
            strokeWidth={2}
            dot={false}
            name="10-Year Avg"
          />

          <Line
            type="monotone"
            dataKey="last"
            stroke={lineColors.last}
            strokeWidth={2}
            dot={false}
            name="Last Year"
          />

          <Line
            type="monotone"
            dataKey="current"
            stroke={lineColors.current}
            strokeWidth={2}
            dot={false}
            name="Current Year"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export default ChartsPage;
