// src/pages/ChartsPage.jsx
import React, { useState, useEffect } from "react";
import { supabase, isSupabaseConfigured } from "../supabaseClient";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
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
    padding: "32px 24px 80px",
    color: "#eef3f5",
    fontFamily: '"Space Grotesk", "Montserrat", sans-serif',
    background:
      "radial-gradient(1200px 700px at 10% -10%, #1b3b4a 0%, #0e1820 45%, #0a0f13 100%)",
  };

  const panelStyle = {
    background: "rgba(10, 16, 20, 0.7)",
    border: "1px solid rgba(255, 255, 255, 0.08)",
    borderRadius: "16px",
    padding: "16px",
    backdropFilter: "blur(6px)",
  };
  const controlPanelStyle = {
    ...panelStyle,
    minHeight: "170px",
  };
  const placeholderChartStyle = {
    width: "100%",
    height: 400,
    marginTop: 40,
    borderRadius: "14px",
    boxShadow: "inset 0 0 0 1px rgba(255, 255, 255, 0.08)",
    background:
      "linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.01))," +
      "repeating-linear-gradient(90deg, rgba(255,255,255,0.08) 0, rgba(255,255,255,0.08) 1px, transparent 1px, transparent 72px)," +
      "repeating-linear-gradient(0deg, rgba(255,255,255,0.06) 0, rgba(255,255,255,0.06) 1px, transparent 1px, transparent 48px)",
  };

  return (
    <div style={containerStyle}>
      {!isSupabaseConfigured && (
        <div style={{ marginBottom: "16px", opacity: 0.8 }}>
          Charts are unavailable until Supabase environment variables are set.
        </div>
      )}
      <div style={{ textAlign: "center", marginBottom: "16px" }}>
        <h1 style={{ margin: 0, fontSize: "28px", letterSpacing: "0.5px" }}>
          {selectedRiver || "Select a river"}
        </h1>
      </div>

      <div className="chart-controls">
        <div
          style={{
            ...controlPanelStyle,
            width: "25%",
            maxWidth: "25%",
            flex: "0 0 25%",
            display: "flex",
            flexDirection: "column",
            gap: "12px",
          }}
        >
          <label style={{ display: "block", marginBottom: "8px", opacity: 0.8 }}>
            River
          </label>
          <select
            value={selectedRiver}
            onChange={(e) => setSelectedRiver(e.target.value)}
            style={{
              width: "100%",
              padding: "10px 12px",
              borderRadius: "10px",
              border: "1px solid rgba(255,255,255,0.15)",
              background: "#0b1216",
              color: "#eef3f5",
            }}
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
                  value={selectedDam}
                  onChange={(e) => setSelectedDam(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "10px 12px",
                    borderRadius: "10px",
                    border: "1px solid rgba(255,255,255,0.15)",
                    background: "#0b1216",
                    color: "#eef3f5",
                  }}
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

        <div
          style={{
            ...controlPanelStyle,
            width: "25%",
            maxWidth: "25%",
            flex: "0 0 25%",
          }}
        >
          <label style={{ display: "block", marginBottom: "8px", opacity: 0.8 }}>
            Fish or Flow
          </label>
          <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
            <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <input
                type="checkbox"
                checked={view === "Fish"}
                onChange={() => setView("Fish")}
              />
              Fish counts
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <input
                type="checkbox"
                checked={view === "Flow"}
                onChange={() => setView("Flow")}
              />
              Flow
            </label>
          </div>
        </div>
        {view === "Fish" && (
          <div
            style={{
              ...controlPanelStyle,
              width: "25%",
              maxWidth: "25%",
              flex: "0 0 25%",
            }}
          >
            <label style={{ marginRight: 8 }}>Species</label>
            <select
              value={selectedSpecies}
              onChange={(e) => setSelectedSpecies(e.target.value)}
              style={{
                width: "100%",
                marginTop: "8px",
                padding: "10px 12px",
                borderRadius: "10px",
                border: "1px solid rgba(255,255,255,0.15)",
                background: "#0b1216",
                color: "#eef3f5",
              }}
            >
              <option value="">Select species...</option>
              {species.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>
        )}
        {view === "Flow" && (
          <div
            style={{
              ...controlPanelStyle,
              width: "25%",
              maxWidth: "25%",
              flex: "0 0 25%",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
              <div>
                <label
                  style={{ display: "block", marginBottom: "6px", opacity: 0.8 }}
                >
                  Station
                </label>
                <select
                  value={selectedFlowSite}
                  onChange={(e) => setSelectedFlowSite(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "10px 12px",
                    borderRadius: "10px",
                    border: "1px solid rgba(255,255,255,0.15)",
                    background: "#0b1216",
                    color: "#eef3f5",
                  }}
                >
                  <option value="">Select station...</option>
                  {flowSites.map((site) => (
                    <option key={site} value={site}>
                      {site}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <div style={{ display: "flex", gap: "16px" }}>
                  <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                    <input
                      type="checkbox"
                      checked={showFlowStage}
                      onChange={() => {
                        setShowFlowStage(true);
                        setShowFlowCfs(false);
                      }}
                    />
                    Stage (ft)
                  </label>
                  <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                    <input
                      type="checkbox"
                      checked={showFlowCfs}
                      onChange={() => {
                        setShowFlowCfs(true);
                        setShowFlowStage(false);
                      }}
                    />
                    CFS
                  </label>
                </div>
              </div>

              <div>
                <div style={{ display: "flex", gap: "16px" }}>
                  <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                    <input
                      type="checkbox"
                      checked={flowWindow === "30d"}
                      onChange={() => setFlowWindow("30d")}
                    />
                    Last 30 days
                  </label>
                  <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                    <input
                      type="checkbox"
                      checked={flowWindow === "7d"}
                      onChange={() => setFlowWindow("7d")}
                    />
                    Last 7 days
                  </label>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      <div style={{ marginTop: "24px", ...panelStyle }}>
        {loading && <div style={{ opacity: 0.7 }}>Loading...</div>}
        {!selectedRiver && <div style={placeholderChartStyle} />}
        {selectedRiver && view === "Fish" && (
          <FishChart data={fishChartData} selectedRiver={selectedRiver} />
        )}
        {selectedRiver && view === "Flow" && (
          <FlowChart data={flowChartData} showCfs={showFlowCfs} showStage={showFlowStage} />
        )}
        {selectedRiver && !view && (
          <div style={{ opacity: 0.7 }}>
            Select a river and a data type to render the chart.
          </div>
        )}
      </div>
    </div>
  );
}

function FlowChart({ data, showCfs, showStage }) {
  if (!data || data.length === 0) {
    return <div style={{ opacity: 0.7 }}>No data available.</div>;
  }
  if (!showCfs && !showStage) return <div>Select a flow metric.</div>;

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

  const yAxisConfig =
    metricValues.length > 0 ? buildNiceTicks(metricValues, 6) : null;

  return (
    <div style={{ width: "100%", height: 400, marginTop: 40 }}>
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid stroke="#333" />

          <XAxis
            dataKey="timestampMs"
            type="number"
            domain={["dataMin", "dataMax"]}
            stroke="#ccc"
            tick={{ fill: "#ccc" }}
            tickFormatter={(value) =>
              new Date(value).toISOString().slice(5, 10)
            }
          />
          <YAxis
            stroke="#ccc"
            tick={{ fill: "#ccc" }}
            domain={yAxisConfig?.domain}
            ticks={yAxisConfig?.ticks}
          />

          <Tooltip
            content={({ active, payload }) => {
              if (!active || !payload || payload.length === 0) return null;
              return (
                <div
                  style={{
                    background: "#111",
                    border: "1px solid #444",
                    padding: "8px 10px",
                  }}
                >
                  {payload.map((entry) => (
                    <div key={entry.dataKey} style={{ color: entry.color }}>
                      {entry.name}: {entry.value}
                    </div>
                  ))}
                </div>
              );
            }}
          />

          <Legend verticalAlign="top" align="center" />

          {showCfs && (
            <Line
              type="monotone"
              dataKey="flow"
              stroke="#00e5ff"
              strokeWidth={3}
              dot={false}
              name="Flow (cfs)"
            />
          )}

          {showStage && (
            <Line
              type="monotone"
              dataKey="stage"
              stroke="#ff9800"
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

  return (
    <div style={{ width: "100%", height: 400, marginTop: 40 }}>
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid stroke="#333" />

          <XAxis
            dataKey="dayOfYear"
            type="number"
            stroke="#ccc"
            tick={{ fill: "#ccc" }}
            ticks={monthTicks.map((tick) => tick.day)}
            domain={[1, 366]}
            tickFormatter={(value) => {
              const match = monthTicks.find((tick) => tick.day === value);
              return match ? match.label : "";
            }}
          />
          <YAxis
            stroke="#ccc"
            tick={{ fill: "#ccc" }}
            label={{
              value: yAxisLabel,
              angle: -90,
              position: "insideLeft",
              fill: "#ccc",
            }}
          />

          <Tooltip
            contentStyle={{ background: "#111", border: "1px solid #444" }}
            labelStyle={{ color: "#fff" }}
          />

          <Legend />

          <Line
            type="monotone"
            dataKey="ten"
            stroke="#777"
            strokeWidth={2}
            dot={false}
            name="10-Year Avg"
          />

          <Line
            type="monotone"
            dataKey="last"
            stroke="#4fc3f7"
            strokeWidth={2}
            dot={false}
            name="Last Year"
          />

          <Line
            type="monotone"
            dataKey="current"
            stroke="#00e5ff"
            strokeWidth={3}
            dot={false}
            name="Current Year"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export default ChartsPage;
