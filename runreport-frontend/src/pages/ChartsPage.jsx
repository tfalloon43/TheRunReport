// src/pages/ChartsPage.jsx
import React, { useState, useEffect } from "react";
import { supabase } from "../supabaseClient";

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

function ChartsPage() {
  // ------------------------------------------------------------
  // STATE
  // ------------------------------------------------------------
  const [view, setView] = useState(""); // "Fish" or "Flow"

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
      console.log("Loading rivers...");

      const riverSet = new Set();

      async function fetchAllRivers(table) {
        const pageSize = 1000;
        let offset = 0;
        let hasMore = true;

        while (hasMore) {
          const { data, error } = await supabase
            .from(table)
            .select("river")
            .range(offset, offset + pageSize - 1);

          if (error) {
            console.error(`${table} river fetch error:`, error);
            return;
          }

          (data || []).forEach((row) => row.river && riverSet.add(row.river));

          if (!data || data.length < pageSize) {
            hasMore = false;
          } else {
            offset += pageSize;
          }
        }
      }

      await fetchAllRivers("Columbia_FishCounts");
      await fetchAllRivers("EscapementReport_PlotData");

      const riverList = [...riverSet].sort();
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
      if (!selectedRiver) {
        setDams([]);
        setSelectedDam("");
        return;
      }

      if (selectedRiver === "Columbia River" || selectedRiver === "Snake River") {
        const { data, error } = await supabase
          .from("Columbia_FishCounts")
          .select("dam_name")
          .eq("river", selectedRiver);

        if (error) {
          console.error("Error loading dams:", error);
          setDams([]);
          setSelectedDam("");
          return;
        }

        const uniqueDams = [...new Set(data.map((row) => row.dam_name))].sort();
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
      setSelectedSpecies(uniqueSpecies[0] || "");
    }

    loadSpecies();
  }, [selectedRiver, selectedDam]);

  // ------------------------------------------------------------
  // WHEN VIEW OR RIVER CHANGES → LOAD FLOW SITES
  // ------------------------------------------------------------
  useEffect(() => {
    async function loadFlowSites() {
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

  return (
    <div style={containerStyle}>
      <div style={{ textAlign: "center" }}>
        <h1 style={{ margin: 0, fontSize: "28px", letterSpacing: "0.5px" }}>
          {selectedRiver || "Select a river"}
        </h1>
      </div>

      <div style={{ display: "flex", flexWrap: "wrap", justifyContent: "space-between" }}>
        <div
          style={{
            ...panelStyle,
            flex: "0 0 27%",
            maxWidth: "27%",
            minWidth: "220px",
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
        </div>

        <div
          style={{
            ...panelStyle,
            flex: "0 0 27%",
            maxWidth: "27%",
            minWidth: "220px",
          }}
        >
          <label style={{ display: "block", marginBottom: "8px", opacity: 0.8 }}>
            Fish or Flow
          </label>
          <div style={{ display: "flex", gap: "16px", flexWrap: "wrap" }}>
            <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <input
                type="checkbox"
                checked={view === "Fish"}
                onChange={() => setView(view === "Fish" ? "" : "Fish")}
              />
              Fish counts
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <input
                type="checkbox"
                checked={view === "Flow"}
                onChange={() => setView(view === "Flow" ? "" : "Flow")}
              />
              Flow
            </label>
          </div>
        </div>
        {view === "Fish" && species.length > 0 && (
          <div
            style={{
              ...panelStyle,
              flex: "0 0 27%",
              maxWidth: "27%",
              minWidth: "220px",
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
              ...panelStyle,
              flex: "0 0 27%",
              maxWidth: "27%",
              minWidth: "220px",
            }}
          >
            <label style={{ display: "block", marginBottom: "8px", opacity: 0.8 }}>
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
        )}
      </div>

      {/* DAM (Columbia / Snake, Fish view only) */}
      {view === "Fish" &&
        (selectedRiver === "Columbia River" ||
          selectedRiver === "Snake River") && (
          <div style={{ marginTop: "16px", ...panelStyle }}>
            <label style={{ marginRight: 8 }}>Dam</label>
            <select
              value={selectedDam}
              onChange={(e) => setSelectedDam(e.target.value)}
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
              {dams.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </div>
        )}

      {/* FLOW CONTROLS */}
      {view === "Flow" && (
        <div style={{ marginTop: "16px", ...panelStyle }}>
          <label style={{ display: "block", marginBottom: "8px", opacity: 0.8 }}>
            Time window
          </label>
          <select
            value={flowWindow}
            onChange={(e) => setFlowWindow(e.target.value)}
            style={{
              width: "100%",
              padding: "10px 12px",
              borderRadius: "10px",
              border: "1px solid rgba(255,255,255,0.15)",
              background: "#0b1216",
              color: "#eef3f5",
              marginBottom: "12px",
            }}
          >
            <option value="7d">Last 7 days</option>
            <option value="30d">Last 30 days</option>
          </select>

          <label style={{ display: "block", marginBottom: "8px", opacity: 0.8 }}>
            Flow metrics
          </label>
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
      )}

      <div style={{ marginTop: "24px", ...panelStyle }}>
        {loading && <div style={{ opacity: 0.7 }}>Loading...</div>}
        {view === "Fish" && <FishChart data={fishChartData} />}
        {view === "Flow" && (
          <FlowChart data={flowChartData} showCfs={showFlowCfs} showStage={showFlowStage} />
        )}
        {!view && (
          <div style={{ opacity: 0.7 }}>
            Select a river and a data type to render the chart.
          </div>
        )}
      </div>
    </div>
  );
}

function FlowChart({ data, showCfs, showStage }) {
  if (!data || data.length === 0) return null;
  if (!showCfs && !showStage) return <div>Select a flow metric.</div>;

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
          <YAxis stroke="#ccc" tick={{ fill: "#ccc" }} />

          <Tooltip
            contentStyle={{ background: "#111", border: "1px solid #444" }}
            labelStyle={{ color: "#fff" }}
          />

          <Legend />

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

function FishChart({ data }) {
  if (!data || data.length === 0) return null;

  return (
    <div style={{ width: "100%", height: 400, marginTop: 40 }}>
      <ResponsiveContainer>
        <LineChart data={data}>
          <CartesianGrid stroke="#333" />

          <XAxis
            dataKey="date"
            stroke="#ccc"
            tick={{ fill: "#ccc" }}
            interval={20}
          />
          <YAxis stroke="#ccc" tick={{ fill: "#ccc" }} />

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
