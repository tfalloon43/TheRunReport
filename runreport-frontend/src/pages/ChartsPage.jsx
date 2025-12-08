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

  const [flowWindow, setFlowWindow] = useState("7d");

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

      // columbia_data
      const { data: col, error: colErr } = await supabase
        .from("columbia_data")
        .select("river");
      if (colErr) console.error("columbia_data error:", colErr);
      if (col) col.forEach((r) => r.river && riverSet.add(r.river));

      // pdf_data
      const { data: pdf, error: pdfErr } = await supabase
        .from("pdf_data")
        .select("river");
      if (pdfErr) console.error("pdf_data error:", pdfErr);
      if (pdf) pdf.forEach((r) => r.river && riverSet.add(r.river));

      // USGS_flow
      const { data: usgs, error: usgsErr } = await supabase
        .from("USGS_flow")
        .select("river");
      if (usgsErr) console.error("USGS_flow error:", usgsErr);
      if (usgs) usgs.forEach((r) => r.river && riverSet.add(r.river));

      // NOAA_flow
      const { data: noaa, error: noaaErr } = await supabase
        .from("NOAA_flow")
        .select("river");
      if (noaaErr) console.error("NOAA_flow error:", noaaErr);
      if (noaa) noaa.forEach((r) => r.river && riverSet.add(r.river));

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
          .from("columbia_data")
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
        fromTable = "columbia_data";
        query = supabase
          .from(fromTable)
          .select("Species_Plot")
          .eq("river", selectedRiver);

        if (selectedDam) {
          query = query.eq("dam_name", selectedDam);
        }
      } else {
        // All other rivers use pdf_data
        fromTable = "pdf_data";
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
        .from("USGS_flow")
        .select("site_name, river");

      const { data: noaa } = await supabase
        .from("NOAA_flow")
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
          .from("columbia_data")
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
          .from("pdf_data")
          .select("MM-DD, metric_type, value, Species_Plot")
          .eq("river", selectedRiver)
          .eq("Species_Plot", selectedSpecies);

        if (error) {
          console.error("Error loading fish chart (pdf_data):", error);
          setFishChartData([]);
          return;
        }

        // Pivot metric_type → {ten, last, current} per MM-DD
        const grouped = {};

        (data || []).forEach((row) => {
          const d = row["MM-DD"];
          if (!grouped[d]) {
            grouped[d] = { date: d, current: null, last: null, ten: null };
          }

          if (row.metric_type === "current_year") grouped[d].current = row.value;
          if (row.metric_type === "previous_year") grouped[d].last = row.value;
          if (row.metric_type === "ten_year_avg") grouped[d].ten = row.value;
        });

        const points = Object.values(grouped);
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
      const { data: usgs } = await supabase
        .from("USGS_flow")
        .select("timestamp, window, site_name, river, flow_cfs, stage_ft")
        .eq("river", selectedRiver)
        .eq("site_name", selectedFlowSite)
        .eq("window", flowWindow);

      const { data: noaa } = await supabase
        .from("NOAA_flow")
        .select("timestamp, window, site_name, river, flow_cfs, stage_ft")
        .eq("river", selectedRiver)
        .eq("site_name", selectedFlowSite)
        .eq("window", flowWindow);

      const combined = [...(usgs || []), ...(noaa || [])];

      const points = combined
        .map((row) => ({
          timestamp: row.timestamp,
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
  async function handleLoadChart() {
    if (view === "Fish") {
      await loadFishChart();
    } else if (view === "Flow") {
      await loadFlowChart();
    }
  }

  // ------------------------------------------------------------
  // RENDER
  // ------------------------------------------------------------

  return (
    <div style={{ padding: "40px", color: "white" }}>
      <h1>Charts</h1>

      {/* VIEW SELECTOR */}
      <div style={{ marginBottom: "16px" }}>
        <label style={{ marginRight: 8 }}>View:</label>
        <select value={view} onChange={(e) => setView(e.target.value)}>
          <option value="">Select...</option>
          <option value="Fish">Fish counts</option>
          <option value="Flow">Flow</option>
        </select>
      </div>

      {/* RIVER */}
      {view && (
        <div style={{ marginBottom: "16px" }}>
          <label style={{ marginRight: 8 }}>River:</label>
          <select
            value={selectedRiver}
            onChange={(e) => setSelectedRiver(e.target.value)}
          >
            <option value="">Select...</option>
            {rivers.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </div>
      )}

      {/* DAM (Columbia / Snake, Fish view only) */}
      {view === "Fish" &&
        (selectedRiver === "Columbia River" ||
          selectedRiver === "Snake River") && (
          <div style={{ marginBottom: "16px" }}>
            <label style={{ marginRight: 8 }}>Dam:</label>
            <select
              value={selectedDam}
              onChange={(e) => setSelectedDam(e.target.value)}
            >
              {dams.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </div>
        )}

      {/* SPECIES (Fish view) */}
      {view === "Fish" && species.length > 0 && (
        <div style={{ marginBottom: "16px" }}>
          <label style={{ marginRight: 8 }}>Species:</label>
          <select
            value={selectedSpecies}
            onChange={(e) => setSelectedSpecies(e.target.value)}
          >
            {species.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>
      )}

      {/* FLOW CONTROLS */}
      {view === "Flow" && (
        <>
          {flowSites.length > 0 && (
            <div style={{ marginBottom: "16px" }}>
              <label style={{ marginRight: 8 }}>Flow Site:</label>
              <select
                value={selectedFlowSite}
                onChange={(e) => setSelectedFlowSite(e.target.value)}
              >
                {flowSites.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </div>
          )}

          <div style={{ marginBottom: "16px" }}>
            <label style={{ marginRight: 8 }}>Window:</label>
            <select
              value={flowWindow}
              onChange={(e) => setFlowWindow(e.target.value)}
            >
              <option value="7d">7 days</option>
              <option value="30d">30 days</option>
              <option value="1y">1 year</option>
            </select>
          </div>
        </>
      )}

      {/* LOAD BUTTON */}
      <button
        onClick={handleLoadChart}
        style={{
          marginTop: "8px",
          padding: "8px 16px",
          background: "#111",
          color: "white",
          borderRadius: "4px",
          border: "1px solid #444",
          cursor: "pointer",
        }}
      >
        {loading ? "Loading..." : "Load Chart"}
      </button>

      {/* TEMP DEBUG OUTPUT */}
      <pre
        style={{
          marginTop: "20px",
          fontSize: "12px",
          maxHeight: "300px",
          overflow: "auto",
          background: "#111",
          padding: "10px",
        }}
      >
        {/* RENDER FISH OR FLOW CHART */}
        {view === "Fish" && <FishChart data={fishChartData} />}
        {view === "Flow" && <FlowChart data={flowChartData} />}
      </pre>
    </div>
  );
}

function FlowChart({ data }) {
  if (!data || data.length === 0) return null;

  // Convert timestamp to readable MM-DD
  const formatted = data.map((row) => ({
    ...row,
    date: new Date(row.timestamp).toISOString().slice(5, 10),
  }));

  return (
    <div style={{ width: "100%", height: 400, marginTop: 40 }}>
      <ResponsiveContainer>
        <LineChart data={formatted}>
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
            dataKey="flow"
            stroke="#00e5ff"
            strokeWidth={3}
            dot={false}
            name="Flow (cfs)"
          />

          <Line
            type="monotone"
            dataKey="stage"
            stroke="#ff9800"
            strokeWidth={2}
            dot={false}
            name="Stage (ft)"
          />
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