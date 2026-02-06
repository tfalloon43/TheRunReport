import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
  CartesianGrid,
} from "recharts";
import { useMemo } from "react";

export default function FishChart({ data }) {
  // Convert MM-DD → full year date objects
  const chartData = useMemo(() => {
    return data.map((row) => ({
      date: new Date(`2024-${row["MM-DD"]}`),
      ten_year: row.ten_year,
      last_year: row.last_year,
      current_year: row.current_year,
    }));
  }, [data]);

  return (
    <div style={{ width: "100%", height: 400 }}>
      <ResponsiveContainer>
        <LineChart data={chartData}>
          <CartesianGrid stroke="rgba(var(--color-text), 0.1)" />

          <XAxis
            dataKey="date"
            tickFormatter={(d) =>
              new Date(d).toLocaleDateString("en-US", {
                month: "2-digit",
                day: "2-digit",
              })
            }
            stroke="rgba(var(--color-text), 0.7)"
          />

          <YAxis stroke="rgba(var(--color-text), 0.7)" />

          <Tooltip
            labelFormatter={(d) =>
              new Date(d).toLocaleDateString("en-US", {
                month: "short",
                day: "numeric",
              })
            }
            contentStyle={{
              background: "rgb(var(--color-bg))",
              border: "1px solid rgba(var(--color-text), 0.2)",
              borderRadius: "6px",
              color: "rgb(var(--color-text))",
            }}
          />

          <Legend />

          <Line
            type="monotone"
            dataKey="ten_year"
            stroke="rgb(var(--chart-line-3))"
            strokeWidth={2}
            dot={false}
            opacity={0.4}
            name="Ten Year Avg"
          />

          <Line
            type="monotone"
            dataKey="last_year"
            stroke="rgb(var(--chart-line-2))"
            strokeWidth={2}
            dot={false}
            name="Last Year"
          />

          <Line
            type="monotone"
            dataKey="current_year"
            stroke="rgb(var(--chart-line-1))"
            strokeWidth={3}
            dot={false}
            name="Current Year"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
