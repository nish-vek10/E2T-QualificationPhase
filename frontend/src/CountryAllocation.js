// src/CountryAllocation.js
import React, { useEffect, useMemo, useState } from "react";
import countries from "i18n-iso-countries";
import enLocale from "i18n-iso-countries/langs/en.json";

countries.registerLocale(enLocale);

// ---- Supabase (read-only)
const SUPABASE_URL  = process.env.REACT_APP_SUPABASE_URL;
const SUPABASE_ANON = process.env.REACT_APP_SUPABASE_ANON_KEY;

const SB_SELECT = "country,pct_goal,is_qualified,status";
const SB_URL = `${SUPABASE_URL}/rest/v1/v_e2t_country_allocation?select=${encodeURIComponent(SB_SELECT)}&order=pct_goal.desc`;

// ---- Helpers to keep flags identical to your leaderboard ----
const COUNTRY_ALIASES = {
  "uk": "United Kingdom","u.k.":"United Kingdom","gb":"United Kingdom","great britain":"United Kingdom","britain":"United Kingdom",
  "uae":"United Arab Emirates","u.a.e.":"United Arab Emirates",
  "usa":"United States of America","u.s.a.":"United States of America","united states":"United States of America","us":"United States of America",
  "russia":"Russian Federation","kyrgyzstan":"Kyrgyz Republic","czech republic":"Czechia",
  "ivory coast":"Côte d'Ivoire","cote d'ivoire":"Côte d'Ivoire","côte d'ivoire":"Côte d'Ivoire",
  "dr congo":"Congo, Democratic Republic of the","democratic republic of the congo":"Congo, Democratic Republic of the","republic of the congo":"Congo",
  "swaziland":"Eswatini","cape verde":"Cabo Verde","palestine":"Palestine, State of",
  "iran":"Iran, Islamic Republic of","syria":"Syrian Arab Republic","moldova":"Moldova, Republic of",
  "venezuela":"Venezuela, Bolivarian Republic of","bolivia":"Bolivia, Plurinational State of",
  "laos":"Lao People's Democratic Republic","brunei":"Brunei Darussalam","vietnam":"Viet Nam",
  "south korea":"Korea, Republic of","north korea":"Korea, Democratic People's Republic of",
  "macau":"Macao","hong kong":"Hong Kong","burma":"Myanmar","myanmar":"Myanmar",
  "north macedonia":"North Macedonia","são tomé and príncipe":"Sao Tome and Principe","sao tome and principe":"Sao Tome and Principe",
  "micronesia":"Micronesia, Federated States of","st kitts and nevis":"Saint Kitts and Nevis","saint kitts and nevis":"Saint Kitts and Nevis",
  "st lucia":"Saint Lucia","saint lucia":"Saint Lucia","st vincent and the grenadines":"Saint Vincent and the Grenadines","saint vincent and the grenadines":"Saint Vincent and the Grenadines",
  "antigua":"Antigua and Barbuda","bahamas":"Bahamas","gambia":"Gambia","bahrein":"Bahrain",
  "netherlands the":"Netherlands","republic of ireland":"Ireland","eswatini":"Eswatini","kosovo":"Kosovo"
};

function resolveCountryAlpha2(rawName) {
  if (!rawName) return null;
  const raw = String(rawName).trim();
  let code = countries.getAlpha2Code(raw, "en");
  if (!code) {
    const alias = COUNTRY_ALIASES[raw.toLowerCase()];
    if (alias) code = countries.getAlpha2Code(alias, "en") || (alias.toLowerCase() === "kosovo" ? "XK" : null);
  }
  if (!code) {
    const cleaned = raw.replace(/[().]/g, "").replace(/\s+/g, " ").trim();
    code = countries.getAlpha2Code(cleaned, "en");
  }
  return code ? code.toLowerCase() : null;
}

function Flag({ country }) {
  const code = resolveCountryAlpha2(country);
  if (!code) return null;
  return (
    <img
      src={`https://flagcdn.com/w40/${code}.png`}
      title={country || ""}
      alt={country || ""}
      loading="lazy"
      style={{
        width: 38, height: 28, objectFit: "cover",
        borderRadius: 3, boxShadow: "0 0 3px rgba(0,0,0,0.6)"
      }}
      onError={(e) => { e.currentTarget.style.display = "none"; }}
    />
  );
}

function fmtPct(n) {
  if (n == null || isNaN(Number(n))) return "";
  return Number(n).toFixed(1) + "%";
}

// ---- Component ----
export default function CountryAllocation() {
  const [rows, setRows] = useState([]);
  const [err, setErr] = useState("");

  useEffect(() => {
    (async () => {
      try {
        if (!SUPABASE_URL || !SUPABASE_ANON) {
          throw new Error("Missing REACT_APP_SUPABASE_URL or REACT_APP_SUPABASE_ANON_KEY");
        }
        const res = await fetch(SB_URL, {
          headers: { apikey: SUPABASE_ANON, Authorization: `Bearer ${SUPABASE_ANON}` }
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
        const data = await res.json();
        setRows(Array.isArray(data) ? data : []);
        setErr("");
      } catch (e) {
        console.error(e);
        setErr("Failed to load country allocation.");
        setRows([]);
      }
    })();
  }, []);

  const container = { maxWidth: 900, margin: "0 auto", padding: "20px" };

  return (
    <div
      style={{
        fontFamily: "Switzer, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, 'Helvetica Neue', sans-serif",
        background: "transparent", color: "#eaeaea"
      }}
    >
      <h1
        style={{
          fontSize: "2.4rem", fontWeight: 900, textAlign: "center", margin: "18px 0",
          letterSpacing: "0.7px", textTransform: "none",
          color: "#fff"
        }}
      >
        Country Allocation Progress
      </h1>

      <div style={{ textAlign: "center", marginBottom: 18, color: "#aaa" }}>
        Towards US$1,000,000 goal • Live stats
      </div>

      <div style={container}>
        {err && <div style={{ color: "#ff6b6b", marginBottom: 12 }}>{err}</div>}

        <div
          style={{
            display: "grid",
            gap: 14,
            gridTemplateColumns: "1fr",
          }}
        >
          {rows.map((r, i) => {
            const pct = Math.max(0, Number(r.pct_goal || 0));
            const pctClamped = Math.min(pct, 100);
            const qualified = !!r.is_qualified;

            return (
              <div
                key={r.country + "_" + i}
                style={{
                  background: "#121212",
                  border: "1px solid #2a2a2a",
                  borderRadius: 16,
                  padding: 14,
                  display: "grid",
                  gridTemplateColumns: "minmax(0,1fr) auto",
                  alignItems: "center",
                  gap: 12,
                }}
              >
                {/* Left: flag + name + progress + caption */}
                <div style={{ display: "grid", gap: 10 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, fontWeight: 800 }}>
                    <Flag country={r.country} />
                    <span style={{ fontSize: 18 }}>{r.country || "Unknown"}</span>
                  </div>

                  {/* progress bar */}
                  <div style={{
                    height: 10, borderRadius: 999, background: "#2a2a2a", overflow: "hidden",
                  }}>
                    <div style={{
                      height: "100%",
                      width: `${pctClamped}%`,
                      background: "linear-gradient(90deg, #20c997, #14b8a6)",
                      transition: "width 500ms ease",
                    }} />
                  </div>

                  {/* caption — percentage only */}
                  <div style={{ fontSize: 12.5, color: "#cfcfcf" }}>
                    <strong>{fmtPct(pct)}</strong> of US$1,000,000
                  </div>
                </div>

                {/* Right: status + big % */}
                <div style={{ display: "grid", gap: 8, justifyItems: "end" }}>
                  <div
                    style={{
                      padding: "6px 12px",
                      borderRadius: 999,
                      fontWeight: 800,
                      fontSize: 12.5,
                      border: "1px solid",
                      borderColor: qualified ? "rgba(52,199,89,0.5)" : "#2a2a2a",
                      background: qualified ? "rgba(52,199,89,0.12)" : "#181818",
                      color: qualified ? "#34c759" : "#cfcfcf",
                    }}
                  >
                    {qualified ? "Qualified" : "Pending"}
                  </div>

                  <div style={{ fontWeight: 900, fontSize: 18 }}>
                    {fmtPct(pct)}
                  </div>
                </div>
              </div>
            );
          })}
        </div>

        {rows.length === 0 && !err && (
          <div style={{ color: "#888", textAlign: "center", marginTop: 20 }}>
            No data.
          </div>
        )}
      </div>
    </div>
  );
}
