import os
import re
from pathlib import Path

import clickhouse_connect
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Flight Delay Outlook", layout="wide", initial_sidebar_state="collapsed")

# ---------------------------------------------------------------- settings
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "admin")
CH_PASS = os.getenv("CH_PASS", "admin123")
CH_DB = os.getenv("CH_DB", "flights")
CH_TABLE = f"{CH_DB}.{os.getenv('PRED_TABLE', 'predictions_v2')}"
CSV_PATH = Path(__file__).parent / "predictions.csv"  # used when ClickHouse is not reachable (online copy)
MIN_SAMPLE = 30  # fewer similar flights than this -> use a broader group

NAVY = "#0B3A9B"
INK = "#0f2a52"
FONT = "Space Grotesk, sans-serif"

AIRLINES = {
    "AA": "American", "DL": "Delta", "UA": "United", "WN": "Southwest", "B6": "JetBlue",
    "AS": "Alaska", "NK": "Spirit", "F9": "Frontier", "G4": "Allegiant", "HA": "Hawaiian",
    "YX": "Republic", "OO": "SkyWest", "9E": "Endeavor", "MQ": "Envoy", "OH": "PSA",
    "YV": "Mesa", "QX": "Horizon", "C5": "CommuteAir", "ZW": "Air Wisconsin", "G7": "GoJet",
}
DAYS = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}

# ---------------------------------------------------------------- styling
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&display=swap');
.stApp{
  font-family:'Space Grotesk',sans-serif;
  background:
    radial-gradient(ellipse 460px 170px at 10% 150px, rgba(255,255,255,.55), transparent 70%),
    radial-gradient(ellipse 560px 200px at 84% 110px, rgba(255,255,255,.50), transparent 70%),
    radial-gradient(ellipse 640px 210px at 52% 330px, rgba(255,255,255,.32), transparent 70%),
    radial-gradient(ellipse 420px 160px at 20% 470px, rgba(255,255,255,.30), transparent 70%),
    linear-gradient(180deg,#4f84c4 0px,#7aa5d6 430px,#c9daee 720px,#eef3fa 920px);
  background-repeat:no-repeat;
}
button,input,select{font-family:inherit}
header[data-testid="stHeader"]{background:transparent}
#MainMenu,footer{visibility:hidden}
.block-container{max-width:1180px;padding-top:1.4rem;padding-bottom:4rem}

.topbar{display:flex;justify-content:space-between;align-items:center;color:#fff}
.brand{font-size:1.45rem;font-weight:600;letter-spacing:-.01em}
.period{border:1px solid rgba(255,255,255,.6);border-radius:999px;padding:6px 16px;font-size:.85rem}

.hero{text-align:center;color:#fff;padding:.5rem 0 1.4rem}
.plane{width:min(760px,92%);margin:0 auto -10px;filter:drop-shadow(0 22px 28px rgba(15,42,82,.30))}
.hero h1{font-size:3.7rem;font-weight:600;line-height:1.04;letter-spacing:-.025em;margin:0;color:#fff;padding:0}
.hero p{font-size:1.02rem;opacity:.92;margin:.9rem 0 1.2rem}
.chips{display:flex;gap:10px;justify-content:center;flex-wrap:wrap}
.chip{background:rgba(255,255,255,.22);color:#fff;font-size:.95rem;padding:10px 24px 10px 30px;
  clip-path:polygon(14px 0,100% 0,100% 100%,14px 100%,0 50%)}

div[data-testid="stHorizontalBlock"]:has(div[data-testid="stSelectbox"]){background:#fff;border-radius:26px;
  padding:20px 24px 18px;box-shadow:0 22px 50px rgba(15,42,82,.28)}
div[data-testid="stSelectbox"] label p{font-size:.78rem;color:#5b6b86}

[data-testid="stPlotlyChart"]{background:#fff;border-radius:22px;padding:10px;
  box-shadow:0 10px 30px rgba(15,42,82,.10);overflow:hidden}
.card{background:#fff;border-radius:22px;padding:24px 28px;box-shadow:0 10px 30px rgba(15,42,82,.10);color:#0f2a52}
.result{min-height:318px;display:flex;flex-direction:column;justify-content:center}
.route{font-size:1rem;color:#5b6b86}
.risk{font-size:2.6rem;font-weight:600;letter-spacing:-.02em;line-height:1.1;margin:.2rem 0 .5rem}
.sentence{font-size:1.05rem;margin:0 0 1.1rem;max-width:34rem}
.stats{display:flex;gap:28px;flex-wrap:wrap;margin-bottom:1rem}
.stats div span{display:block;font-size:.8rem;color:#5b6b86}
.stats div b{font-size:1.35rem;font-weight:600}
.basis{font-size:.85rem;color:#5b6b86}
.section{font-size:1.7rem;font-weight:600;letter-spacing:-.01em;color:#0f2a52;margin:2.6rem 0 .9rem}
.kpi span{display:block;font-size:.85rem;color:#5b6b86}
.kpi b{font-size:2rem;font-weight:600;letter-spacing:-.02em}
.kpi{padding:18px 24px}
.hint{font-size:.9rem;color:#5b6b86;margin:-.4rem 0 .8rem}
</style>
""",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------- data access
@st.cache_data(ttl=60, show_spinner=False)
def backend():
    """ClickHouse when it is reachable (the Docker platform), otherwise predictions.csv (online copy)."""
    try:
        clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB, connect_timeout=3
        ).query(f"SELECT 1 FROM {CH_TABLE} LIMIT 1")
        return "clickhouse"
    except Exception:  # noqa: BLE001
        return "csv" if CSV_PATH.exists() else None


@st.cache_resource(show_spinner=False)
def duck():
    import duckdb  # only needed for the CSV copy

    con = duckdb.connect()
    con.execute(f"CREATE TABLE preds AS SELECT * FROM read_csv_auto('{CSV_PATH.as_posix()}')")
    return con


MODE = backend()
TABLE = CH_TABLE if MODE == "clickhouse" else "preds"
DOW = "toDayOfWeek(FlightDate)" if MODE == "clickhouse" else "isodow(FlightDate)"  # 1 = Monday in both


@st.cache_data(ttl=600, show_spinner=False)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    params = params or {}
    if MODE == "clickhouse":
        client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB
        )
        return client.query_df(sql, parameters=params)
    sql = re.sub(r"\{(\w+):\w+\}", r"$\1", sql)  # {name:Type} -> $name for DuckDB
    cur = duck().cursor()
    return (cur.execute(sql, params) if params else cur.execute(sql)).df()


if MODE is None:
    st.error("No data source found. Start the ClickHouse container or add predictions.csv next to this file.")
    st.stop()


COLS = {"airline": "Reporting_Airline", "origin": "Origin", "dest": "Dest", "block": "DepTimeBlk"}


def run_level(conds: dict, dow: int | None):
    """Average probability and observed late rate for flights matching conds."""
    where, params = [], {}
    for key, value in conds.items():
        where.append(f"{COLS[key]} = {{{key}:String}}")
        params[key] = value
    if dow:
        where.append(f"{DOW} = {{dow:UInt8}}")
        params["dow"] = dow
    sql = (
        f"SELECT count(*) AS n, avg(delay_probability) AS p, avg(actual_delayed) AS a "
        f"FROM {TABLE} WHERE {' AND '.join(where)}"
    )
    row = q(sql, params).iloc[0]
    return int(row["n"]), float(row["p"]), float(row["a"])


def lookup(origin, dest, airline, block, dow):
    """Start with the most specific group and widen it until there are enough flights."""
    levels = []
    if airline:
        levels.append(("this airline on this route at this time", dict(airline=airline, origin=origin, dest=dest, block=block)))
    levels += [
        ("this route at this time", dict(origin=origin, dest=dest, block=block)),
        ("this route", dict(origin=origin, dest=dest)),
        ("this airport at this time", dict(origin=origin, block=block)),
        ("this airport", dict(origin=origin)),
    ]
    best = None
    for i, (label, conds) in enumerate(levels):
        n, p, a = run_level(conds, dow)
        if n == 0:
            continue
        best = dict(n=n, p=p, a=a, label=label, exact=(i == 0))
        if n >= MIN_SAMPLE:
            break
    return best


def fmt_block(b: str) -> str:
    return f"{b[:2]}:{b[2:4]} to {b[5:7]}:{b[7:9]}"


def airline_name(code: str) -> str:
    return f"{AIRLINES.get(code, code)} ({code})"


def risk_level(p: float):
    if p < 0.20:
        return "Low risk", "#2e8b57"
    if p < 0.35:
        return "Moderate risk", "#b87d00"
    return "High risk", "#c0392b"


def style(fig, height):
    fig.update_layout(
        height=height, margin=dict(l=20, r=20, t=50, b=30), paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family=FONT, color=INK), title=dict(font=dict(size=16)),
    )
    return fig


# ---------------------------------------------------------------- overview numbers
try:
    kpi = q(
        f"SELECT count(*) n, avg(delay_probability) p, avg(actual_delayed) a, count(DISTINCT Origin) airports, "
        f"count(DISTINCT Reporting_Airline) airlines, min(FlightDate) d0, max(FlightDate) d1 FROM {TABLE}"
    ).iloc[0]
except Exception as exc:  # noqa: BLE001
    st.error(f"Could not read {TABLE} from ClickHouse. Check that the clickhouse container is running. ({exc})")
    st.stop()

overall_p = float(kpi["p"])
period = f"{pd.Timestamp(kpi['d0']):%d %b %Y} to {pd.Timestamp(kpi['d1']):%d %b %Y}"

# ---------------------------------------------------------------- hero
st.markdown(
    f"""
<div class="topbar"><div class="brand">Flight Delay Outlook</div><div class="period">{period}</div></div>
<div class="hero">
  <svg class="plane" viewBox="0 0 800 220" xmlns="http://www.w3.org/2000/svg">
    <path d="M70 108 L36 24 L98 24 L176 98 Z" fill="#e6edf7"/>
    <path d="M372 118 L574 128 L430 196 L352 196 Z" fill="#d9e2f0"/>
    <path d="M58 122 Q58 96 124 92 L640 92 Q762 100 788 126 Q762 152 640 154 L124 154 Q58 150 58 122 Z" fill="#ffffff"/>
    <ellipse cx="478" cy="168" rx="44" ry="15" fill="#c9d4e5"/>
    <path d="M706 108 Q744 108 770 122 L706 122 Z" fill="#6c86ad"/>
    <g fill="#9fb4d3">
      <circle cx="200" cy="118" r="5"/><circle cx="232" cy="118" r="5"/><circle cx="264" cy="118" r="5"/>
      <circle cx="296" cy="118" r="5"/><circle cx="328" cy="118" r="5"/><circle cx="360" cy="118" r="5"/>
      <circle cx="560" cy="118" r="5"/><circle cx="592" cy="118" r="5"/><circle cx="624" cy="118" r="5"/>
      <circle cx="656" cy="118" r="5"/><circle cx="688" cy="118" r="5"/>
    </g>
  </svg>
  <h1>Will your flight arrive on time?</h1>
  <p>Pick a route, an airline and a departure time to see how likely it is to land 15 minutes or more late.</p>
  <div class="chips">
    <div class="chip">{int(kpi['n']):,} flights analyzed</div>
    <div class="chip">{int(kpi['airports'])} departure airports</div>
    <div class="chip">{int(kpi['airlines'])} airlines</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------- search card
origins = q(f"SELECT DISTINCT Origin FROM {TABLE} ORDER BY Origin")["Origin"].tolist()
dests = q(f"SELECT DISTINCT Dest FROM {TABLE} ORDER BY Dest")["Dest"].tolist()
carriers = q(f"SELECT DISTINCT Reporting_Airline FROM {TABLE} ORDER BY Reporting_Airline")["Reporting_Airline"].tolist()
blocks = q(f"SELECT DISTINCT DepTimeBlk FROM {TABLE} ORDER BY DepTimeBlk")["DepTimeBlk"].tolist()


def pick(options, value):
    return options.index(value) if value in options else 0


# no st.form here: every change in a selectbox reruns the script, so the result updates instantly
c1, c2, c3, c4, c5 = st.columns([1.1, 1.1, 1.5, 1.4, 1.2])
origin = c1.selectbox("From", origins, index=pick(origins, "ATL"))
dest = c2.selectbox("To", dests, index=pick(dests, "LAX"))
airline = c3.selectbox("Airline", [None] + carriers, format_func=lambda c: "Any airline" if c is None else airline_name(c))
block = c4.selectbox("Departure time", blocks, index=pick(blocks, "1800-1859"), format_func=fmt_block)
dow = c5.selectbox("Day of week", [None] + list(DAYS), format_func=lambda d: "Any day" if d is None else DAYS[d])

# ---------------------------------------------------------------- result
res = lookup(origin, dest, airline, block, dow)
st.markdown("<div style='height:1.4rem'></div>", unsafe_allow_html=True)

if res is None:
    st.markdown(
        f"<div class='card'>No flights found for {origin} in the data. Try another departure airport.</div>",
        unsafe_allow_html=True,
    )
else:
    p = res["p"]
    level_text, color = risk_level(p)
    diff = (p - overall_p) * 100
    vs = f"{abs(diff):.1f} points {'above' if diff >= 0 else 'below'} the average flight"
    if res["exact"]:
        basis = f"Based on {res['n']:,} flights that match your selection."
    else:
        extra = " on the same weekday" if dow else ""
        basis = (
            f"Not enough flights match every filter, so this uses {res['label']}{extra} "
            f"({res['n']:,} flights)."
        )

    g, r = st.columns([1, 1.25])
    with g:
        fig = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=p * 100,
                number={"suffix": "%", "valueformat": ".1f", "font": {"size": 46, "color": INK}},
                gauge={
                    "axis": {"range": [0, 100], "tickcolor": "#9fb0c9"},
                    "bar": {"color": NAVY, "thickness": 0.28},
                    "bgcolor": "white",
                    "borderwidth": 0,
                    "steps": [
                        {"range": [0, 20], "color": "#d6eedd"},
                        {"range": [20, 35], "color": "#fbe9bd"},
                        {"range": [35, 100], "color": "#f7cfca"},
                    ],
                },
            )
        )
        style(fig, 300).update_layout(margin=dict(l=24, r=24, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
    with r:
        st.markdown(
            f"""
<div class="card result">
  <div class="route">{origin} to {dest}, {fmt_block(block)}{'' if dow is None else ', ' + DAYS[dow]}</div>
  <div class="risk" style="color:{color}">{level_text}</div>
  <p class="sentence">Out of every 100 similar flights, about {round(p * 100)} are expected to arrive
  15 minutes or more late. That is {vs}.</p>
  <div class="stats">
    <div><span>Delay probability</span><b>{p * 100:.1f}%</b></div>
    <div><span>Actually arrived late</span><b>{res['a'] * 100:.1f}%</b></div>
    <div><span>Average flight</span><b>{overall_p * 100:.1f}%</b></div>
  </div>
  <div class="basis">{basis}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    # delay risk across the day for this route (falls back to the departure airport)
    dow_sql = f"AND {DOW} = {{dow:UInt8}}" if dow else ""
    params = {"origin": origin, "dest": dest, **({"dow": dow} if dow else {})}
    curve = q(
        f"SELECT DepTimeBlk b, count(*) n, avg(delay_probability) p FROM {TABLE} "
        f"WHERE Origin = {{origin:String}} AND Dest = {{dest:String}} {dow_sql} "
        f"GROUP BY b HAVING n >= 10 ORDER BY b",
        params,
    )
    scope = f"{origin} to {dest}"
    if len(curve) < 3:
        curve = q(
            f"SELECT DepTimeBlk b, count(*) n, avg(delay_probability) p FROM {TABLE} "
            f"WHERE Origin = {{origin:String}} {dow_sql} GROUP BY b HAVING n >= 10 ORDER BY b",
            {k: v for k, v in params.items() if k != "dest"},
        )
        scope = f"all flights from {origin}"
    if len(curve):
        x = [b[:2] + ":00" for b in curve["b"]]
        fig = go.Figure(
            go.Scatter(
                x=x, y=curve["p"] * 100, mode="lines+markers", customdata=curve["n"],
                line=dict(color=NAVY, width=3, shape="spline"), marker=dict(size=7, color=NAVY),
                fill="tozeroy", fillcolor="rgba(11,58,155,.08)",
                hovertemplate="%{x}<br>%{y:.1f}% delay probability<br>%{customdata:,} flights<extra></extra>",
            )
        )
        sel = curve[curve["b"] == block]
        if len(sel):
            fig.add_trace(
                go.Scatter(
                    x=[block[:2] + ":00"], y=sel["p"] * 100, mode="markers",
                    marker=dict(size=15, color="#f08a24", line=dict(color="white", width=2)),
                    hoverinfo="skip", showlegend=False,
                )
            )
        fig.update_layout(showlegend=False, yaxis=dict(ticksuffix="%", gridcolor="#e8eef7", rangemode="tozero"),
                          xaxis=dict(title="Departure hour"), title=f"Delay probability across the day, {scope}")
        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
        st.plotly_chart(style(fig, 340), use_container_width=True)

# ---------------------------------------------------------------- overview
st.markdown("<div class='section'>Overview</div>", unsafe_allow_html=True)
k = st.columns(4)
for col, (label, value) in zip(
    k,
    [
        ("Flights analyzed", f"{int(kpi['n']):,}"),
        ("Average delay probability", f"{overall_p * 100:.1f}%"),
        ("Arrived 15+ minutes late", f"{float(kpi['a']) * 100:.1f}%"),
        ("Departure airports", f"{int(kpi['airports'])}"),
    ],
):
    col.markdown(f"<div class='card kpi'><span>{label}</span><b>{value}</b></div>", unsafe_allow_html=True)

st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
a, b = st.columns(2)

by_block = q(f"SELECT DepTimeBlk b, count(*) n, avg(delay_probability) p FROM {TABLE} GROUP BY b ORDER BY b")
fig = go.Figure(go.Bar(x=[v[:2] + ":00" for v in by_block["b"]], y=by_block["p"] * 100, marker_color=NAVY,
                       customdata=by_block["n"], hovertemplate="%{x}<br>%{y:.1f}%<br>%{customdata:,} flights<extra></extra>"))
fig.update_layout(title="Delay probability by departure time", yaxis=dict(ticksuffix="%", gridcolor="#e8eef7"))
a.plotly_chart(style(fig, 340), use_container_width=True)

by_day = q(f"SELECT {DOW} d, count(*) n, avg(delay_probability) p FROM {TABLE} GROUP BY d ORDER BY d")
fig = go.Figure(go.Bar(x=[DAYS[int(d)] for d in by_day["d"]], y=by_day["p"] * 100, marker_color=NAVY,
                       customdata=by_day["n"], hovertemplate="%{x}<br>%{y:.1f}%<br>%{customdata:,} flights<extra></extra>"))
fig.update_layout(title="Delay probability by day of week", yaxis=dict(ticksuffix="%", gridcolor="#e8eef7"))
b.plotly_chart(style(fig, 340), use_container_width=True)

st.markdown("<div style='height:.6rem'></div>", unsafe_allow_html=True)
a, b = st.columns(2)

apt = q(f"SELECT Origin, count(*) n, avg(delay_probability) p FROM {TABLE} GROUP BY Origin ORDER BY n DESC LIMIT 12")
apt = apt.sort_values("p")
fig = go.Figure(go.Bar(y=apt["Origin"], x=apt["p"] * 100, orientation="h", marker_color=NAVY, customdata=apt["n"],
                       hovertemplate="%{y}<br>%{x:.1f}%<br>%{customdata:,} flights<extra></extra>"))
fig.update_layout(title="The 12 busiest airports", xaxis=dict(ticksuffix="%", gridcolor="#e8eef7"))
a.plotly_chart(style(fig, 420), use_container_width=True)

air = q(
    f"SELECT Reporting_Airline c, count(*) n, avg(delay_probability) p FROM {TABLE} "
    f"GROUP BY c HAVING n >= 5000 ORDER BY p"
)
fig = go.Figure(go.Bar(y=[AIRLINES.get(c, c) for c in air["c"]], x=air["p"] * 100, orientation="h", marker_color=NAVY,
                       customdata=air["n"], hovertemplate="%{y}<br>%{x:.1f}%<br>%{customdata:,} flights<extra></extra>"))
fig.update_layout(title="Airlines with 5,000+ flights", xaxis=dict(ticksuffix="%", gridcolor="#e8eef7"))
b.plotly_chart(style(fig, 420), use_container_width=True)

st.markdown("<div style='height:.6rem'></div>", unsafe_allow_html=True)
heat = q(f"SELECT {DOW} d, DepTimeBlk b, avg(delay_probability) p FROM {TABLE} GROUP BY d, b")
grid = heat.pivot(index="d", columns="b", values="p").sort_index()
fig = go.Figure(
    go.Heatmap(
        z=grid.values * 100, x=[v[:2] + ":00" for v in grid.columns], y=[DAYS[int(d)] for d in grid.index],
        colorscale=[[0, "#eaf1fb"], [1, NAVY]], colorbar=dict(ticksuffix="%", thickness=12),
        hovertemplate="%{y}, %{x}<br>%{z:.1f}%<extra></extra>",
    )
)
fig.update_layout(title="When delays are most likely: day of week by departure hour", yaxis=dict(autorange="reversed"))
st.plotly_chart(style(fig, 360), use_container_width=True)
