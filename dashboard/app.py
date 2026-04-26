"""Streamlit dashboard tổng hợp toàn bộ EDA của nhóm Lab 01."""


from pathlib import Path
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings

warnings.filterwarnings("ignore")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  1. MASTER CSS 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def inject_custom_css(c):
    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    
    :root {{ 
        --f-body: 'Inter', -apple-system, sans-serif; --r-lg: 12px;
        --void: {c['void']}; --surface: {c['surf']}; 
        --t1: {c['t1']}; --t2: {c['t2']}; --t3: {c['t3']}; 
        --neon: {c['acc']}; --rail: {c['rail']}; 
    }} 
    
    html, body, [class*="css"] {{ font-family: var(--f-body) !important; color: var(--t1) !important; }}
    [data-testid="stAppViewContainer"] {{ background-color: var(--void) !important; }}
    
    [data-testid="stAppViewContainer"] p, 
    [data-testid="stAppViewContainer"] span, 
    [data-testid="stAppViewContainer"] label, 
    [data-testid="stAppViewContainer"] h1, 
    [data-testid="stAppViewContainer"] h2, 
    [data-testid="stAppViewContainer"] h3, 
    [data-testid="stAppViewContainer"] li {{
        color: var(--t1) !important;
    }}

    /* Selectbox theo giao diện */
    [data-testid="stSelectbox"] label {{
        color: var(--t1) !important;
        font-weight: 700 !important;
        font-size: 0.9rem !important;
    }}
    [data-baseweb="select"] > div {{
        background-color: var(--surface) !important;
        border: 1px solid var(--rail) !important;
        border-radius: 10px !important;
    }}
    [data-baseweb="select"] [role="combobox"] {{
        color: var(--t1) !important;
        background-color: transparent !important;
        font-weight: 500 !important;
    }}
    [data-baseweb="select"] > div:focus-within {{
        border-color: var(--neon) !important;
        box-shadow: 0 0 0 1px var(--neon) inset !important;
    }}
    [data-baseweb="select"] [aria-expanded="true"] {{
        border-color: var(--neon) !important;
        box-shadow: 0 0 0 1px var(--neon) inset !important;
    }}
    [data-baseweb="popover"] [role="listbox"] {{
        background-color: var(--surface) !important;
        border: 1px solid var(--rail) !important;
        border-radius: 10px !important;
    }}
    [data-baseweb="menu"] [role="option"] {{
        color: var(--t1) !important;
        background-color: transparent !important;
    }}
    [data-baseweb="menu"] [role="option"]:hover {{
        background-color: var(--rail) !important;
    }}

    /* Popover theo giao diện */
    [data-testid="stPopover"] button,
    [data-testid="stPopover"] button * {{
        color: var(--t1) !important;
    }}
    [data-testid="stPopover"] button {{
        background-color: var(--surface) !important;
        border-color: var(--rail) !important;
    }}
    /* ========================================================= */

    header[data-testid="stHeader"] {{ background: rgba(0,0,0,0) !important; visibility: visible !important; }}
    #MainMenu, footer {{ visibility: hidden; }}
    
    /* Giao diện Sidebar */
    [data-testid="stSidebar"] {{ background: var(--surface) !important; border-right: 1px solid var(--rail) !important; }}
    .sb-brand {{ padding: 0.5rem 0 1rem; text-align: center; }}
    .sb-logo {{ font-size: 1.6rem; font-weight: 800; color: var(--t1) !important; }}
    .sb-logo span {{ color: var(--neon) !important; }}
    .sb-sub {{ font-size: 0.6rem; color: var(--t3) !important; letter-spacing: 0.1em; text-transform: uppercase; }}
    
    /* Căn chỉnh nút Popover */
    div[data-testid="stPopover"] button {{ width: 100% !important; border-radius: 8px !important; text-align: left !important; font-family: 'Inter', sans-serif !important; padding: 10px 15px !important; }}
    div[data-testid="stPopover"] button:hover {{ border-color: var(--neon) !important; }}
    div[data-testid="stSelectbox"] input {{ pointer-events: none; }}
    
    /* Giao diện KPI Grid */
    .site-header {{ padding: 1.5rem 0; border-bottom: 1px solid var(--rail); margin-bottom: 2rem; }}
    .kpi-grid {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 1px; background: var(--rail); border-radius: var(--r-lg); overflow: hidden; margin-bottom: 2rem; }}
    .kpi-cell {{ background: var(--surface); padding: 1.2rem; text-align: center; border: 1px solid var(--rail); }}
    
    .kpi-tag {{ font-size: 0.85rem; font-weight: 600; color: var(--t2) !important; text-transform: uppercase; margin-bottom: 4px; display: block; }}
    .kpi-sub {{ font-size: 0.75rem; color: var(--t3) !important; margin-top: 4px; display: block; }}
    .kpi-num {{ font-size: 1.6rem; font-weight: 800; color: var(--t1) !important; line-height: 1.2; }}
    .kpi-num.hot {{ color: var(--neon) !important; }}

    /* Khung viền bọc biểu đồ của Streamlit */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        background-color: var(--surface) !important;
        border: 1px solid var(--rail) !important;
        border-radius: var(--r-lg) !important;
        padding: 1.5rem !important; 
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        box-sizing: border-box !important;
    }}

    /* DataFrame theo giao diện */
    div[data-testid="stDataFrame"] {{
        background-color: var(--surface) !important;
        border: 1px solid var(--rail) !important;
        border-radius: 10px !important;
    }}
    div[data-testid="stDataFrame"] [role="grid"] {{
        background-color: var(--surface) !important;
        color: var(--t1) !important;
    }}
    div[data-testid="stDataFrame"] [role="columnheader"] {{
        background-color: var(--surface) !important;
        color: var(--t1) !important;
        font-weight: 700 !important;
        border-bottom: 1px solid var(--rail) !important;
    }}
    div[data-testid="stDataFrame"] [role="gridcell"] {{
        background-color: var(--surface) !important;
        color: var(--t1) !important;
        border-top: 1px solid var(--rail) !important;
    }}
    </style>
    """, unsafe_allow_html=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  2. PAGE CONFIG & DATA LOADER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.set_page_config(page_title="TIKI INTELLIGENCE", page_icon="◈", layout="wide", initial_sidebar_state="expanded")

DATA_DIR = Path(__file__).parent.parent / "data" / "processed"
DATA_CANDIDATES = [DATA_DIR / "fact_product_enriched.csv", DATA_DIR / "fact_product_merged.csv"]

def normalize_bool(series: pd.Series) -> pd.Series:
    mapping = {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False}
    return series.astype(str).str.strip().str.lower().map(mapping).fillna(False)

@st.cache_data(show_spinner="Đang nạp dữ liệu...")
def load_data():
    df = pd.DataFrame()
    for path in DATA_CANDIDATES:
        if path.exists():
            df = pd.read_csv(path, encoding="utf-8-sig"); break
            
    if df.empty: # Dummy Data
        rng = np.random.default_rng(42); N = 800
        df = pd.DataFrame({
            "product_id": [f"tiki_{i}" for i in range(N)],
            "category_name": rng.choice(["Điện tử", "Gia dụng", "Sách", "Mỹ phẩm", "Thời trang"], N),
            "price_current": np.exp(rng.normal(12, 1.2, N)).clip(10000, 50000000),
            "discount_percent": rng.uniform(0, 50, N), "sold_count": rng.integers(0, 5000, N),
            "rating": rng.uniform(3.5, 5.0, N), "review_count": rng.integers(0, 1000, N),
            "is_mall": rng.choice([True, False], N), "has_video": rng.choice([True, False], N),
            "is_freeship": rng.choice([True, False], N)
        })

    if "discount_percent" in df.columns:
        df["discount_bucket"] = pd.cut(df["discount_percent"].fillna(0), bins=[-1, 0, 10, 30, 50, 70, 101], labels=["0%", "1–10%", "11–30%", "31–50%", "51–70%", ">70%"])
    df["revenue_est"] = df.get("price_current", 0) * df.get("sold_count", 0).fillna(0)
    for col in ["has_video", "is_mall", "is_freeship"]:
        if col in df.columns: df[col] = normalize_bool(df[col])
    return df

df_raw = load_data()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  3. SIDEBAR & BỘ LỌC
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with st.sidebar:
    st.markdown('<div class="sb-brand"><div class="sb-logo">◈ TIKI <span>ANALYTICS</span></div><div class="sb-sub">Analytics Platform · v2.0</div></div>', unsafe_allow_html=True)
    st.divider()

    st.markdown("###  BỘ LỌC DỮ LIỆU")
    all_cats = sorted(df_raw["category_name"].dropna().unique())
    with st.popover(" CHỌN DANH MỤC"):
        select_all = st.toggle("Chọn tất cả", value=True)
        sel_cats = [cat for cat in all_cats if st.checkbox(cat, value=select_all, key=f"sb_{cat}")]
    
    price_r = st.slider("Khoảng giá (VND)", int(df_raw["price_current"].min()), int(df_raw["price_current"].max()), (int(df_raw["price_current"].min()), int(df_raw["price_current"].max())))
    disc_min = st.slider("Discount tối thiểu (%)", 0, 100, 0)
    rat_min  = st.slider("Rating tối thiểu", 0.0, 5.0, 0.0, 0.1)
    
    with st.popover(" VẬN CHUYỂN"):
        ship_opt = st.radio("Trạng thái Freeship:", options=["Tất cả", "Có freeship", "Không freeship"], index=0, key="ship_radio")

    st.markdown('<div style="flex-grow: 1;"></div>', unsafe_allow_html=True) 
    st.divider()
    st.markdown('<div style="text-align:center; font-size:0.75rem; color:var(--t3); padding:5px 0;"><div style="font-weight:700; color:var(--t2); margin-bottom:5px;">DEVELOPED BY TEAM 13</div>Tuan • Thinh • The Anh • Y • Duong</div>', unsafe_allow_html=True)

# Lọc dữ liệu
df = df_raw.copy()
if sel_cats: df = df[df["category_name"].isin(sel_cats)]
else: df = df.iloc[0:0]
df = df[(df["price_current"] >= price_r[0]) & (df["price_current"] <= price_r[1])]
df = df[df["discount_percent"].fillna(0) >= disc_min]
df = df[df["rating"].fillna(0) >= rat_min]
if ship_opt == "Có freeship": df = df[df["is_freeship"] == True]
elif ship_opt == "Không freeship": df = df[df["is_freeship"] == False]

N = len(df); pct = N / max(1, len(df_raw)) * 100

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  4. HEADER & THEME LOGIC
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
h_left, h_right = st.columns([5, 1])
with h_right:
    theme_choice = st.selectbox("🎨 GIAO DIỆN", ["Sáng", "Tối", "Mù màu", "Hệ thống"], index=0, key="theme_mode")
    if theme_choice == "Sáng":
        c = {"void": "#FFFFFF", "surf": "#F8FAFC", "t1": "#0F172A", "t2": "#334155", "t3": "#64748B", "acc": "#0062FF", "rail": "rgba(0,0,0,0.05)"}
    elif theme_choice == "Tối":
        c = {"void": "#0B0F19", "surf": "#141B2D", "t1": "#FFFFFF", "t2": "#F8FAFC", "t3": "#CBD5E1", "acc": "#FF5F1F", "rail": "rgba(255,255,255,0.15)"}

    elif theme_choice == "Mù màu":
        c = {
            "void": "#FFFFFF",    
            "surf": "#F0F4F8",     
            "t1":   "#000000",     
            "t2":   "#1A2634",     
            "t3":   "#4A5568",  
            "acc":  "#0072B2",  
            "rail": "#B0B8C1",      
        }

    else: 
        c = {"void": "#F1F5F9", "surf": "#FFFFFF", "t1": "#1E293B", "t2": "#475569", "t3": "#94A3B8", "acc": "#0F172A", "rail": "rgba(0,0,0,0.08)"}
    
    # Kích hoạt CSS
    inject_custom_css(c)

with h_left:
    st.markdown(f"""
    <div class="site-header" style="border-bottom:none; margin-bottom:0; padding-bottom:0;">
        <div style="font-size:0.75rem; color:var(--t3); font-weight:700; letter-spacing:0.05em; margin-bottom:5px;">◈ E-COMMERCE INTELLIGENCE · LAB 01 · HCMUS</div>
        <div style="font-size:2.4rem; font-weight:800; color:var(--t1); line-height:1;">TIKI <em style="color:var(--neon); font-style:normal;">ANALYTICS</em></div>
    </div>
    """, unsafe_allow_html=True)
st.markdown('<div style="border-bottom: 1px solid var(--rail); margin: 15px 0 25px 0;"></div>', unsafe_allow_html=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  5. PLOTLY ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if theme_choice == "Mù màu":
    C = [
        "#0072B2",  
        "#E69F00",  
        "#56B4E9",  
        "#D55E00",  
        "#44AA99", 
        "#F0E442",  
        "#000000",  
    ]
elif theme_choice == "Tối":
    C = ["#60A5FA", "#F97316", "#34D399", "#F472B6", "#FACC15"]
else:
    C = ["#0062FF", "#FF5F1F", "#2DDBB4", "#FF4E6A", "#E8B86D"]
FULL_BAR = dict(displayModeBar=True, scrollZoom=True)

def fig_layout(height=400, x_title=None, y_title=None, x_log=False, y_log=False, horizontal_legend=False, margin=None):
    txt_color = c["t1"]
    grid_color = c["rail"]
    
    _AX = dict(gridcolor=grid_color, zeroline=False, tickfont=dict(size=10, family="Inter", color=txt_color))
    layout = dict(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor ="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=txt_color, size=11),
        margin=margin if margin else dict(l=10, r=10, t=50, b=10),
        colorway=C, height=height
    )
    leg = dict(bgcolor="rgba(0,0,0,0)", bordercolor=grid_color, borderwidth=1, font=dict(size=10, color=txt_color))
    if horizontal_legend: leg.update(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1)
    layout['legend'] = leg
    layout['xaxis'] = {**_AX, "title": {"text": x_title, "font": {"color": txt_color}}} if x_title else _AX
    if x_log: layout['xaxis']['type'] = 'log'
    layout['yaxis'] = {**_AX, "title": {"text": y_title, "font": {"color": txt_color}}} if y_title else _AX
    if y_log: layout['yaxis']['type'] = 'log'
    return layout

if not hasattr(st, "_original_plotly_chart"):
    st._original_plotly_chart = st.plotly_chart

def accessible_plotly_chart(fig, **kwargs):
    current_theme = st.session_state.get("theme_mode", "Sáng")
    if current_theme == "Tối":
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color=c["t1"]),
            colorway=C,
            legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=c["rail"], borderwidth=1),
        )
    elif current_theme == "Mù màu":
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#000000"),    
            colorway=C,
            coloraxis=dict(colorscale="Cividis"),   
            legend=dict(
                bgcolor="rgba(255,255,255,0.95)",   
                bordercolor="#B0B8C1",
                borderwidth=1.5,
            ),
        )
        patterns = ["/", "\\", "x", "-", "|", "+", "."]
        for i, trace in enumerate(fig.data):
            color = C[i % len(C)]
            if getattr(trace, "type", "") in ["bar", "histogram"]:
                trace.update(marker=dict(
                    color=color,
                    pattern=dict(
                        shape=patterns[i % len(patterns)],
                        fillmode="overlay",
                        fgcolor="rgba(0,0,0,0.55)",  
                        size=6,
                    ),
                    line=dict(width=1.5, color="#000000")  
                ))
            if getattr(trace, "type", "") in ["heatmap", "histogram2d"]:
                trace.update(colorscale="Cividis")
            if getattr(trace, "type", "") == "scatter":
                trace.update(
                    marker=dict(
                        color=color,
                        size=8,                             
                        symbol=["circle", "square", "diamond", "cross", "x",
                                "triangle-up", "triangle-down"][i % 7], 
                        line=dict(width=1.5, color="#000000")
                    ),
                    line=dict(color=color, width=2.0)       
                )
            if getattr(trace, "type", "") == "box":
                trace.update(
                    marker=dict(color=color, size=5),
                    line=dict(color="#000000", width=1.5),  
                    fillcolor=color
                )
            if getattr(trace, "type", "") == "pie":
                value_len = len(trace.values) if getattr(trace, "values", None) is not None else 0
                if value_len:
                    trace.update(
                        marker=dict(
                            colors=[C[j % len(C)] for j in range(value_len)],
                            line=dict(color="#000000", width=2),  
                        ),
                        textinfo="label+percent",  
                    )
    else:
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color=c["t1"]),
            colorway=C,
            legend=dict(bgcolor="rgba(255,255,255,0.65)", bordercolor=c["rail"], borderwidth=1),
        )

    if "theme" not in kwargs:
        kwargs["theme"] = None
    st._original_plotly_chart(fig, **kwargs)

st.plotly_chart = accessible_plotly_chart

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  6. TABS CONTENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
T1, T2, T3, T4, T5, T6 = st.tabs([
    "01 — TỔNG QUAN", 
    "02 — TRƯNG BÀY & GIẢM GIÁ", 
    "03 — GIÁ & DOANH THU",
    "04 — DANH MỤC", 
    "05 — PHÂN KHÚC GIÁ", 
    "06 — GIÁ TÂM LÝ & UY TÍN"
])

# ════════════════════════════════════════════════
# TAB 01 — TỔNG QUAN
# ════════════════════════════════════════════════
with T1:
    rev_B  = df["revenue_est"].sum() / 1e9 if "revenue_est" in df.columns else 0
    sold_K = df["sold_count"].sum() / 1e3 if "sold_count" in df.columns else 0
    ar     = df["rating"].mean() if "rating" in df.columns else 0
    ad     = df["discount_percent"].mean() if "discount_percent" in df.columns else 0
    af     = (df["is_freeship"].mean() * 100) if "is_freeship" in df.columns else 0

    st.markdown(f"""
    <div class="kpi-grid">
      <div class="kpi-cell accent"><span class="kpi-tag">◈ Sản phẩm lọc</span><div class="kpi-num hot">{N:,}</div><div class="kpi-sub">{pct:.1f}% dataset</div></div>
      <div class="kpi-cell"><span class="kpi-tag">Doanh thu ước tính</span><div class="kpi-num">{rev_B:.2f}B</div><div class="kpi-sub">VND</div></div>
      <div class="kpi-cell"><span class="kpi-tag">Lượt bán</span><div class="kpi-num">{sold_K:.1f}K</div><div class="kpi-sub">đơn vị</div></div>
      <div class="kpi-cell"><span class="kpi-tag">Rating trung bình</span><div class="kpi-num">{ar:.2f}</div><div class="kpi-sub">/ 5.0 sao</div></div>
      <div class="kpi-cell"><span class="kpi-tag">Discount trung bình</span><div class="kpi-num">{ad:.1f}%</div><div class="kpi-sub">so với giá gốc</div></div>
      <div class="kpi-cell"><span class="kpi-tag">Freeship</span><div class="kpi-num">{af:.0f}%</div><div class="kpi-sub">miễn phí ship</div></div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            if "category_name" in df.columns and "sold_count" in df.columns:
                top_cat = df.dropna(subset=["category_name", "sold_count"]).groupby("category_name", as_index=False)["sold_count"].mean().sort_values("sold_count", ascending=False).head(10)
                fig_cat = px.bar(top_cat, x="category_name", y="sold_count", labels={"category_name": "Danh mục", "sold_count": "Lượt bán TB"}, color_discrete_sequence=[C[0]])
                fig_cat.update_layout(**fig_layout(height=450, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Thị trường — Top danh mục theo lượt bán TB</b>", font=dict(size=14)))
                st.plotly_chart(fig_cat, use_container_width=True, config=FULL_BAR)

    with c2:
        with st.container(border=True):
            if "price_current" in df.columns:
                fig_hist = px.histogram(df.dropna(subset=["price_current"]), x="price_current", nbins=40, labels={"price_current": "Giá (VND)"}, color_discrete_sequence=[C[1]])
                fig_hist.update_layout(**fig_layout(height=450, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Giá cả — Phân bố giá sản phẩm</b>", font=dict(size=14)))
                st.plotly_chart(fig_hist, use_container_width=True, config=FULL_BAR)

# ════════════════════════════════════════════════
# TAB 02 — TRƯNG BÀY & GIẢM GIÁ
# ════════════════════════════════════════════════
with T2:
    st.markdown('<div style="margin-bottom: 1.5rem;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ CHẤT LƯỢNG TRƯNG BÀY SẢN PHẨM MALL</h3></div>', unsafe_allow_html=True)
    req_mall = ["is_mall", "image_count", "has_video", "five_star_with_image_count", "review_count", "sold_count"]
    if all(col in df.columns for col in req_mall):
        mall_df = df[df["is_mall"] == True].copy().dropna(subset=["sold_count"])
        if not mall_df.empty:
            mall_df["image_count"] = pd.to_numeric(mall_df["image_count"], errors="coerce").fillna(0)
            mall_df["has_video_num"] = mall_df["has_video"].astype(int)
            mall_df["review_count"] = pd.to_numeric(mall_df["review_count"], errors="coerce").fillna(0)
            mall_df["five_star_with_image_count"] = pd.to_numeric(mall_df["five_star_with_image_count"], errors="coerce").fillna(0)
            review_denom = mall_df["review_count"].replace(0, pd.NA)
            mall_df["five_star_img_rate"] = (mall_df["five_star_with_image_count"] / review_denom).fillna(0).clip(lower=0, upper=1)
            mall_df["display_quality_score"] = ((mall_df["image_count"].clip(upper=10) / 10) * 40 + mall_df["has_video_num"] * 30 + (mall_df["five_star_img_rate"].clip(upper=0.30) / 0.30) * 30)
            q75 = mall_df["sold_count"].quantile(0.75)

            st.markdown(f"""
            <div class="kpi-grid" style="grid-template-columns: repeat(3, 1fr);">
                <div class="kpi-cell"><span class="kpi-tag">Số SP Mall</span><div class="kpi-num hot">{len(mall_df):,}</div></div>
                <div class="kpi-cell"><span class="kpi-tag">Lượt bán TB (Mall)</span><div class="kpi-num">{mall_df['sold_count'].mean():,.0f}</div></div>
                <div class="kpi-cell"><span class="kpi-tag">Ngưỡng Top 25%</span><div class="kpi-num">>= {q75:,.0f}</div></div>
            </div>
            """, unsafe_allow_html=True)

            c1, c2 = st.columns(2)
            with c1:
                with st.container(border=True):
                    fig_score = px.scatter(mall_df, x="display_quality_score", y="sold_count",
                                           color=mall_df["has_video_num"].map({1: "Có video", 0: "Không video"}),
                                           color_discrete_sequence=[C[0], C[1]],   # Blue, Orange
                                           labels={"display_quality_score": "Điểm trưng bày", "sold_count": "Lượt bán", "color": "Trạng thái"})
                    fig_score.update_layout(**fig_layout(height=420, horizontal_legend=False, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Điểm trưng bày và lượt bán</b>", font=dict(size=14)))
                    st.plotly_chart(fig_score, use_container_width=True, config=FULL_BAR)

            with c2:
                with st.container(border=True):
                    mall_df["image_bucket"] = pd.cut(mall_df["image_count"], bins=[-1, 3, 6, 9, 99], labels=["0-3", "4-6", "7-9", "10+"])
                    sold_by_bucket = mall_df.groupby("image_bucket", observed=False, as_index=False)["sold_count"].mean()
                    fig_bucket = px.bar(sold_by_bucket, x="image_bucket", y="sold_count",
                                        color_discrete_sequence=[C[2]],   # Sky Blue
                                        labels={"image_bucket": "Nhóm số ảnh", "sold_count": "Lượt bán TB"})
                    fig_bucket.update_layout(**fig_layout(height=420, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Lượt bán TB theo nhóm số ảnh</b>", font=dict(size=14)))
                    st.plotly_chart(fig_bucket, use_container_width=True, config=FULL_BAR)

    st.markdown('<div style="margin: 2.5rem 0 1.5rem 0;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ HIỆU QUẢ GIẢM GIÁ THEO DẢI DISCOUNT</h3></div>', unsafe_allow_html=True)
    if "sold_count" in df.columns and "discount_percent" in df.columns:
        promo_df = df.dropna(subset=["sold_count", "discount_percent"]).copy()
        promo_df = promo_df[(promo_df["sold_count"] >= 0) & (promo_df["discount_percent"] >= 0)]
        promo_df["discount_percent"] = promo_df["discount_percent"].clip(upper=100)
        
        if not promo_df.empty:
            promo_df["discount_band"] = pd.cut(promo_df["discount_percent"], bins=[-0.01, 0, 5, 10, 20, 30, 50, 100], labels=["0%", "1-5%", "6-10%", "11-20%", "21-30%", "31-50%", ">50%"])
            band_stats = promo_df.groupby("discount_band", observed=False)["sold_count"].agg(avg_sold="mean", median_sold="median", sample_size="count").reset_index()

            st.markdown(f"""
            <div class="kpi-grid" style="grid-template-columns: repeat(3, 1fr);">
                <div class="kpi-cell"><span class="kpi-tag">Mẫu phân tích</span><div class="kpi-num hot">{len(promo_df):,}</div></div>
                <div class="kpi-cell"><span class="kpi-tag">Discount TB (%)</span><div class="kpi-num">{promo_df['discount_percent'].mean():.1f}%</div></div>
                <div class="kpi-cell"><span class="kpi-tag">Lượt bán TB</span><div class="kpi-num">{promo_df['sold_count'].mean():,.0f}</div></div>
            </div>
            """, unsafe_allow_html=True)

            g1, g2 = st.columns(2)
            with g1:
                with st.container(border=True):
                    fig_band = px.bar(band_stats.sort_values("avg_sold", ascending=False),
                                      x="discount_band", y="avg_sold",
                                      color="sample_size",
                                      color_continuous_scale="Cividis",   
                                      labels={"discount_band": "Dải discount", "avg_sold": "Lượt bán TB", "sample_size": "Mẫu"})
                    fig_band.update_layout(**fig_layout(height=420, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Lượt bán TB theo dải discount</b>", font=dict(size=14)))
                    st.plotly_chart(fig_band, use_container_width=True, config=FULL_BAR)

            with g2:
                with st.container(border=True):
                    fig_box = px.box(promo_df, x="discount_band", y="sold_count",
                                     color_discrete_sequence=[C[3]],   
                                     labels={"discount_band": "Dải discount", "sold_count": "Lượt bán"})
                    fig_box.update_layout(**fig_layout(height=420, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Phân bố lượt bán thực tế (Log Scale)</b>", font=dict(size=14)))
                    fig_box.update_yaxes(type="log")
                    st.plotly_chart(fig_box, use_container_width=True, config=FULL_BAR)

            with st.container(border=True):
                st.markdown('<div style="font-weight: 600; color: var(--t1); margin-bottom: 15px; font-size: 14px;">Chi tiết hiệu quả theo dải</div>', unsafe_allow_html=True)
                disp_band = band_stats.sort_values("avg_sold", ascending=False).rename(columns={"discount_band": "Dải Discount", "avg_sold": "Lượt bán TB", "median_sold": "Lượt bán Trung vị", "sample_size": "Số lượng SP"})
                st.dataframe(disp_band, use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════
# TAB 03 — GIÁ & DOANH THU
# ════════════════════════════════════════════════
with T3:
    st.markdown('<div style="margin-bottom: 1.5rem;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ ĐỘ NHẠY GIÁ & DOANH THU</h3></div>', unsafe_allow_html=True)

    med_price = df['price_current'].median() if "price_current" in df.columns else 0
    med_sold  = df['sold_count'].median() if "sold_count" in df.columns else 0
    total_rev = df['revenue_est'].sum() if "revenue_est" in df.columns else 0
    rating_series = df["rating"].dropna() if "rating" in df.columns else pd.Series(dtype=float)
    high_rating_ratio = ((rating_series >= 4.5).mean() * 100) if not rating_series.empty else 0

    st.markdown(f"""
    <div class="kpi-grid" style="grid-template-columns: repeat(4, 1fr);">
        <div class="kpi-cell"><span class="kpi-tag">Giá trung vị</span><div class="kpi-num">{med_price:,.0f} <span style="font-size:1rem; font-weight:500;">VND</span></div></div>
        <div class="kpi-cell"><span class="kpi-tag">Lượt bán trung vị</span><div class="kpi-num">{med_sold:,.0f}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Doanh thu ước tính</span><div class="kpi-num hot">{total_rev/1e9:,.1f} <span style="font-size:1rem; font-weight:500;">Tỷ</span></div></div>
        <div class="kpi-cell"><span class="kpi-tag">Tỷ lệ Rating ≥ 4.5</span><div class="kpi-num">{high_rating_ratio:.1f}%</div></div>
    </div>
    """, unsafe_allow_html=True)

    req_price = ["price_current", "sold_count"]
    if all(col in df.columns for col in req_price):
        price_df = df.dropna(subset=req_price).copy()
        price_df = price_df[(price_df["price_current"] > 0) & (price_df["sold_count"] > 0)]
        color_col = "price_bucket" if "price_bucket" in price_df.columns else None

        c1, c2 = st.columns(2)
        with c1:
            with st.container(border=True):
                fig_scatter = px.scatter(price_df, x="price_current", y="sold_count",
                                          color=color_col, log_x=True, log_y=True, opacity=0.6,
                                          color_discrete_sequence=C,
                                          labels={"price_current": "Giá (VND)", "sold_count": "Lượt bán", "price_bucket": "Phân khúc"})
                fig_scatter.update_layout(**fig_layout(height=450, horizontal_legend=False, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Giá vs Lượt bán (Log-Log Scale)</b>", font=dict(size=14)))
                st.plotly_chart(fig_scatter, use_container_width=True, config=FULL_BAR)

        with c2:
            with st.container(border=True):
                if len(price_df) >= 20:
                    tmp = price_df.copy()
                    tmp["price_bin"] = pd.qcut(tmp["price_current"], q=10, duplicates="drop")
                    bin_df = tmp.groupby("price_bin", observed=False).agg(avg_sold=("sold_count", "mean"), revenue=("revenue_est", "sum")).reset_index()
                    bin_df["price_bin"] = bin_df["price_bin"].astype(str)

                    fig_mix = make_subplots(specs=[[{"secondary_y": True}]])
                    fig_mix.add_trace(go.Bar(x=bin_df["price_bin"], y=bin_df["revenue"]/1e9, name="Doanh thu (Tỷ)", marker_color=C[0]), secondary_y=False)
                    fig_mix.add_trace(go.Scatter(x=bin_df["price_bin"], y=bin_df["avg_sold"], mode="lines+markers", name="Lượt bán TB", line=dict(color=C[1], width=3)), secondary_y=True)
                    
                    fig_mix.update_layout(**fig_layout(height=450, horizontal_legend=True, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Doanh thu & Lượt bán theo khoảng giá</b>", font=dict(size=14)))
                    fig_mix.update_yaxes(title_text="Doanh thu (Tỷ)", gridcolor="rgba(128,128,128,0.1)", secondary_y=False)
                    fig_mix.update_yaxes(title_text="Lượt bán TB", gridcolor="rgba(0,0,0,0)", secondary_y=True)
                    fig_mix.update_xaxes(tickangle=-40)
                    st.plotly_chart(fig_mix, use_container_width=True, config=FULL_BAR)
                else:
                    st.info("Không đủ dữ liệu để chia 10 nhóm giá.")

    st.markdown('<div style="margin: 2.5rem 0 1.5rem 0;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ TÁC ĐỘNG CỦA RATING & REVIEW</h3></div>', unsafe_allow_html=True)
    req_rtg = ["price_current", "rating", "review_count", "sold_count"]
    if all(col in df.columns for col in req_rtg):
        d = df.dropna(subset=req_rtg).copy()
        
        c3, c4 = st.columns([1, 1.3])
        with c3:
            with st.container(border=True):
                corr_df = d[req_rtg].corr(method="spearman")
                corr_df.columns = ["Giá", "Rating", "Review", "Lượt bán"]; corr_df.index = ["Giá", "Rating", "Review", "Lượt bán"]
                fig_corr = px.imshow(corr_df, text_auto=".2f", aspect="auto",
                                     color_continuous_scale="PuOr",     
                                     color_continuous_midpoint=0)
                fig_corr.update_layout(**fig_layout(height=400, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Ma trận tương quan Spearman</b>", font=dict(size=14)))
                st.plotly_chart(fig_corr, use_container_width=True, config=FULL_BAR)

        with c4:
            with st.container(border=True):
                d2 = d[(d["rating"] >= 0) & (d["rating"] <= 5)].copy()
                d2["rating_group"] = d2["rating"].round(1).astype(str)
                d2 = d2.sort_values("rating_group")
                fig_box = px.box(d2, x="rating_group", y="sold_count",
                                 color_discrete_sequence=[C[4]],    
                                 labels={"rating_group": "Mức Rating", "sold_count": "Lượt bán"})
                fig_box.update_layout(**fig_layout(height=400, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Phân phối lượt bán theo nhóm Rating</b>", font=dict(size=14)))
                fig_box.update_yaxes(type="log")
                st.plotly_chart(fig_box, use_container_width=True, config=FULL_BAR)

        with st.container(border=True):
            bins_review = [0, 10, 50, 200, 1000, np.inf]
            labels_review = ["0-10", "11-50", "51-200", "201-1000", "1000+"]
            bins_rating = [-1, 3.5, 4.0, 4.5, 5.0]
            labels_rating = ["<3.5", "3.6-4.0", "4.1-4.5", "4.6-5.0"]
            d3 = d.copy()
            d3["review_segment"] = pd.cut(d3["review_count"], bins=bins_review, labels=labels_review)
            d3["rating_segment"] = pd.cut(d3["rating"], bins=bins_rating, labels=labels_rating)
            pivot = d3.pivot_table(values="sold_count", index="rating_segment", columns="review_segment", aggfunc="mean", observed=False)
            
            if not pivot.empty:
                fig_heat = px.imshow(pivot, text_auto=".0f", aspect="auto",
                                     color_continuous_scale="Cividis", 
                                     labels=dict(x="Lượng Review", y="Mức Rating", color="Lượt bán TB"))
                fig_heat.update_layout(**fig_layout(height=450, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Hiệu ứng kết hợp: Rating × Khối lượng Review</b>", font=dict(size=14)))
                st.plotly_chart(fig_heat, use_container_width=True, config=FULL_BAR)

# ════════════════════════════════════════════════
# TAB 04 — DANH MỤC
# ════════════════════════════════════════════════
with T4:
    st.markdown('<div style="margin-bottom: 1.5rem;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ HIỆU QUẢ THEO DANH MỤC VÀ PHÂN KHÚC GIÁ</h3></div>', unsafe_allow_html=True)

    if "price_bucket" not in df.columns and "price_current" in df.columns:
        df["price_bucket"] = pd.cut(df["price_current"], bins=[-np.inf, 100000, 500000, 1000000, 5000000, np.inf], labels=["<100k", "100k-500k", "500k-1M", "1M-5M", ">5M"])

    category_col = "category_name"
    n_cats = df[category_col].nunique() if category_col in df.columns else 0
    n_buckets = df['price_bucket'].astype(str).nunique() if "price_bucket" in df.columns else 0
    disc_ratio = (df["discount_percent"].fillna(0) > 0).mean() * 100 if "discount_percent" in df.columns else 0
    
    lift_text = "N/A"
    if {"discount_percent", "sold_count"}.issubset(df.columns):
        lift_df = df.dropna(subset=["discount_percent", "sold_count"]).copy()
        sold_discount = lift_df[lift_df["discount_percent"] > 0]["sold_count"].mean()
        sold_non_discount = lift_df[lift_df["discount_percent"] <= 0]["sold_count"].mean()
        if pd.notna(sold_discount) and pd.notna(sold_non_discount) and sold_non_discount != 0:
            lift_text = f"{((sold_discount - sold_non_discount) / sold_non_discount) * 100:+.1f}%"

    st.markdown(f"""
    <div class="kpi-grid" style="grid-template-columns: repeat(4, 1fr);">
        <div class="kpi-cell"><span class="kpi-tag">Số danh mục</span><div class="kpi-num">{n_cats:,}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Số phân khúc giá</span><div class="kpi-num">{n_buckets:,}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Tỷ lệ SP có giảm giá</span><div class="kpi-num">{disc_ratio:.1f}%</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Lift bán hàng (Discount)</span><div class="kpi-num hot">{lift_text}</div></div>
    </div>
    """, unsafe_allow_html=True)

    req = ["sold_count", "price_bucket", category_col]
    if all(col in df.columns for col in req):
        ydf = df.dropna(subset=req).copy()
        top_cats = ydf[category_col].astype(str).value_counts().head(12).index.tolist()
        ydf = ydf[ydf[category_col].astype(str).isin(top_cats)].copy()
        agg = ydf.groupby([category_col, "price_bucket"], observed=False)["sold_count"].mean().reset_index()
        
        c1, c2 = st.columns(2)
        with c1:
            with st.container(border=True):
                fig_bar = px.bar(agg, x=category_col, y="sold_count", color="price_bucket", barmode="group",
                                 color_discrete_sequence=C,
                                 labels={category_col: "Danh mục", "sold_count": "Lượt bán TB", "price_bucket": "Phân khúc"})
                fig_bar.update_layout(**fig_layout(height=450, horizontal_legend=False, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Lượt bán TB theo danh mục và phân khúc giá</b>", font=dict(size=14)), xaxis_tickangle=-40)
                st.plotly_chart(fig_bar, use_container_width=True, config=FULL_BAR)

        with c2:
            with st.container(border=True):
                heat = agg.pivot(index=category_col, columns="price_bucket", values="sold_count")
                fig_heat = px.imshow(heat, text_auto=".0f", aspect="auto",
                                     color_continuous_scale="Cividis",
                                     labels=dict(x="Phân khúc giá", y="Danh mục", color="Lượt bán TB"))
                fig_heat.update_layout(**fig_layout(height=450, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Heatmap lượt bán TB theo phân khúc</b>", font=dict(size=14)))
                st.plotly_chart(fig_heat, use_container_width=True, config=FULL_BAR)

        with st.container(border=True):
            st.markdown('<div style="font-weight: 600; color: var(--t1); margin-bottom: 15px; font-size: 14px;">Top 3 phân khúc giá hoạt động tốt nhất ở mỗi Danh mục</div>', unsafe_allow_html=True)
            stats = ydf.groupby([category_col, "price_bucket"], observed=False).agg(so_luong_sp=("product_id", "count"), luot_ban_tb=("sold_count", "mean")).reset_index().sort_values("luot_ban_tb", ascending=False)
            top3 = stats.groupby(category_col, observed=False).head(3)
            top3.columns = ["Danh mục", "Phân khúc giá", "Số lượng SP", "Lượt bán TB"]
            st.dataframe(top3, use_container_width=True, hide_index=True)

    st.markdown('<div style="margin: 2.5rem 0 1.5rem 0;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ TÁC ĐỘNG GIẢM GIÁ LÊN DOANH SỐ</h3></div>', unsafe_allow_html=True)
    if "discount_percent" in df.columns and "sold_count" in df.columns:
        y2 = df.dropna(subset=["sold_count", "discount_percent"]).copy()
        y2["Trang_Thai_Giam_Gia"] = np.where(y2["discount_percent"] > 0, "Có Giảm Giá", "Không Giảm Giá")

        d1, d2 = st.columns(2)
        with d1:
            with st.container(border=True):
                discount_mean = y2.groupby("Trang_Thai_Giam_Gia", as_index=False)["sold_count"].mean()
                fig_bar2 = px.bar(discount_mean, x="Trang_Thai_Giam_Gia", y="sold_count",
                                  color="Trang_Thai_Giam_Gia",
                                  color_discrete_map={"Có Giảm Giá": C[3], "Không Giảm Giá": C[0]},  # Vermillion, Blue
                                  text_auto=".1f",
                                  labels={"Trang_Thai_Giam_Gia": "Trạng thái", "sold_count": "Lượt bán TB"})
                fig_bar2.update_layout(**fig_layout(height=420, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Lượt bán TB: Có vs Không Giảm Giá</b>", font=dict(size=14)), showlegend=False)
                st.plotly_chart(fig_bar2, use_container_width=True, config=FULL_BAR)

        with d2:
            with st.container(border=True):
                discount_sum = y2.groupby("Trang_Thai_Giam_Gia", as_index=False)["sold_count"].sum()
                fig_pie = px.pie(discount_sum, names="Trang_Thai_Giam_Gia", values="sold_count", hole=0.5,
                                 color="Trang_Thai_Giam_Gia",
                                 color_discrete_map={"Có Giảm Giá": C[3], "Không Giảm Giá": C[0]})  # Vermillion, Blue
                fig_pie.update_layout(**fig_layout(height=420, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Tỷ trọng Tổng lượt bán thực tế</b>", font=dict(size=14)))
                st.plotly_chart(fig_pie, use_container_width=True, config=FULL_BAR)

        with st.container(border=True):
            if category_col in y2.columns:
                top_cats2 = y2[category_col].astype(str).value_counts().head(10).index.tolist()
                y2c = y2[y2[category_col].astype(str).isin(top_cats2)]
                cat_discount = y2c.groupby([category_col, "Trang_Thai_Giam_Gia"], observed=False)["sold_count"].mean().reset_index()
                
                fig_cat_disc = px.bar(cat_discount, x=category_col, y="sold_count",
                                      color="Trang_Thai_Giam_Gia", barmode="group",
                                      color_discrete_map={"Có Giảm Giá": C[3], "Không Giảm Giá": C[0]},
                                      text_auto=".0f",
                                      labels={category_col: "Danh mục", "sold_count": "Lượt bán TB", "Trang_Thai_Giam_Gia": "Trạng thái"})
                fig_cat_disc.update_layout(**fig_layout(height=450, horizontal_legend=True, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Lượt bán TB Có/Không giảm giá theo Danh mục</b>", font=dict(size=14)), xaxis_tickangle=-30)
                st.plotly_chart(fig_cat_disc, use_container_width=True, config=FULL_BAR)

# ════════════════════════════════════════════════
# TAB 05 — PHÂN KHÚC GIÁ
# ════════════════════════════════════════════════
with T5:
    st.markdown('<div style="margin-bottom: 1.5rem;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ PHÂN KHÚC THỊ TRƯỜNG THEO GIÁ VÀ RATING</h3></div>', unsafe_allow_html=True)
    if "price_bucket" not in df.columns and "price_current" in df.columns:
        df["price_bucket"] = pd.cut(df["price_current"], bins=[-np.inf, 100000, 500000, 1000000, 5000000, np.inf], labels=["<100k", "100k-500k", "500k-1M", "1M-5M", ">5M"])

    n_buckets = df["price_bucket"].nunique() if "price_bucket" in df.columns else 0
    top_bucket = df["price_bucket"].value_counts().index[0] if "price_bucket" in df.columns and not df["price_bucket"].empty else "N/A"
    
    top_rev_bucket = "N/A"
    if {"price_bucket", "revenue_est"}.issubset(df.columns):
        rev_by_bucket = df.dropna(subset=["price_bucket", "revenue_est"]).groupby("price_bucket", observed=False)["revenue_est"].sum()
        top_rev_bucket = rev_by_bucket.idxmax() if not rev_by_bucket.empty else "N/A"
        
    avg_rating = df['rating'].mean() if "rating" in df.columns else 0

    st.markdown(f"""
    <div class="kpi-grid" style="grid-template-columns: repeat(4, 1fr);">
        <div class="kpi-cell"><span class="kpi-tag">Số phân khúc giá</span><div class="kpi-num">{n_buckets}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Phân khúc phổ biến nhất</span><div class="kpi-num hot" style="font-size:1.4rem;">{top_bucket}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Phân khúc DT cao nhất</span><div class="kpi-num" style="color:#D55E00; font-size:1.4rem;">{top_rev_bucket}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Rating trung bình</span><div class="kpi-num">{avg_rating:.2f} ⭐</div></div>
    </div>
    """, unsafe_allow_html=True)

    req = ["price_bucket", "price_current", "sold_count", "rating"]
    if all(col in df.columns for col in req):
        d = df.dropna(subset=req).copy()
        d = d[(d["price_current"] > 0) & (d["sold_count"] >= 0)]

        c1, c2 = st.columns(2)
        with c1:
            with st.container(border=True):
                count_df = d.groupby("price_bucket", observed=False, as_index=False)["product_id"].count()
                count_df.columns = ["price_bucket", "so_luong_sp"]
                fig_count = px.bar(count_df, x="price_bucket", y="so_luong_sp", text_auto=True,
                                   color_discrete_sequence=[C[0]],   # Blue
                                   labels={"price_bucket": "Phân khúc giá", "so_luong_sp": "Số lượng SP"})
                fig_count.update_layout(**fig_layout(height=420, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Phân bố Số lượng SP theo phân khúc giá</b>", font=dict(size=14)))
                st.plotly_chart(fig_count, use_container_width=True, config=FULL_BAR)

        with c2:
            with st.container(border=True):
                fig_box = px.box(d, x="price_bucket", y="sold_count",
                                 color_discrete_sequence=[C[1]],   
                                 labels={"price_bucket": "Phân khúc giá", "sold_count": "Lượt bán"})
                fig_box.update_layout(**fig_layout(height=420, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Phân phối Lượt bán theo phân khúc (Log Scale)</b>", font=dict(size=14)))
                fig_box.update_yaxes(type="log")
                st.plotly_chart(fig_box, use_container_width=True, config=FULL_BAR)

        with st.container(border=True):
            seg = d.groupby("price_bucket", observed=False).agg(revenue=("revenue_est", "sum"), sold_mean=("sold_count", "mean")).reset_index()
            fig_mix = make_subplots(specs=[[{"secondary_y": True}]])
            fig_mix.add_trace(go.Bar(x=seg["price_bucket"], y=seg["revenue"]/1e9, name="Doanh thu (Tỷ VND)", marker_color=C[0]), secondary_y=False)
            fig_mix.add_trace(go.Scatter(x=seg["price_bucket"], y=seg["sold_mean"], mode="lines+markers", name="Lượt bán TB", line=dict(color=C[1], width=3)), secondary_y=True)
            fig_mix.update_layout(**fig_layout(height=450, horizontal_legend=False, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Doanh thu và Lượt bán TB theo phân khúc</b>", font=dict(size=14)))
            fig_mix.update_yaxes(title_text="Doanh thu (Tỷ)", gridcolor="rgba(128,128,128,0.1)", secondary_y=False)
            fig_mix.update_yaxes(title_text="Lượt bán TB", gridcolor="rgba(0,0,0,0)", secondary_y=True)
            st.plotly_chart(fig_mix, use_container_width=True, config=FULL_BAR)

        e1, e2 = st.columns(2)
        with e1:
            with st.container(border=True):
                d_heat = d.copy()
                d_heat["rating_group"] = d_heat["rating"].round(1)
                pivot = d_heat.pivot_table(values="sold_count", index="price_bucket", columns="rating_group", aggfunc="mean", observed=False)
                fig_heat = px.imshow(pivot, aspect="auto",
                                     color_continuous_scale="Cividis", 
                                     text_auto=".0f",
                                     labels=dict(x="Mức Rating", y="Phân khúc giá", color="Lượt bán TB"))
                fig_heat.update_layout(**fig_layout(height=450, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Heatmap Lượt bán TB theo Phân khúc & Rating</b>", font=dict(size=14)))
                st.plotly_chart(fig_heat, use_container_width=True, config=FULL_BAR)
            
        with e2:
            with st.container(border=True):
                fig_scatter = px.scatter(d, x="price_current", y="sold_count",
                                          color="price_bucket", log_x=True, log_y=True, opacity=0.6,
                                          color_discrete_sequence=C,
                                          labels={"price_current": "Giá (VND)", "sold_count": "Lượt bán", "price_bucket": "Phân khúc"})
                fig_scatter.update_layout(**fig_layout(height=450, horizontal_legend=True, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Giá vs Lượt bán theo phân khúc (Log-Log)</b>", font=dict(size=14)))
                st.plotly_chart(fig_scatter, use_container_width=True, config=FULL_BAR)

# ════════════════════════════════════════════════
# TAB 06 — GIÁ TÂM LÝ & UY TÍN SHOP
# ════════════════════════════════════════════════
with T6:
    st.markdown('<div style="margin-bottom: 1.5rem;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ HIỆU ỨNG GIÁ TÂM LÝ & TRUST INDEX</h3></div>', unsafe_allow_html=True)
    psy_avg_text, regular_avg_text, delta_text, delta_val = "N/A", "N/A", "N/A", 0
    
    if {"price_current", "sold_count"}.issubset(df.columns):
        d_kpi = df.dropna(subset=["price_current", "sold_count"]).copy()
        d_kpi = d_kpi[d_kpi["price_current"] > 0]
        if not d_kpi.empty:
            d_kpi["is_psy_price"] = d_kpi["price_current"].astype(int).astype(str).str[-3:].str.contains("9")
            psy_mean = d_kpi[d_kpi["is_psy_price"]]["sold_count"].mean()
            regular_mean = d_kpi[~d_kpi["is_psy_price"]]["sold_count"].mean()
            psy_avg_text = f"{psy_mean:,.0f}" if pd.notna(psy_mean) else "N/A"
            regular_avg_text = f"{regular_mean:,.0f}" if pd.notna(regular_mean) else "N/A"
            if pd.notna(psy_mean) and pd.notna(regular_mean) and regular_mean != 0:
                delta_val = ((psy_mean - regular_mean) / regular_mean) * 100
                delta_text = f"{delta_val:+.1f}%"

    trust_text = "N/A"
    if {"rating", "review_count"}.issubset(df.columns):
        trust_series = (df["rating"] * 0.7) + (np.log1p(df["review_count"]) * 0.3)
        trust_text = f"{trust_series.mean():.2f}" if not trust_series.dropna().empty else "N/A"

    delta_color = C[4] if delta_val > 0 else C[3]   
    st.markdown(f"""
    <div class="kpi-grid" style="grid-template-columns: repeat(4, 1fr);">
        <div class="kpi-cell"><span class="kpi-tag">Lượt bán TB (Giá tâm lý)</span><div class="kpi-num hot">{psy_avg_text}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Lượt bán TB (Giá thường)</span><div class="kpi-num">{regular_avg_text}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Chênh lệch hiệu quả</span><div class="kpi-num" style="color:{delta_color};">{delta_text}</div></div>
        <div class="kpi-cell"><span class="kpi-tag">Trust Index Trung bình</span><div class="kpi-num">{trust_text}</div></div>
    </div>
    """, unsafe_allow_html=True)

    req = ["price_current", "sold_count"]
    if all(col in df.columns for col in req):
        d = df.dropna(subset=req).copy()
        d = d[d["price_current"] > 0]
        def is_psychological_price(price_val: float) -> bool:
            if pd.isna(price_val): return False
            return "9" in str(int(price_val))[-3:]

        d["is_psy_price"] = d["price_current"].apply(is_psychological_price)
        psy_viz = d.groupby("is_psy_price", as_index=False)["sold_count"].mean()
        psy_viz["label"] = psy_viz["is_psy_price"].map({True: "Giá có đuôi 9 (Tâm lý)", False: "Giá thông thường"})

        with st.container(border=True):
            fig1 = px.bar(psy_viz.sort_values("sold_count", ascending=False),
                          x="label", y="sold_count", color="label", text_auto=".0f",
                          color_discrete_map={
                              "Giá có đuôi 9 (Tâm lý)": C[0],  
                              "Giá thông thường": C[2]             
                          },
                          labels={"label": "Chiến lược giá", "sold_count": "Lượt bán TB"})
            fig1.update_layout(**fig_layout(height=450, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>So sánh Lượt bán TB: Giá đuôi 9 vs Giá tròn</b>", font=dict(size=14)), showlegend=False)
            st.plotly_chart(fig1, use_container_width=True, config=FULL_BAR)

    st.markdown('<div style="margin: 2.5rem 0 1.5rem 0;"><h3 style="color: var(--t1); font-weight: 700; font-size: 1.4rem;">◈ TRUST INDEX VÀ UY TÍN GIAN HÀNG</h3></div>', unsafe_allow_html=True)
    req2 = ["rating", "review_count", "sold_count"]
    if all(col in df.columns for col in req2):
        d2 = df.dropna(subset=req2).copy()
        d2["trust_index_score"] = (d2["rating"] * 0.7) + (np.log1p(d2["review_count"]) * 0.3)
        d2 = d2[d2["sold_count"] > 0]
        
        if not d2.empty:
            color_col = "is_mall" if "is_mall" in d2.columns else None
            hover_name = "product_name" if "product_name" in d2.columns else None
            
            with st.container(border=True):
                fig2 = px.scatter(d2, x="trust_index_score", y="sold_count",
                                   size="revenue_est" if "revenue_est" in d2.columns else None,
                                   color=color_col, hover_name=hover_name, opacity=0.7,
                                   color_discrete_sequence=[C[1], C[0]],   
                                   labels={"trust_index_score": "Điểm Trust Index", "sold_count": "Lượt bán", "is_mall": "Loại Gian Hàng", "revenue_est": "Doanh thu"})
                fig2.update_layout(**fig_layout(height=500, horizontal_legend=False, margin=dict(t=50, l=10, r=10, b=10)), title=dict(text="<b>Tương quan giữa Trust Index và Lượt bán thực tế</b>", font=dict(size=14)))
                fig2.update_yaxes(type="log")
                st.plotly_chart(fig2, use_container_width=True, config=FULL_BAR)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOOTER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
st.markdown("""
<div style="border-top:1px solid var(--rail); margin-top:3rem; padding:1.5rem 0 2rem; display:flex; justify-content:space-between; align-items:flex-start;">
  <div style="font-size:0.75rem; color:var(--t3); letter-spacing:0.05em; line-height: 1.6;">
    <b style="color:var(--t1);">◈ TIKI INTELLIGENCE</b> · E-COMMERCE ANALYTICS<br>
    Data Visualization · VNU-HCMUS
  </div>
  <div style="font-size:0.75rem; color:var(--t3); letter-spacing:0.05em; text-align:right; line-height: 1.6;">
    <b>Developed by Team 13:</b><br>
    Tuan · Thinh · The Anh · Y · Duong
  </div>
</div>
""", unsafe_allow_html=True)