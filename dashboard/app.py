"""Streamlit dashboard tổng hợp toàn bộ EDA của nhóm Lab 01."""

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
from plotly.subplots import make_subplots


st.set_page_config(page_title="TMDT Analytics Dashboard", page_icon="cart", layout="wide")

PALETTE = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#17becf"]
COLORBLIND_PALETTE = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#CC79A7", "#56B4E9", "#F0E442", "#000000"]
COLORBLIND_PATTERN_SEQUENCE = ["/", "\\", "x", "-", "+", "."]
COLORBLIND_CONTINUOUS_SCALE = "Cividis"
px.defaults.template = "plotly_white"
px.defaults.color_discrete_sequence = PALETTE

DATA_DIR = Path(__file__).parent.parent / "data" / "processed"
DATA_CANDIDATES = [
    DATA_DIR / "fact_product_enriched.csv",
    DATA_DIR / "fact_product_merged.csv",
]


def normalize_bool(series: pd.Series) -> pd.Series:
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
    }
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map(mapping)
        .fillna(False)
    )


@st.cache_data
def load_data() -> tuple[pd.DataFrame, str]:
    for path in DATA_CANDIDATES:
        if path.exists():
            return pd.read_csv(path, encoding="utf-8-sig"), path.name
    return pd.DataFrame(), ""


def missing_columns(df_input: pd.DataFrame, required_cols: list[str]) -> list[str]:
    return [col for col in required_cols if col not in df_input.columns]


def get_category_col(df_input: pd.DataFrame) -> str | None:
    if "category_name" in df_input.columns:
        return "category_name"
    if "category_id" in df_input.columns:
        return "category_id"
    return None


def ensure_price_bucket(df_input: pd.DataFrame) -> None:
    if "price_bucket" in df_input.columns or "price_current" not in df_input.columns:
        return
    df_input["price_bucket"] = pd.cut(
        df_input["price_current"],
        bins=[-np.inf, 100000, 500000, 1000000, 5000000, np.inf],
        labels=["<100k", "100k-500k", "500k-1M", "1M-5M", ">5M"],
    )


def apply_theme(theme_mode: str) -> None:
    st.session_state["theme_mode"] = theme_mode
    if theme_mode == "Mù màu":
        px.defaults.template = "plotly_white"
        pio.templates.default = "plotly_white"
        px.defaults.color_discrete_sequence = COLORBLIND_PALETTE
        css = """
        <style>
        [data-testid="stAppViewContainer"] { background-color: #f8fafc; color: #111827; }
        [data-testid="stHeader"] { background: rgba(248, 250, 252, 0.95); }
        [data-testid="stSidebar"] { background-color: #f1f5f9; }
        [data-testid="stSidebar"] * { color: #111827 !important; }
        .stTabs [data-baseweb="tab"] { color: #334155 !important; }
        .stTabs [aria-selected="true"] {
            color: #111827 !important;
            font-weight: 700;
            border-bottom: 3px solid #0f172a;
        }
        h1, h2, h3, h4, h5, h6, p, span, label, div { color: #111827; }
        div[data-testid="stMetric"] {
            background-color: #ffffff;
            border: 2px solid #94a3b8;
            border-radius: 12px;
            padding: 8px;
        }
        div[data-testid="stDataFrame"] {
            background-color: #ffffff !important;
            border: 2px solid #94a3b8;
            border-radius: 10px;
        }
        div[data-testid="stDataFrame"] [role="grid"] {
            background-color: #ffffff !important;
            color: #111827 !important;
        }
        div[data-testid="stDataFrame"] [role="columnheader"] {
            background-color: #e2e8f0 !important;
            color: #111827 !important;
            font-weight: 700;
        }
        div[data-testid="stDataFrame"] [role="gridcell"] {
            background-color: #ffffff !important;
            color: #111827 !important;
        }
        </style>
        """
    elif theme_mode == "Tối":
        px.defaults.template = "plotly_dark"
        pio.templates.default = "plotly_dark"
        px.defaults.color_discrete_sequence = PALETTE
        css = """
        <style>
        [data-testid="stAppViewContainer"] { background-color: #0f172a; color: #e2e8f0; }
        [data-testid="stHeader"] { background: rgba(15, 23, 42, 0.75); }
        [data-testid="stSidebar"] { background-color: #111827; }
        [data-testid="stSidebar"] * { color: #e5e7eb !important; }
        .stTabs [data-baseweb="tab"] { color: #cbd5e1 !important; }
        .stTabs [aria-selected="true"] { color: #ffffff !important; }
        h1, h2, h3, h4, h5, h6, p, span, label, div { color: #e2e8f0; }
        div[data-testid="stMetric"] {
            background-color: #111827;
            border: 1px solid #1f2937;
            border-radius: 12px;
            padding: 8px;
        }
        div[data-testid="stDataFrame"] {
            background-color: #0b1220 !important;
            border: 1px solid #1f2937;
            border-radius: 10px;
        }
        div[data-testid="stDataFrame"] [role="grid"] {
            background-color: #0b1220 !important;
            color: #e2e8f0 !important;
        }
        div[data-testid="stDataFrame"] [role="columnheader"] {
            background-color: #1e293b !important;
            color: #e2e8f0 !important;
        }
        div[data-testid="stDataFrame"] [role="gridcell"] {
            background-color: #0b1220 !important;
            color: #e2e8f0 !important;
        }
        </style>
        """
    elif theme_mode == "Sáng":
        px.defaults.template = "plotly_white"
        pio.templates.default = "plotly_white"
        px.defaults.color_discrete_sequence = PALETTE
        css = """
        <style>
        [data-testid="stAppViewContainer"] { background-color: #f8fafc; color: #0f172a; }
        [data-testid="stHeader"] { background: rgba(248, 250, 252, 0.85); }
        [data-testid="stSidebar"] { background-color: #eef2ff; }
        [data-testid="stSidebar"] * { color: #0f172a !important; }
        .stTabs [data-baseweb="tab"] { color: #334155 !important; }
        .stTabs [aria-selected="true"] { color: #0f172a !important; }
        h1, h2, h3, h4, h5, h6, p, span, label, div { color: #0f172a; }
        div[data-testid="stMetric"] {
            background-color: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            padding: 8px;
        }
        div[data-testid="stDataFrame"] {
            background-color: #ffffff !important;
            border: 1px solid #dbe4f0;
            border-radius: 10px;
        }
        div[data-testid="stDataFrame"] [role="grid"] {
            background-color: #ffffff !important;
            color: #0f172a !important;
        }
        div[data-testid="stDataFrame"] [role="columnheader"] {
            background-color: #f1f5f9 !important;
            color: #0f172a !important;
        }
        div[data-testid="stDataFrame"] [role="gridcell"] {
            background-color: #ffffff !important;
            color: #0f172a !important;
        }
        </style>
        """
    else:
        px.defaults.template = "plotly_white"
        pio.templates.default = "plotly_white"
        px.defaults.color_discrete_sequence = PALETTE
        css = """
        <style>
        @media (prefers-color-scheme: dark) {
            [data-testid="stAppViewContainer"] { background-color: #0f172a; color: #e2e8f0; }
            [data-testid="stHeader"] { background: rgba(15, 23, 42, 0.75); }
            [data-testid="stSidebar"] { background-color: #111827; }
            [data-testid="stSidebar"] * { color: #e5e7eb !important; }
            .stTabs [data-baseweb="tab"] { color: #cbd5e1 !important; }
            .stTabs [aria-selected="true"] { color: #ffffff !important; }
            div[data-testid="stMetric"] {
                background-color: #111827;
                border: 1px solid #1f2937;
                border-radius: 12px;
                padding: 8px;
            }
            div[data-testid="stDataFrame"] {
                background-color: #0b1220 !important;
                border: 1px solid #1f2937;
                border-radius: 10px;
            }
            div[data-testid="stDataFrame"] [role="grid"] {
                background-color: #0b1220 !important;
                color: #e2e8f0 !important;
            }
            div[data-testid="stDataFrame"] [role="columnheader"] {
                background-color: #1e293b !important;
                color: #e2e8f0 !important;
            }
            div[data-testid="stDataFrame"] [role="gridcell"] {
                background-color: #0b1220 !important;
                color: #e2e8f0 !important;
            }
        }
        @media (prefers-color-scheme: light) {
            [data-testid="stAppViewContainer"] { background-color: #f8fafc; color: #0f172a; }
            [data-testid="stHeader"] { background: rgba(248, 250, 252, 0.85); }
            [data-testid="stSidebar"] { background-color: #eef2ff; }
            [data-testid="stSidebar"] * { color: #0f172a !important; }
            .stTabs [data-baseweb="tab"] { color: #334155 !important; }
            .stTabs [aria-selected="true"] { color: #0f172a !important; }
            div[data-testid="stMetric"] {
                background-color: #ffffff;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                padding: 8px;
            }
            div[data-testid="stDataFrame"] {
                background-color: #ffffff !important;
                border: 1px solid #dbe4f0;
                border-radius: 10px;
            }
            div[data-testid="stDataFrame"] [role="grid"] {
                background-color: #ffffff !important;
                color: #0f172a !important;
            }
            div[data-testid="stDataFrame"] [role="columnheader"] {
                background-color: #f1f5f9 !important;
                color: #0f172a !important;
            }
            div[data-testid="stDataFrame"] [role="gridcell"] {
                background-color: #ffffff !important;
                color: #0f172a !important;
            }
        }
        </style>
        """

    st.markdown(css, unsafe_allow_html=True)


def render_plotly_chart(fig: go.Figure) -> None:
    mode = st.session_state.get("theme_mode", "Theo hệ thống")
    is_dark = mode == "Tối"
    is_colorblind = mode == "Mù màu"

    if is_dark:
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="#0f172a",
            plot_bgcolor="#0f172a",
            font={"color": "#e2e8f0"},
        )
    elif is_colorblind:
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            font={"color": "#111827"},
            colorway=COLORBLIND_PALETTE,
            legend={"bgcolor": "rgba(255,255,255,0.9)", "bordercolor": "#94a3b8", "borderwidth": 1},
        )

        # Add redundant visual cues for colorblind accessibility.
        for idx, trace in enumerate(fig.data):
            if getattr(trace, "type", "") == "bar":
                trace.marker.pattern = {"shape": COLORBLIND_PATTERN_SEQUENCE[idx % len(COLORBLIND_PATTERN_SEQUENCE)]}

        fig.update_traces(
            marker_line_width=0.8,
            marker_line_color="#1f2937",
            selector={"type": "bar"},
        )
        fig.update_traces(
            marker={"line": {"width": 0.8, "color": "#1f2937"}},
            selector={"type": "scatter"},
        )
        fig.update_traces(colorscale=COLORBLIND_CONTINUOUS_SCALE, selector={"type": "heatmap"})
        fig.update_traces(colorscale=COLORBLIND_CONTINUOUS_SCALE, selector={"type": "histogram2d"})
        fig.update_layout(coloraxis={"colorscale": COLORBLIND_CONTINUOUS_SCALE})
    else:
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="#ffffff",
            plot_bgcolor="#ffffff",
            font={"color": "#0f172a"},
        )

    st.plotly_chart(fig, use_container_width=True, theme=None)


def apply_tab_filters(
    df_input: pd.DataFrame,
    key_prefix: str,
    *,
    enable_mall: bool = False,
    enable_video: bool = False,
    enable_category: bool = False,
    enable_price_bucket: bool = False,
    enable_discount: bool = False,
    enable_rating: bool = False,
    enable_review: bool = False,
) -> pd.DataFrame:
    filtered_tab = df_input.copy()
    with st.expander("Bộ lọc", expanded=False):
        if enable_mall and "is_mall" in filtered_tab.columns:
            mall_option = st.selectbox(
                "Loại gian hàng",
                ["Tất cả", "Mall", "Thường"],
                key=f"{key_prefix}_mall",
            )
            if mall_option == "Mall":
                filtered_tab = filtered_tab[filtered_tab["is_mall"] == True]
            elif mall_option == "Thường":
                filtered_tab = filtered_tab[filtered_tab["is_mall"] == False]

        if enable_video and "has_video" in filtered_tab.columns:
            video_option = st.selectbox(
                "Trạng thái video",
                ["Tất cả", "Có video", "Không video"],
                key=f"{key_prefix}_video",
            )
            if video_option == "Có video":
                filtered_tab = filtered_tab[filtered_tab["has_video"] == True]
            elif video_option == "Không video":
                filtered_tab = filtered_tab[filtered_tab["has_video"] == False]

        if enable_category:
            category_col = get_category_col(filtered_tab)
            if category_col:
                category_options = (
                    filtered_tab[category_col].dropna().astype(str).value_counts().head(30).index.tolist()
                )
                selected_categories = st.multiselect(
                    "Danh mục (tối đa 30 danh mục phổ biến)",
                    options=category_options,
                    default=[],
                    key=f"{key_prefix}_categories",
                )
                if selected_categories:
                    filtered_tab = filtered_tab[filtered_tab[category_col].astype(str).isin(selected_categories)]

        if enable_price_bucket and "price_bucket" in filtered_tab.columns:
            bucket_options = [
                b for b in ["<100k", "100k-500k", "500k-1M", "1M-5M", ">5M"] if b in filtered_tab["price_bucket"].astype(str).unique()
            ]
            selected_buckets = st.multiselect(
                "Phân khúc giá",
                options=bucket_options,
                default=bucket_options,
                key=f"{key_prefix}_price_bucket",
            )
            if selected_buckets:
                filtered_tab = filtered_tab[filtered_tab["price_bucket"].astype(str).isin(selected_buckets)]

        if enable_discount and "discount_percent" in filtered_tab.columns:
            valid_discount = filtered_tab["discount_percent"].dropna()
            if not valid_discount.empty:
                min_discount = float(valid_discount.min())
                max_discount = float(valid_discount.max())
                selected_discount = st.slider(
                    "Khoảng giảm giá (%)",
                    min_value=min_discount,
                    max_value=max_discount,
                    value=(min_discount, max_discount),
                    key=f"{key_prefix}_discount",
                )
                filtered_tab = filtered_tab[
                    (filtered_tab["discount_percent"] >= selected_discount[0])
                    & (filtered_tab["discount_percent"] <= selected_discount[1])
                ]

        if enable_rating and "rating" in filtered_tab.columns:
            valid_rating = filtered_tab["rating"].dropna()
            if not valid_rating.empty:
                min_rating = float(valid_rating.min())
                max_rating = float(valid_rating.max())
                selected_rating = st.slider(
                    "Khoảng rating",
                    min_value=min_rating,
                    max_value=max_rating,
                    value=(min_rating, max_rating),
                    key=f"{key_prefix}_rating",
                )
                filtered_tab = filtered_tab[
                    (filtered_tab["rating"] >= selected_rating[0])
                    & (filtered_tab["rating"] <= selected_rating[1])
                ]

        if enable_review and "review_count" in filtered_tab.columns:
            valid_review = filtered_tab["review_count"].dropna()
            if not valid_review.empty:
                min_review = int(valid_review.min())
                max_review = int(valid_review.max())
                selected_review = st.slider(
                    "Khoảng số lượng review",
                    min_value=min_review,
                    max_value=max_review,
                    value=(min_review, max_review),
                    key=f"{key_prefix}_review_count",
                )
                filtered_tab = filtered_tab[
                    (filtered_tab["review_count"] >= selected_review[0])
                    & (filtered_tab["review_count"] <= selected_review[1])
                ]

    st.caption(f"Số dòng sau lọc của trang: {len(filtered_tab):,}")
    return filtered_tab


def render_question_group(title: str, questions: list[str]) -> None:
    st.markdown(f"### {title}")
    st.caption("Câu hỏi phân tích:")
    for idx, question in enumerate(questions, start=1):
        st.markdown(f"{idx}. {question}")


def render_overview(filtered: pd.DataFrame, source_name: str) -> None:
    st.title("Dashboard tối ưu giá và tỷ lệ chuyển đổi")
    if filtered.empty:
        st.warning("Không có dữ liệu phù hợp bộ lọc hiện tại.")
        return

    st.subheader("Toàn cảnh hiệu suất sản phẩm")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Tổng sản phẩm", f"{len(filtered):,}")
    k2.metric(
        "Giá trung bình (VND)",
        f"{filtered['price_current'].mean():,.0f}" if "price_current" in filtered.columns else "N/A",
    )
    k3.metric("Rating trung bình", f"{filtered['rating'].mean():.2f}" if "rating" in filtered.columns else "N/A")
    k4.metric("Lượt bán trung bình", f"{filtered['sold_count'].mean():,.0f}" if "sold_count" in filtered.columns else "N/A")

    c1, c2 = st.columns(2)
    with c1:
        category_col = get_category_col(filtered)
        if category_col and "sold_count" in filtered.columns:
            top_cat = (
                filtered.dropna(subset=[category_col, "sold_count"])
                .groupby(category_col, as_index=False)["sold_count"]
                .mean()
                .sort_values("sold_count", ascending=False)
                .head(10)
            )
            fig_cat = px.bar(
                top_cat,
                x=category_col,
                y="sold_count",
                title="Top danh mục theo lượt bán trung bình",
                labels={category_col: "Danh mục", "sold_count": "Lượt bán TB"},
            )
            render_plotly_chart(fig_cat)
        else:
            st.info("Không đủ cột danh mục và lượt bán để vẽ biểu đồ top danh mục.")

    with c2:
        if "price_current" in filtered.columns:
            fig_hist = px.histogram(
                filtered.dropna(subset=["price_current"]),
                x="price_current",
                nbins=40,
                title="Phân bố giá sản phẩm",
                labels={"price_current": "Giá (VND)"},
            )
            render_plotly_chart(fig_hist)
        else:
            st.info("Không đủ cột giá để vẽ histogram.")


def render_thinh(filtered: pd.DataFrame) -> None:
    st.subheader("Chất lượng Trưng bày của Sản phẩm Mall")

    thinh1_required = [
        "is_mall",
        "image_count",
        "has_video",
        "five_star_with_image_count",
        "review_count",
        "sold_count",
    ]
    miss_1 = missing_columns(filtered, thinh1_required)
    if miss_1:
        st.warning(f"Thiếu cột cho trang Trưng bày và Giảm giá: {', '.join(miss_1)}")
    else:
        mall_df = filtered[filtered["is_mall"]].copy().dropna(subset=["sold_count"])
        if mall_df.empty:
            st.info("Không có dữ liệu Mall sau khi lọc.")
        else:
            mall_df["image_count"] = pd.to_numeric(mall_df["image_count"], errors="coerce").fillna(0)
            mall_df["has_video_num"] = normalize_bool(mall_df["has_video"]).astype(int)
            mall_df["review_count"] = pd.to_numeric(mall_df["review_count"], errors="coerce").fillna(0)
            mall_df["five_star_with_image_count"] = pd.to_numeric(
                mall_df["five_star_with_image_count"], errors="coerce"
            ).fillna(0)

            review_denominator = mall_df["review_count"].replace(0, pd.NA)
            mall_df["five_star_img_rate"] = (
                (mall_df["five_star_with_image_count"] / review_denominator).fillna(0).clip(lower=0, upper=1)
            )
            mall_df["display_quality_score"] = (
                (mall_df["image_count"].clip(upper=10) / 10) * 40
                + mall_df["has_video_num"] * 30
                + (mall_df["five_star_img_rate"].clip(upper=0.30) / 0.30) * 30
            )

            q75 = mall_df["sold_count"].quantile(0.75)
            top_quartile = mall_df[mall_df["sold_count"] >= q75].copy()
            std_image = int(max(6, round(top_quartile["image_count"].median()))) if not top_quartile.empty else 6
            std_video_rate = top_quartile["has_video_num"].mean() if not top_quartile.empty else 0
            std_five_star_rate = (
                max(0.10, round(top_quartile["five_star_img_rate"].median(), 2)) if not top_quartile.empty else 0.10
            )

            m1, m2, m3 = st.columns(3)
            m1.metric("Số sản phẩm Mall", f"{len(mall_df):,}")
            m2.metric("Lượt bán TB (Mall)", f"{mall_df['sold_count'].mean():,.0f}")
            m3.metric("Ngưỡng top 25%", f">= {q75:,.0f}")

            c1, c2 = st.columns(2)
            with c1:
                fig_score = px.scatter(
                    mall_df,
                    x="display_quality_score",
                    y="sold_count",
                    color=mall_df["has_video_num"].map({1: "Có video", 0: "Không video"}),
                    title="Điểm trưng bày và lượt bán",
                    labels={"display_quality_score": "Điểm trưng bày", "sold_count": "Lượt bán", "color": "Video"},
                )
                render_plotly_chart(fig_score)
            with c2:
                mall_df["image_bucket"] = pd.cut(
                    mall_df["image_count"],
                    bins=[-1, 3, 6, 9, 99],
                    labels=["0-3", "4-6", "7-9", "10+"],
                )
                sold_by_bucket = (
                    mall_df.groupby("image_bucket", observed=False, as_index=False)["sold_count"]
                    .mean()
                    .sort_values("image_bucket")
                )
                fig_bucket = px.bar(
                    sold_by_bucket,
                    x="image_bucket",
                    y="sold_count",
                    title="Lượt bán TB theo nhóm số ảnh",
                    labels={"image_bucket": "Nhóm số ảnh", "sold_count": "Lượt bán TB"},
                )
                render_plotly_chart(fig_bucket)

    st.markdown("---")
    st.subheader("Hiệu quả Giảm giá theo Dải Discount")

    thinh2_required = ["sold_count", "discount_percent"]
    miss_2 = missing_columns(filtered, thinh2_required)
    if miss_2:
        st.warning(f"Thiếu cột cho trang Trưng bày và Giảm giá: {', '.join(miss_2)}")
        return

    promo_df = filtered.dropna(subset=["sold_count", "discount_percent"]).copy()
    promo_df = promo_df[(promo_df["sold_count"] >= 0) & (promo_df["discount_percent"] >= 0)]
    promo_df["discount_percent"] = promo_df["discount_percent"].clip(upper=100)
    if promo_df.empty:
        st.info("Không có dữ liệu hợp lệ cho phân tích giảm giá.")
        return

    promo_df["discount_band"] = pd.cut(
        promo_df["discount_percent"],
        bins=[-0.01, 0, 5, 10, 20, 30, 50, 100],
        labels=["0%", "1-5%", "6-10%", "11-20%", "21-30%", "31-50%", ">50%"],
    )
    band_stats = (
        promo_df.groupby("discount_band", observed=False)["sold_count"]
        .agg(avg_sold="mean", median_sold="median", sample_size="count")
        .reset_index()
    )

    min_sample = max(30, int(len(promo_df) * 0.03))
    candidates = band_stats[band_stats["sample_size"] >= min_sample].copy()
    if candidates.empty:
        candidates = band_stats.copy()

    p1, p2, p3 = st.columns(3)
    p1.metric("Mẫu phân tích", f"{len(promo_df):,}")
    p2.metric("Discount TB (%)", f"{promo_df['discount_percent'].mean():.1f}")
    p3.metric("Lượt bán TB", f"{promo_df['sold_count'].mean():,.0f}")

    g1, g2 = st.columns(2)
    with g1:
        fig_band = px.bar(
            band_stats.sort_values("avg_sold", ascending=False),
            x="discount_band",
            y="avg_sold",
            color="sample_size",
            title="Lượt bán TB theo nhóm discount",
            labels={"discount_band": "Nhóm discount", "avg_sold": "Lượt bán TB"},
        )
        render_plotly_chart(fig_band)
    with g2:
        fig_box = px.box(
            promo_df,
            x="discount_band",
            y="sold_count",
            title="Phân bố lượt bán theo nhóm discount",
            labels={"discount_band": "Nhóm discount", "sold_count": "Lượt bán"},
        )
        render_plotly_chart(fig_box)

    st.dataframe(band_stats.sort_values("avg_sold", ascending=False), use_container_width=True)


def render_tuan(filtered: pd.DataFrame) -> None:
    st.subheader("Độ nhạy Giá và Doanh thu")

    if filtered.empty:
        st.info("Không có dữ liệu để hiển thị trên trang này.")
        return

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(
        "Giá trung vị (VND)",
        f"{filtered['price_current'].median():,.0f}" if "price_current" in filtered.columns else "N/A",
    )
    k2.metric(
        "Lượt bán trung vị",
        f"{filtered['sold_count'].median():,.0f}" if "sold_count" in filtered.columns else "N/A",
    )
    k3.metric(
        "Tổng doanh thu ước tính",
        f"{filtered['revenue_est'].sum():,.0f}" if "revenue_est" in filtered.columns else "N/A",
    )
    if "rating" in filtered.columns:
        rating_series = filtered["rating"].dropna()
        high_rating_ratio = ((rating_series >= 4.5).mean() * 100) if not rating_series.empty else np.nan
        k4.metric("Tỷ lệ rating >= 4.5", f"{high_rating_ratio:.1f}%" if pd.notna(high_rating_ratio) else "N/A")
    else:
        k4.metric("Tỷ lệ rating >= 4.5", "N/A")

    req = ["price_current", "sold_count"]
    miss = missing_columns(filtered, req)
    if miss:
        st.warning(f"Thiếu cột cho trang Giá, Doanh thu và Đánh giá: {', '.join(miss)}")
    else:
        price_df = filtered.dropna(subset=req).copy()
        price_df = price_df[(price_df["price_current"] > 0) & (price_df["sold_count"] > 0)]

        c1, c2 = st.columns(2)
        with c1:
            fig_scatter = px.scatter(
                price_df,
                x="price_current",
                y="sold_count",
                color="price_bucket" if "price_bucket" in price_df.columns else None,
                log_x=True,
                log_y=True,
                title="Price vs Sold Count (log-log)",
                labels={"price_current": "Giá (VND)", "sold_count": "Lượt bán"},
            )
            render_plotly_chart(fig_scatter)

        with c2:
            if len(price_df) >= 20:
                tmp = price_df.copy()
                tmp["price_bin"] = pd.qcut(tmp["price_current"], q=10, duplicates="drop")
                bin_df = (
                    tmp.groupby("price_bin", observed=False)
                    .agg(avg_sold=("sold_count", "mean"), revenue=("revenue_est", "sum"))
                    .reset_index()
                )
                bin_df["price_bin"] = bin_df["price_bin"].astype(str)

                fig_mix = make_subplots(specs=[[{"secondary_y": True}]])
                fig_mix.add_trace(
                    go.Bar(x=bin_df["price_bin"], y=bin_df["revenue"], name="Tong doanh thu"),
                    secondary_y=False,
                )
                fig_mix.add_trace(
                    go.Scatter(x=bin_df["price_bin"], y=bin_df["avg_sold"], mode="lines+markers", name="Luot ban TB"),
                    secondary_y=True,
                )
                fig_mix.update_layout(title="Doanh thu và lượt bán theo khoảng giá")
                fig_mix.update_xaxes(title_text="Price bin")
                fig_mix.update_yaxes(title_text="Tổng doanh thu", secondary_y=False)
                fig_mix.update_yaxes(title_text="Lượt bán TB", secondary_y=True)
                render_plotly_chart(fig_mix)
            else:
                st.info("Không đủ dữ liệu để tạo qcut 10 nhóm giá.")

    st.markdown("---")
    st.subheader("Tác động của Rating và Review")

    req2 = ["price_current", "rating", "review_count", "sold_count"]
    miss2 = missing_columns(filtered, req2)
    if miss2:
        st.warning(f"Thiếu cột cho trang Giá, Doanh thu và Đánh giá: {', '.join(miss2)}")
        return

    d = filtered.dropna(subset=req2).copy()
    corr_cols = ["price_current", "rating", "review_count", "sold_count"]
    corr_df = d[corr_cols].corr(method="spearman")
    fig_corr = px.imshow(
        corr_df,
        text_auto=".2f",
        title="Ma trận tương quan Spearman",
        color_continuous_scale="RdBu_r",
        aspect="auto",
    )
    render_plotly_chart(fig_corr)

    c3, c4 = st.columns(2)
    with c3:
        d2 = d[(d["rating"] >= 0) & (d["rating"] <= 5)].copy()
        d2["rating_group"] = d2["rating"].round(1).astype(str)
        fig_box = px.box(
            d2,
            x="rating_group",
            y="sold_count",
            title="Phân phối lượt bán theo rating group",
            labels={"rating_group": "Rating group", "sold_count": "Lượt bán"},
        )
        fig_box.update_yaxes(type="log")
        render_plotly_chart(fig_box)

    with c4:
        bins_review = [0, 10, 50, 200, 1000, np.inf]
        labels_review = ["0-10", "10-50", "50-200", "200-1000", "1000+"]
        bins_rating = [0, 3.5, 4.0, 4.5, 5.0]
        labels_rating = ["<3.5", "3.5-4.0", "4.0-4.5", "4.5-5.0"]
        d3 = d.copy()
        d3["review_segment"] = pd.cut(d3["review_count"], bins=bins_review, labels=labels_review)
        d3["rating_segment"] = pd.cut(d3["rating"], bins=bins_rating, labels=labels_rating)
        pivot = d3.pivot_table(
            values="sold_count",
            index="rating_segment",
            columns="review_segment",
            aggfunc="mean",
            observed=False,
        )
        if not pivot.empty:
            fig_heat = px.imshow(
                pivot,
                text_auto=".1f",
                aspect="auto",
                title="Ma trận hiệu quả Rating x Review",
                color_continuous_scale="YlGnBu",
            )
            render_plotly_chart(fig_heat)
        else:
            st.info("Không đủ dữ liệu để tạo ma trận Rating x Review.")


def render_y(filtered: pd.DataFrame) -> None:
    st.subheader("Danh mục và Phân khúc Giá")

    if filtered.empty:
        st.info("Không có dữ liệu để hiển thị trên trang này.")
        return

    category_col = get_category_col(filtered)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric(
        "Số danh mục",
        f"{filtered[category_col].nunique():,}" if category_col is not None else "N/A",
    )
    k2.metric(
        "Số phân khúc giá",
        f"{filtered['price_bucket'].astype(str).nunique():,}" if "price_bucket" in filtered.columns else "N/A",
    )
    if "discount_percent" in filtered.columns:
        discount_ratio = (filtered["discount_percent"].fillna(0) > 0).mean() * 100
        k3.metric("Tỷ lệ sản phẩm có giảm giá", f"{discount_ratio:.1f}%")
    else:
        k3.metric("Tỷ lệ sản phẩm có giảm giá", "N/A")

    lift_text = "N/A"
    if {"discount_percent", "sold_count"}.issubset(filtered.columns):
        lift_df = filtered.dropna(subset=["discount_percent", "sold_count"]).copy()
        sold_discount = lift_df[lift_df["discount_percent"] > 0]["sold_count"].mean()
        sold_non_discount = lift_df[lift_df["discount_percent"] == 0]["sold_count"].mean()
        if pd.notna(sold_discount) and pd.notna(sold_non_discount) and sold_non_discount != 0:
            lift_text = f"{((sold_discount - sold_non_discount) / sold_non_discount) * 100:+.1f}%"
    k4.metric("Lift bán hàng do giảm giá", lift_text)

    req = ["sold_count", "price_bucket"]
    miss = missing_columns(filtered, req)
    if category_col is None:
        miss.append("category_name/category_id")
    if miss:
        st.warning(f"Thiếu cột cho trang Danh mục và Khuyến mãi: {', '.join(miss)}")
    else:
        ydf = filtered.dropna(subset=[category_col, "price_bucket", "sold_count"]).copy()
        top_cats = ydf[category_col].astype(str).value_counts().head(12).index.tolist()
        ydf = ydf[ydf[category_col].astype(str).isin(top_cats)].copy()

        agg = (
            ydf.groupby([category_col, "price_bucket"], observed=False)["sold_count"]
            .mean()
            .reset_index()
        )
        c1, c2 = st.columns(2)
        with c1:
            fig_bar = px.bar(
                agg,
                x=category_col,
                y="sold_count",
                color="price_bucket",
                barmode="group",
                title="Lượt bán TB theo danh mục và phân khúc giá",
                labels={category_col: "Danh mục", "sold_count": "Lượt bán TB", "price_bucket": "Phân khúc giá"},
            )
            fig_bar.update_layout(xaxis_tickangle=-40)
            render_plotly_chart(fig_bar)
        with c2:
            heat = agg.pivot(index=category_col, columns="price_bucket", values="sold_count")
            fig_heat = px.imshow(
                heat,
                text_auto=".0f",
                aspect="auto",
                title="Heatmap lượt bán TB theo danh mục và phân khúc giá",
                color_continuous_scale="Blues",
            )
            render_plotly_chart(fig_heat)

        stats = (
            ydf.groupby([category_col, "price_bucket"], observed=False)
            .agg(so_luong_sp=("product_id", "count"), luot_ban_tb=("sold_count", "mean"))
            .reset_index()
            .sort_values("luot_ban_tb", ascending=False)
        )
        top3 = stats.groupby(category_col, observed=False).head(3)
        st.dataframe(top3, use_container_width=True)

    st.markdown("---")
    st.subheader("Tác động Giảm giá theo Danh mục")

    if "discount_percent" not in filtered.columns or "sold_count" not in filtered.columns:
        st.warning("Thiếu cột discount_percent hoặc sold_count cho trang Danh mục và Khuyến mãi.")
        return

    y2 = filtered.dropna(subset=["sold_count", "discount_percent"]).copy()
    y2["Trang_Thai_Giam_Gia"] = np.where(y2["discount_percent"] > 0, "Có Giảm Giá", "Không Giảm Giá")

    d1, d2 = st.columns(2)
    with d1:
        discount_mean = y2.groupby("Trang_Thai_Giam_Gia", as_index=False)["sold_count"].mean()
        fig_bar = px.bar(
            discount_mean,
            x="Trang_Thai_Giam_Gia",
            y="sold_count",
            color="Trang_Thai_Giam_Gia",
            title="Lượt bán trung bình: Có và Không giảm giá",
            labels={"Trang_Thai_Giam_Gia": "Trạng thái", "sold_count": "Lượt bán TB"},
            text_auto=".1f",
        )
        render_plotly_chart(fig_bar)
    with d2:
        discount_sum = y2.groupby("Trang_Thai_Giam_Gia", as_index=False)["sold_count"].sum()
        fig_pie = px.pie(
            discount_sum,
            names="Trang_Thai_Giam_Gia",
            values="sold_count",
            hole=0.4,
            title="Tỷ trọng tổng lượt bán theo trạng thái giảm giá",
        )
        render_plotly_chart(fig_pie)

    category_col = get_category_col(y2)
    if category_col:
        top_cats = y2[category_col].astype(str).value_counts().head(10).index.tolist()
        y2c = y2[y2[category_col].astype(str).isin(top_cats)]
        cat_discount = (
            y2c.groupby([category_col, "Trang_Thai_Giam_Gia"], observed=False)["sold_count"]
            .mean()
            .reset_index()
        )
        fig_cat = px.bar(
            cat_discount,
            x=category_col,
            y="sold_count",
            color="Trang_Thai_Giam_Gia",
            barmode="group",
            title="Lượt bán TB Có/Không giảm giá theo từng danh mục",
            labels={category_col: "Danh mục", "sold_count": "Lượt bán TB", "Trang_Thai_Giam_Gia": "Trạng thái"},
            text_auto=".0f",
        )
        fig_cat.update_layout(xaxis_tickangle=-40)
        render_plotly_chart(fig_cat)


def render_the_anh(filtered: pd.DataFrame) -> None:
    st.subheader("Phân khúc Thị trường theo Giá và Rating")

    if filtered.empty:
        st.info("Không có dữ liệu để hiển thị trên trang này.")
        return

    k1, k2, k3, k4 = st.columns(4)
    if "price_bucket" in filtered.columns:
        bucket_series = filtered["price_bucket"].astype(str)
        k1.metric("Số phân khúc giá", f"{bucket_series.nunique():,}")
        top_bucket_count = bucket_series.value_counts()
        k2.metric("Phân khúc phổ biến nhất", top_bucket_count.index[0] if not top_bucket_count.empty else "N/A")
    else:
        k1.metric("Số phân khúc giá", "N/A")
        k2.metric("Phân khúc phổ biến nhất", "N/A")

    if {"price_bucket", "revenue_est"}.issubset(filtered.columns):
        rev_by_bucket = filtered.dropna(subset=["price_bucket", "revenue_est"]).groupby("price_bucket")["revenue_est"].sum()
        k3.metric("Phân khúc doanh thu cao nhất", str(rev_by_bucket.idxmax()) if not rev_by_bucket.empty else "N/A")
    else:
        k3.metric("Phân khúc doanh thu cao nhất", "N/A")

    k4.metric(
        "Rating trung bình",
        f"{filtered['rating'].mean():.2f}" if "rating" in filtered.columns else "N/A",
    )

    req = ["price_bucket", "price_current", "sold_count", "rating"]
    miss = missing_columns(filtered, req)
    if miss:
        st.warning(f"Thiếu cột cho trang Phân khúc theo Giá: {', '.join(miss)}")
        return

    d = filtered.dropna(subset=req).copy()
    d = d[(d["price_current"] > 0) & (d["sold_count"] >= 0)]

    c1, c2 = st.columns(2)
    with c1:
        count_df = d.groupby("price_bucket", observed=False, as_index=False)["product_id"].count()
        count_df.columns = ["price_bucket", "so_luong_sp"]
        fig_count = px.bar(
            count_df,
            x="price_bucket",
            y="so_luong_sp",
            title="Phân bố số lượng sản phẩm theo phân khúc giá",
            labels={"price_bucket": "Phân khúc giá", "so_luong_sp": "Số lượng"},
        )
        render_plotly_chart(fig_count)

    with c2:
        fig_box = px.box(
            d,
            x="price_bucket",
            y="sold_count",
            title="Phân phối lượt bán theo phân khúc giá",
            labels={"price_bucket": "Phân khúc giá", "sold_count": "Lượt bán"},
        )
        fig_box.update_yaxes(type="log")
        render_plotly_chart(fig_box)

    seg = (
        d.groupby("price_bucket", observed=False)
        .agg(revenue=("revenue_est", "sum"), sold_mean=("sold_count", "mean"))
        .reset_index()
    )
    fig_mix = make_subplots(specs=[[{"secondary_y": True}]])
    fig_mix.add_trace(go.Bar(x=seg["price_bucket"], y=seg["revenue"], name="Revenue"), secondary_y=False)
    fig_mix.add_trace(
        go.Scatter(x=seg["price_bucket"], y=seg["sold_mean"], mode="lines+markers", name="Sold mean"),
        secondary_y=True,
    )
    fig_mix.update_layout(title="Doanh thu và lượt bán theo phân khúc")
    fig_mix.update_xaxes(title_text="Price bucket")
    fig_mix.update_yaxes(title_text="Revenue", secondary_y=False)
    fig_mix.update_yaxes(title_text="Sold mean", secondary_y=True)
    render_plotly_chart(fig_mix)

    e1, e2 = st.columns(2)
    with e1:
        d_heat = d.copy()
        d_heat["rating_group"] = d_heat["rating"].round(1)
        pivot = d_heat.pivot_table(
            values="sold_count",
            index="price_bucket",
            columns="rating_group",
            aggfunc="mean",
            observed=False,
        )
        fig_heat = px.imshow(
            pivot,
            aspect="auto",
            color_continuous_scale="YlGnBu",
            title="Sold theo price segment và rating",
        )
        render_plotly_chart(fig_heat)
    with e2:
        fig_scatter = px.scatter(
            d,
            x="price_current",
            y="sold_count",
            color="price_bucket",
            log_x=True,
            log_y=True,
            title="Giá vs Sold theo phân khúc",
            labels={"price_current": "Giá", "sold_count": "Sold"},
        )
        render_plotly_chart(fig_scatter)


def render_duong(filtered: pd.DataFrame) -> None:
    st.subheader("Hiệu ứng Giá Tâm lý")

    if filtered.empty:
        st.info("Không có dữ liệu để hiển thị trên trang này.")
        return

    psy_avg_text = "N/A"
    regular_avg_text = "N/A"
    delta_text = "N/A"
    if {"price_current", "sold_count"}.issubset(filtered.columns):
        d_kpi = filtered.dropna(subset=["price_current", "sold_count"]).copy()
        d_kpi = d_kpi[d_kpi["price_current"] > 0]
        if not d_kpi.empty:
            d_kpi["is_psy_price"] = d_kpi["price_current"].astype(int).astype(str).str[-3:].str.contains("9")
            psy_mean = d_kpi[d_kpi["is_psy_price"]]["sold_count"].mean()
            regular_mean = d_kpi[~d_kpi["is_psy_price"]]["sold_count"].mean()
            psy_avg_text = f"{psy_mean:,.1f}" if pd.notna(psy_mean) else "N/A"
            regular_avg_text = f"{regular_mean:,.1f}" if pd.notna(regular_mean) else "N/A"
            if pd.notna(psy_mean) and pd.notna(regular_mean) and regular_mean != 0:
                delta_text = f"{((psy_mean - regular_mean) / regular_mean) * 100:+.1f}%"

    trust_text = "N/A"
    if {"rating", "review_count"}.issubset(filtered.columns):
        trust_series = (filtered["rating"] * 0.7) + (np.log1p(filtered["review_count"]) * 0.3)
        trust_text = f"{trust_series.mean():.2f}" if not trust_series.dropna().empty else "N/A"

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Lượt bán TB giá tâm lý", psy_avg_text)
    k2.metric("Lượt bán TB giá thường", regular_avg_text)
    k3.metric("Chênh lệch hiệu quả", delta_text)
    k4.metric("Trust index trung bình", trust_text)

    req = ["price_current", "sold_count"]
    miss = missing_columns(filtered, req)
    if miss:
        st.warning(f"Thiếu cột cho trang Giá tâm lý và Uy tín shop: {', '.join(miss)}")
    else:
        d = filtered.dropna(subset=req).copy()
        d = d[d["price_current"] > 0]

        def is_psychological_price(price_val: float) -> bool:
            if pd.isna(price_val):
                return False
            tail = str(int(price_val))[-3:]
            return "9" in tail

        d["is_psy_price"] = d["price_current"].apply(is_psychological_price)
        psy_viz = d.groupby("is_psy_price", as_index=False)["sold_count"].mean()
        psy_viz["label"] = psy_viz["is_psy_price"].map({True: "Giá dưới 9", False: "Giá thông thường"})

        fig = px.bar(
            psy_viz.sort_values("sold_count", ascending=False),
            x="label",
            y="sold_count",
            color="label",
            text_auto=".1f",
            title="So sánh lượt bán trung bình: giá dưới 9 vs giá tròn",
            labels={"label": "Nhóm giá", "sold_count": "Lượt bán TB"},
        )
        fig.update_layout(showlegend=False)
        render_plotly_chart(fig)

    st.markdown("---")
    st.subheader("Trust Index và Uy tín Shop")

    req2 = ["rating", "review_count", "sold_count"]
    miss2 = missing_columns(filtered, req2)
    if miss2:
        st.warning(f"Thiếu cột cho trang Giá tâm lý và Uy tín shop: {', '.join(miss2)}")
        return

    d2 = filtered.dropna(subset=req2).copy()
    d2["trust_index_score"] = (d2["rating"] * 0.7) + (np.log1p(d2["review_count"]) * 0.3)
    d2 = d2[d2["sold_count"] > 0]
    if d2.empty:
        st.info("Không có dữ liệu sold_count > 0 cho scatter trust index.")
        return

    color_col = "is_mall" if "is_mall" in d2.columns else None
    fig2 = px.scatter(
        d2,
        x="trust_index_score",
        y="sold_count",
        size="revenue_est" if "revenue_est" in d2.columns else None,
        color=color_col,
        hover_name="product_name" if "product_name" in d2.columns else None,
        title="Tương quan trust index và lượt bán",
        labels={"trust_index_score": "Trust Index", "sold_count": "Lượt bán"},
    )
    render_plotly_chart(fig2)


df, source_name = load_data()

if not df.empty:
    numeric_cols = [
        "price_current",
        "price_original",
        "sold_count",
        "rating",
        "review_count",
        "image_count",
        "review_with_image_count",
        "five_star_with_image_count",
        "discount_percent",
        "promotion_label_count",
        "shipping_fee",
        "shop_rating",
        "follower_count",
        "response_rate",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    bool_cols = [
        "has_video",
        "is_mall",
        "is_freeship",
        "price_ends_with_9",
        "has_freeship_xtra_label",
        "has_coinback_label",
        "has_voucher_label",
    ]
    for col in bool_cols:
        if col in df.columns:
            df[col] = normalize_bool(df[col])

    if "discount_percent" in df.columns:
        df["discount_percent"] = df["discount_percent"].fillna(0).clip(lower=0, upper=100)
    if "price_original" in df.columns and "price_current" in df.columns:
        df["price_original"] = df["price_original"].fillna(df["price_current"])
    if "crawled_at" in df.columns:
        df["crawl_dt"] = pd.to_datetime(df["crawled_at"], errors="coerce")

    ensure_price_bucket(df)
    if "discount_percent" in df.columns:
        df["is_discounted"] = df["discount_percent"] > 0
    if {"price_current", "sold_count"}.issubset(df.columns):
        df["revenue_est"] = df["price_current"] * df["sold_count"]


st.sidebar.header("Giao diện")
theme_mode = st.sidebar.radio(
    "Chế độ hiển thị",
    options=["Theo hệ thống", "Sáng", "Tối", "Mù màu"],
    index=0,
)
apply_theme(theme_mode)

filtered = df.copy()


tabs = st.tabs(
    [
        "Tổng quan",
        "Trưng bày & Giảm giá",
        "Giá, Doanh thu & Đánh giá",
        "Danh mục & Khuyến mãi",
        "Phân khúc theo Giá",
        "Giá tâm lý & Uy tín shop",
    ]
)

with tabs[0]:
    filtered_overview = apply_tab_filters(
        filtered,
        "tab_overview",
        enable_category=True,
        enable_price_bucket=True,
    )
    render_overview(filtered_overview, source_name)

with tabs[1]:
    filtered_display = apply_tab_filters(
        filtered,
        "tab_display",
        enable_mall=True,
        enable_video=True,
        enable_discount=True,
    )
    render_thinh(filtered_display)

with tabs[2]:
    filtered_price_rating = apply_tab_filters(
        filtered,
        "tab_price_rating",
        enable_price_bucket=True,
        enable_rating=True,
        enable_review=True,
    )
    render_tuan(filtered_price_rating)

with tabs[3]:
    filtered_category = apply_tab_filters(
        filtered,
        "tab_category",
        enable_category=True,
        enable_price_bucket=True,
        enable_discount=True,
    )
    render_y(filtered_category)

with tabs[4]:
    filtered_segment = apply_tab_filters(
        filtered,
        "tab_segment",
        enable_price_bucket=True,
        enable_rating=True,
    )
    render_the_anh(filtered_segment)

with tabs[5]:
    filtered_psy = apply_tab_filters(
        filtered,
        "tab_psy",
        enable_mall=True,
        enable_rating=True,
        enable_review=True,
    )
    render_duong(filtered_psy)
