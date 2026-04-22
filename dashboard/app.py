"""Streamlit dashboard tong hop toan bo EDA cua nhom Lab 01."""

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


st.set_page_config(page_title="TMDT Analytics Dashboard", page_icon="cart", layout="wide")

PALETTE = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#17becf"]
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


def render_overview(filtered: pd.DataFrame, source_name: str) -> None:
    st.title("Dashboard tối ưu giá và tỷ lệ chuyển đổi")
    if filtered.empty:
        st.warning("Không có dữ liệu phù hợp bộ lọc hiện tại.")
        return

    st.caption(f"Nguồn dữ liệu: {source_name}")
    st.caption("KPI tổng quan đặt trước, phân tích chi tiết đặt sau theo nguyên tắc visual hierarchy.")

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
            st.plotly_chart(fig_cat, use_container_width=True)
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
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("Không đủ cột giá để vẽ histogram.")


def render_thinh(filtered: pd.DataFrame) -> None:
    st.subheader("THINH: Chất lượng trưng bày chi tiết và doanh số Mall")
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
        st.warning(f"Thiếu cột cho THINH: {', '.join(miss_1)}")
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

            st.success(
                "Đề xuất 2 tiêu chuẩn trưng bày: "
                f"(1) tối thiểu {std_image} ảnh, ưu tiên video (tỷ lệ video top nhóm {std_video_rate:.0%}); "
                f"(2) tỷ lệ review 5 sao kèm ảnh >= {std_five_star_rate:.0%}."
            )

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
                st.plotly_chart(fig_score, use_container_width=True)
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
                st.plotly_chart(fig_bucket, use_container_width=True)

    st.markdown("---")
    st.subheader("THINH: Hiệu quả giảm giá và ngưỡng discount tối ưu")
    thinh2_required = ["sold_count", "discount_percent"]
    miss_2 = missing_columns(filtered, thinh2_required)
    if miss_2:
        st.warning(f"Thiếu cột cho THINH: {', '.join(miss_2)}")
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
    top2 = candidates.sort_values("avg_sold", ascending=False).head(2)

    p1, p2, p3 = st.columns(3)
    p1.metric("Mẫu phân tích", f"{len(promo_df):,}")
    p2.metric("Discount TB (%)", f"{promo_df['discount_percent'].mean():.1f}")
    p3.metric("Lượt bán TB", f"{promo_df['sold_count'].mean():,.0f}")

    if not top2.empty:
        rec_text = ", ".join(top2["discount_band"].astype(str).tolist())
        st.success(f"Nhóm discount ưu tiên để thử nghiệm Q3-Q4/2026: {rec_text}.")

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
        st.plotly_chart(fig_band, use_container_width=True)
    with g2:
        fig_box = px.box(
            promo_df,
            x="discount_band",
            y="sold_count",
            title="Phân bố lượt bán theo nhóm discount",
            labels={"discount_band": "Nhóm discount", "sold_count": "Lượt bán"},
        )
        st.plotly_chart(fig_box, use_container_width=True)

    st.dataframe(band_stats.sort_values("avg_sold", ascending=False), use_container_width=True)


def render_tuan(filtered: pd.DataFrame) -> None:
    st.subheader("TUAN: Mối quan hệ giá bán và lượt bán")
    req = ["price_current", "sold_count"]
    miss = missing_columns(filtered, req)
    if miss:
        st.warning(f"Thiếu cột cho TUAN: {', '.join(miss)}")
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
            st.plotly_chart(fig_scatter, use_container_width=True)

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
                st.plotly_chart(fig_mix, use_container_width=True)
            else:
                st.info("Không đủ dữ liệu để tạo qcut 10 nhóm giá.")

    st.markdown("---")
    st.subheader("TUAN: Tác động rating và review")
    req2 = ["price_current", "rating", "review_count", "sold_count"]
    miss2 = missing_columns(filtered, req2)
    if miss2:
        st.warning(f"Thiếu cột cho TUAN: {', '.join(miss2)}")
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
    st.plotly_chart(fig_corr, use_container_width=True)

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
        st.plotly_chart(fig_box, use_container_width=True)

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
            st.plotly_chart(fig_heat, use_container_width=True)
        else:
            st.info("Không đủ dữ liệu để tạo ma trận Rating x Review.")


def render_y(filtered: pd.DataFrame) -> None:
    st.subheader("Y: Danh mục và phân khúc giá đến lượt bán")
    category_col = get_category_col(filtered)
    req = ["sold_count", "price_bucket"]
    miss = missing_columns(filtered, req)
    if category_col is None:
        miss.append("category_name/category_id")
    if miss:
        st.warning(f"Thiếu cột cho Y: {', '.join(miss)}")
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
            st.plotly_chart(fig_bar, use_container_width=True)
        with c2:
            heat = agg.pivot(index=category_col, columns="price_bucket", values="sold_count")
            fig_heat = px.imshow(
                heat,
                text_auto=".0f",
                aspect="auto",
                title="Heatmap lượt bán TB theo danh mục x phân khúc giá",
                color_continuous_scale="Blues",
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        stats = (
            ydf.groupby([category_col, "price_bucket"], observed=False)
            .agg(so_luong_sp=("product_id", "count"), luot_ban_tb=("sold_count", "mean"))
            .reset_index()
            .sort_values("luot_ban_tb", ascending=False)
        )
        top3 = stats.groupby(category_col, observed=False).head(3)
        st.dataframe(top3, use_container_width=True)

    st.markdown("---")
    st.subheader("Y: Ảnh hưởng giảm giá đến lượt bán")
    if "discount_percent" not in filtered.columns or "sold_count" not in filtered.columns:
        st.warning("Thiếu cột discount_percent hoặc sold_count cho Y.")
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
        st.plotly_chart(fig_bar, use_container_width=True)
    with d2:
        discount_sum = y2.groupby("Trang_Thai_Giam_Gia", as_index=False)["sold_count"].sum()
        fig_pie = px.pie(
            discount_sum,
            names="Trang_Thai_Giam_Gia",
            values="sold_count",
            hole=0.4,
            title="Tỷ trọng tổng lượt bán theo trạng thái giảm giá",
        )
        st.plotly_chart(fig_pie, use_container_width=True)

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
        st.plotly_chart(fig_cat, use_container_width=True)


def render_the_anh(filtered: pd.DataFrame) -> None:
    st.subheader("THE ANH: Tổng hợp biểu đồ trong EDA")
    req = ["price_bucket", "price_current", "sold_count", "rating"]
    miss = missing_columns(filtered, req)
    if miss:
        st.warning(f"Thiếu cột cho THE ANH: {', '.join(miss)}")
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
        st.plotly_chart(fig_count, use_container_width=True)

    with c2:
        fig_box = px.box(
            d,
            x="price_bucket",
            y="sold_count",
            title="Phân phối lượt bán theo phân khúc giá",
            labels={"price_bucket": "Phân khúc giá", "sold_count": "Lượt bán"},
        )
        fig_box.update_yaxes(type="log")
        st.plotly_chart(fig_box, use_container_width=True)

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
    st.plotly_chart(fig_mix, use_container_width=True)

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
        st.plotly_chart(fig_heat, use_container_width=True)
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
        st.plotly_chart(fig_scatter, use_container_width=True)


def render_duong(filtered: pd.DataFrame) -> None:
    st.subheader("DUONG: Hiệu ứng giá tâm lý")
    req = ["price_current", "sold_count"]
    miss = missing_columns(filtered, req)
    if miss:
        st.warning(f"Thiếu cột cho DUONG: {', '.join(miss)}")
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
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("DUONG: Trust index và hiệu quả bán hàng")
    req2 = ["rating", "review_count", "sold_count"]
    miss2 = missing_columns(filtered, req2)
    if miss2:
        st.warning(f"Thiếu cột cho DUONG: {', '.join(miss2)}")
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
    st.plotly_chart(fig2, use_container_width=True)


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


st.sidebar.header("Bộ lọc")
filtered = df.copy()

if not df.empty:
    if "platform_id" in df.columns:
        options = ["Tat ca"] + sorted(df["platform_id"].dropna().astype(str).unique().tolist())
        selected_platform = st.sidebar.selectbox("Sàn TMDT", options)
        if selected_platform != "Tat ca":
            filtered = filtered[filtered["platform_id"].astype(str) == selected_platform]

    if "crawled_by" in df.columns:
        crawlers = ["Tat ca"] + sorted(df["crawled_by"].dropna().astype(str).unique().tolist())
        selected_owner = st.sidebar.selectbox("Nguồn crawl", crawlers)
        if selected_owner != "Tat ca":
            filtered = filtered[filtered["crawled_by"].astype(str) == selected_owner]

    if "price_current" in filtered.columns:
        valid_prices = filtered["price_current"].dropna()
        if not valid_prices.empty:
            min_price = float(valid_prices.min())
            max_price = float(valid_prices.max())
            selected_range = st.sidebar.slider(
                "Khoảng giá (VND)",
                min_value=min_price,
                max_value=max_price,
                value=(min_price, max_price),
            )
            filtered = filtered[
                (filtered["price_current"] >= selected_range[0])
                & (filtered["price_current"] <= selected_range[1])
            ]


tabs = st.tabs(
    [
        "Tong quan",
        "THINH",
        "TUAN",
        "Y",
        "THE ANH",
        "DUONG",
        "Tong ket",
    ]
)

with tabs[0]:
    render_overview(filtered, source_name)

with tabs[1]:
    render_thinh(filtered)

with tabs[2]:
    render_tuan(filtered)

with tabs[3]:
    render_y(filtered)

with tabs[4]:
    render_the_anh(filtered)

with tabs[5]:
    render_duong(filtered)

with tabs[6]:
    st.title("Tổng kết và nguyên tắc trực quan hoá")
    if not filtered.empty and "crawled_by" in filtered.columns:
        contrib = filtered["crawled_by"].astype(str).value_counts().reset_index()
        contrib.columns = ["Thành viên", "Số dòng"]
        fig_contrib = px.pie(
            contrib,
            names="Thành viên",
            values="Số dòng",
            title="Tỷ lệ đóng góp dữ liệu theo thành viên",
            hole=0.35,
        )
        st.plotly_chart(fig_contrib, use_container_width=True)

    st.subheader("Checklist visual đã áp dụng")
    st.markdown("1. KPI tổng quan đặt trước, chart chi tiết đặt sau.")
    st.markdown("2. Đơn vị đo lường và tiêu đề được viết rõ ràng cho từng biểu đồ.")
    st.markdown("3. Màu sắc nhất quán, không dùng 3D chart, ưu tiên bar-scatter-box-heatmap.")
    st.markdown("4. So sánh theo nhóm có sắp xếp và giảm noise bằng lọc/phân khúc.")
