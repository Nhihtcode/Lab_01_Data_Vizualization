"""Streamlit dashboard cho do an Lab 01."""

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(page_title="TMDT Analytics Dashboard", page_icon="🛒", layout="wide")

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


df, source_name = load_data()

if not df.empty:
    numeric_cols = [
        "price_current",
        "sold_count",
        "rating",
        "review_count",
        "image_count",
        "review_with_image_count",
        "five_star_with_image_count",
        "discount_percent",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    bool_cols = ["has_video", "is_mall", "is_freeship"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = normalize_bool(df[col])

    if "crawled_at" in df.columns:
        df["crawl_dt"] = pd.to_datetime(df["crawled_at"], errors="coerce")


st.sidebar.header("Bo loc")
filtered = df.copy()

if not df.empty:
    if "platform_id" in df.columns:
        options = ["Tat ca"] + sorted(df["platform_id"].dropna().astype(str).unique().tolist())
        selected_platform = st.sidebar.selectbox("San TMDT", options)
        if selected_platform != "Tat ca":
            filtered = filtered[filtered["platform_id"].astype(str) == selected_platform]

    if "crawled_by" in df.columns:
        crawlers = ["Tat ca"] + sorted(df["crawled_by"].dropna().astype(str).unique().tolist())
        selected_owner = st.sidebar.selectbox("Nguon crawl", crawlers)
        if selected_owner != "Tat ca":
            filtered = filtered[filtered["crawled_by"].astype(str) == selected_owner]

    if "price_current" in filtered.columns:
        valid_prices = filtered["price_current"].dropna()
        if not valid_prices.empty:
            min_price = float(valid_prices.min())
            max_price = float(valid_prices.max())
            selected_range = st.sidebar.slider(
                "Khoang gia (VND)",
                min_value=min_price,
                max_value=max_price,
                value=(min_price, max_price),
            )
            filtered = filtered[
                (filtered["price_current"] >= selected_range[0])
                & (filtered["price_current"] <= selected_range[1])
            ]


tab1, tab2, tab3 = st.tabs(["Tong quan", "Phan tich chi tiet", "So sanh va tong ket"])


with tab1:
    st.title("Dashboard toi uu gia va ty le chuyen doi")

    if filtered.empty:
        st.warning("Khong co du lieu phu hop bo loc hien tai.")
    else:
        st.caption(f"Nguon du lieu: {source_name}")
        st.caption("Bo cuc theo nguyen tac: KPI tong quan truoc, sau do moi den phan tich nhan qua.")

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Tong san pham", f"{len(filtered):,}")
        k2.metric(
            "Gia trung binh (VND)",
            f"{filtered['price_current'].mean():,.0f}" if "price_current" in filtered.columns else "N/A",
        )
        k3.metric(
            "Rating trung binh",
            f"{filtered['rating'].mean():.2f}" if "rating" in filtered.columns else "N/A",
        )
        k4.metric(
            "Luot ban trung binh",
            f"{filtered['sold_count'].mean():,.0f}" if "sold_count" in filtered.columns else "N/A",
        )

        q1, q2 = st.columns(2)
        with q1:
            if {"category_name", "sold_count"}.issubset(filtered.columns):
                top_cat = (
                    filtered.dropna(subset=["category_name", "sold_count"])
                    .groupby("category_name", as_index=False)["sold_count"]
                    .mean()
                    .sort_values("sold_count", ascending=False)
                    .head(10)
                )
                fig_cat = px.bar(
                    top_cat,
                    x="category_name",
                    y="sold_count",
                    title="Top danh muc theo luot ban trung binh",
                    labels={"category_name": "Danh muc", "sold_count": "Luot ban TB"},
                )
                st.plotly_chart(fig_cat, use_container_width=True)
            else:
                st.info("Khong du cot category_name/sold_count de ve top danh muc.")

        with q2:
            if "price_current" in filtered.columns:
                fig_hist = px.histogram(
                    filtered.dropna(subset=["price_current"]),
                    x="price_current",
                    nbins=40,
                    title="Phan bo gia san pham",
                    labels={"price_current": "Gia (VND)"},
                )
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("Khong du cot price_current de ve phan bo gia.")


with tab2:
    st.title("Phan tich cho SMART cua Thinh")

    st.subheader("THINH 1 - Chat luong trang chi tiet va doanh so Mall")
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
        st.warning(f"Thieu cot cho THINH 1: {', '.join(miss_1)}")
    else:
        mall_df = filtered[filtered["is_mall"]].copy().dropna(subset=["sold_count"])

        if mall_df.empty:
            st.info("Khong co du lieu Mall sau khi loc.")
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
                max(0.10, round(top_quartile["five_star_img_rate"].median(), 2))
                if not top_quartile.empty
                else 0.10
            )

            m1, m2, m3 = st.columns(3)
            m1.metric("So san pham Mall", f"{len(mall_df):,}")
            m2.metric("Luot ban TB (Mall)", f"{mall_df['sold_count'].mean():,.0f}")
            m3.metric("Nguong top 25% luot ban", f">= {q75:,.0f}")

            st.success(
                "De xuat 2 tieu chuan trung bay cho Q3/2026: "
                f"(1) toi thieu {std_image} anh/san pham, uu tien co video (ti le video top nhom {std_video_rate:.0%}); "
                f"(2) duy tri ti le review 5 sao kem anh >= {std_five_star_rate:.0%}."
            )

            c1, c2 = st.columns(2)
            with c1:
                fig_score = px.scatter(
                    mall_df,
                    x="display_quality_score",
                    y="sold_count",
                    color=mall_df["has_video_num"].map({1: "Co video", 0: "Khong video"}),
                    title="Diem trung bay va luot ban",
                    labels={
                        "display_quality_score": "Diem trung bay",
                        "sold_count": "Luot ban",
                        "color": "Video",
                    },
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
                    title="Luot ban TB theo nhom so anh",
                    labels={"image_bucket": "Nhom so anh", "sold_count": "Luot ban TB"},
                )
                st.plotly_chart(fig_bucket, use_container_width=True)

    st.markdown("---")

    st.subheader("THINH 2 - Hieu qua giam gia va nguong discount toi uu")
    thinh2_required = ["sold_count", "discount_percent"]
    miss_2 = missing_columns(filtered, thinh2_required)

    if miss_2:
        st.warning(f"Thieu cot cho THINH 2: {', '.join(miss_2)}")
    else:
        promo_df = filtered.dropna(subset=["sold_count", "discount_percent"]).copy()
        promo_df = promo_df[(promo_df["sold_count"] >= 0) & (promo_df["discount_percent"] >= 0)]
        promo_df["discount_percent"] = promo_df["discount_percent"].clip(upper=100)

        if promo_df.empty:
            st.info("Khong co du lieu hop le cho phan tich giam gia.")
        else:
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
            p1.metric("Mau phan tich", f"{len(promo_df):,}")
            p2.metric("Discount TB (%)", f"{promo_df['discount_percent'].mean():.1f}")
            p3.metric("Luot ban TB", f"{promo_df['sold_count'].mean():,.0f}")

            if not top2.empty:
                rec_text = ", ".join(top2["discount_band"].astype(str).tolist())
                st.success(
                    "De xuat 2 nguong giam gia uu tien de tang chuyen doi: "
                    f"{rec_text} (chi lay nhom du kich thuoc mau)."
                )

            g1, g2 = st.columns(2)
            with g1:
                fig_band = px.bar(
                    band_stats.sort_values("avg_sold", ascending=False),
                    x="discount_band",
                    y="avg_sold",
                    color="sample_size",
                    title="Luot ban TB theo nhom discount",
                    labels={"discount_band": "Nhom discount", "avg_sold": "Luot ban TB"},
                )
                st.plotly_chart(fig_band, use_container_width=True)

            with g2:
                fig_box = px.box(
                    promo_df,
                    x="discount_band",
                    y="sold_count",
                    title="Phan bo luot ban theo nhom discount",
                    labels={"discount_band": "Nhom discount", "sold_count": "Luot ban"},
                )
                st.plotly_chart(fig_box, use_container_width=True)

            category_col = "category_name" if "category_name" in promo_df.columns else "category_id"
            if category_col in promo_df.columns:
                top_discount = promo_df[promo_df["discount_percent"] >= promo_df["discount_percent"].quantile(0.75)]
                if not top_discount.empty:
                    cat_stats = (
                        top_discount.groupby(category_col, as_index=False)["sold_count"]
                        .mean()
                        .sort_values("sold_count", ascending=False)
                        .head(10)
                    )
                    fig_cat = px.bar(
                        cat_stats,
                        x=category_col,
                        y="sold_count",
                        title="Top danh muc hieu qua trong nhom discount cao",
                        labels={"sold_count": "Luot ban TB"},
                    )
                    st.plotly_chart(fig_cat, use_container_width=True)

            st.dataframe(band_stats.sort_values("avg_sold", ascending=False), use_container_width=True)

    st.markdown("---")

    st.subheader("TUAN - Gia ban va luot ban")
    tuan_required = ["price_current", "sold_count", "price_bucket"]
    miss_tuan = missing_columns(filtered, tuan_required)
    if miss_tuan:
        st.info(f"Thieu cot cho section TUAN: {', '.join(miss_tuan)}")
    else:
        fig_price = px.scatter(
            filtered.dropna(subset=["price_current", "sold_count"]),
            x="price_current",
            y="sold_count",
            color="price_bucket",
            title="Moi quan he gia va luot ban",
            labels={"price_current": "Gia (VND)", "sold_count": "Luot ban"},
        )
        st.plotly_chart(fig_price, use_container_width=True)


with tab3:
    st.title("So sanh va tong ket")

    if not filtered.empty and "crawled_by" in filtered.columns:
        st.subheader("Dong gop du lieu theo thanh vien")
        contrib = filtered["crawled_by"].astype(str).value_counts().reset_index()
        contrib.columns = ["Thanh vien", "So dong"]
        fig_contrib = px.pie(contrib, names="Thanh vien", values="So dong", title="Ty le du lieu moi nguoi crawl")
        st.plotly_chart(fig_contrib, use_container_width=True)

    st.subheader("Nguyen tac trinh bay da ap dung")
    st.markdown("1. Bat dau tu KPI tong quan, sau do moi drill-down vao tung gia thuyet.")
    st.markdown("2. Su dung mau va template nhat quan de giam tai nhan thuc.")
    st.markdown("3. Moi bieu do deu co title ro rang va don vi do luong.")
