"""
dashboard/app.py — Skeleton Streamlit Dashboard
Cài đặt: pip install streamlit pandas plotly
Chạy:    streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# ── Cấu hình trang ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TMĐT Analytics Dashboard",
    page_icon="🛒",
    layout="wide",
)

# ── Load dữ liệu ─────────────────────────────────────────────────────────────
DATA_PATH = Path(__file__).parent.parent / "data" / "processed" / "fact_product_merged.csv"

@st.cache_data
def load_data():
    if not DATA_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
    return df

df = load_data()

# ── Sidebar Filter ────────────────────────────────────────────────────────────
st.sidebar.header("🔍 Bộ lọc")
if not df.empty:
    platforms = ["Tất cả"] + sorted(df["platform_id"].dropna().unique().tolist())
    sel_platform = st.sidebar.selectbox("Sàn TMĐT", platforms)

    price_min, price_max = float(df["price_current"].min()), float(df["price_current"].max())
    sel_price = st.sidebar.slider("Khoảng giá (VNĐ)", price_min, price_max, (price_min, price_max))

    filtered = df.copy()
    if sel_platform != "Tất cả":
        filtered = filtered[filtered["platform_id"] == sel_platform]
    filtered = filtered[
        (filtered["price_current"] >= sel_price[0]) &
        (filtered["price_current"] <= sel_price[1])
    ]
else:
    filtered = df

# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — Tổng quan
# TAB 2 — Phân tích chi tiết theo mục tiêu từng thành viên
# TAB 3 — So sánh & tổng kết
# ════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3 = st.tabs(["📊 Tổng quan", "🔬 Phân tích chi tiết", "📋 So sánh & tổng kết"])

# ── TAB 1 ─────────────────────────────────────────────────────────────────────
with tab1:
    st.title("📊 Tổng quan dữ liệu TMĐT")

    if df.empty:
        st.warning("⚠️ Chưa có dữ liệu. Hãy chạy crawler và đặt file vào `data/processed/`.")
    else:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Tổng sản phẩm", f"{len(filtered):,}")
        col2.metric("Giá TB (VNĐ)",  f"{filtered['price_current'].mean():,.0f}")
        col3.metric("Rating TB",      f"{filtered['rating'].mean():.2f} ⭐")
        col4.metric("Lượt bán TB",    f"{filtered['sold_count'].mean():,.0f}")

        st.subheader("Phân bố giá theo danh mục")
        if "category_id" in filtered.columns:
            fig = px.box(filtered, x="category_id", y="price_current",
                         title="Phân bố giá theo danh mục")
            st.plotly_chart(fig, use_container_width=True)

# ── TAB 2 ─────────────────────────────────────────────────────────────────────
with tab2:
    st.title("🔬 Phân tích chi tiết")
    st.info("Mỗi thành viên thêm section phân tích của mình vào đây.")

    # === TUẤN — Giá vs Lượt bán ===
    st.subheader("TUẤN — Giá bán vs Lượt bán")
    if not filtered.empty and "sold_count" in filtered.columns:
        fig2 = px.scatter(
            filtered.dropna(subset=["sold_count"]),
            x="price_current", y="sold_count",
            color="price_bucket",
            title="Mối quan hệ Giá x Lượt bán",
            labels={"price_current": "Giá (VNĐ)", "sold_count": "Lượt bán"},
        )
        st.plotly_chart(fig2, use_container_width=True)

    # === Thêm phần phân tích của các thành viên khác bên dưới ===
    st.markdown("---")
    st.info("💡 Các thành viên: thêm biểu đồ phân tích của mình vào file `dashboard/app.py`")

# ── TAB 3 ─────────────────────────────────────────────────────────────────────
with tab3:
    st.title("📋 So sánh & Tổng kết")
    st.info("Tổng hợp kết quả phân tích và kết luận chung của nhóm.")

    if not filtered.empty and "crawled_by" in filtered.columns:
        st.subheader("Đóng góp dữ liệu theo thành viên")
        contrib = filtered["crawled_by"].value_counts().reset_index()
        contrib.columns = ["Thành viên", "Số dòng"]
        fig3 = px.pie(contrib, names="Thành viên", values="Số dòng",
                      title="Tỷ lệ dữ liệu mỗi người cào")
        st.plotly_chart(fig3, use_container_width=True)
