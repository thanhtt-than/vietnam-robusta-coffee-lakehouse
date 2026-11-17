"""Vietnam Robusta Coffee Lakehouse — Streamlit Dashboard.

Reads local Parquet exports written by dashboard/export_marts.py.
Does not modify Iceberg, Nessie, or MinIO.
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Vietnam Robusta | Data Lakehouse", page_icon="☕", layout="wide", initial_sidebar_state="expanded")
DATA_DIR = Path(__file__).resolve().parent / "data"
NAMES = {
    "Tổng quan": "mart_coffee_market_daily",
    "Xu hướng giá": "mart_coffee_price_trends",
    "Nội địa & London": "mart_futures_domestic_spread",
    "Sản xuất": "mart_coffee_production_annual",
    "Xuất khẩu": "mart_coffee_export_annual",
    "Thời tiết": "mart_weather_price_impact",
    "Nhiên liệu": "mart_fuel_price_impact",
    "Tin tức": "mart_market_news_analytics",
    "Phòng biểu đồ": None,
}

st.markdown("""<style>
.block-container {padding-top: 1.4rem; padding-bottom: 2rem}
[data-testid="stMetric"] {background: rgba(125,125,125,.07); padding: 12px 14px; border-radius: 11px; border: 1px solid rgba(130,130,130,.15)}
</style>""", unsafe_allow_html=True)

@st.cache_data(show_spinner="Đang tải dữ liệu Parquet…", ttl=120)
def read_mart(name: str) -> pd.DataFrame:
    path = DATA_DIR / name
    if not path.is_dir() or not list(path.glob("*.parquet")):
        raise FileNotFoundError(f"Không tìm thấy dữ liệu: {path}. Hãy chạy dashboard/export_marts.py trước.")
    df = pd.read_parquet(path)
    for col in ("market_date", "fuel_effective_date", "diesel_quote_date", "futures_trade_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def n(value, digits=0):
    if pd.isna(value): return "—"
    return f"{value:,.{digits}f}"

def safe_mean(s):
    return pd.to_numeric(s, errors="coerce").mean()

def numeric(df, *cols):
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def view_chart(fig, key=None):
    fig.update_layout(margin=dict(l=0, r=0, t=35, b=0), legend_title_text="", hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True, key=key)

def time_filter(df, title="Khoảng thời gian"):
    if "market_date" not in df.columns or df.empty: return df
    lo, hi = df.market_date.min().date(), df.market_date.max().date()
    if lo == hi: return df
    picked = st.sidebar.date_input(title, value=(lo, hi), min_value=lo, max_value=hi, key="date_range")
    if not isinstance(picked, (tuple, list)) or len(picked) != 2:
        st.info("Vui lòng chọn đủ ngày bắt đầu và kết thúc.")
        st.stop()
    a, b = sorted(picked)
    return df.loc[df.market_date.between(pd.Timestamp(a), pd.Timestamp(b))].copy()

def choices(df, col, title, key, prefer=None):
    vals = sorted(df[col].dropna().astype(str).unique().tolist())
    if not vals:
        st.warning(f"Không có dữ liệu cho bộ lọc {title}.")
        st.stop()
    default = prefer if prefer in vals else vals[0]
    return st.sidebar.selectbox(title, vals, index=vals.index(default), key=key)

def filters_source_province(df, source_col="source_family", province_col="province", key_prefix=""):
    if source_col in df.columns:
        source = choices(df, source_col, "Nguồn giá cà phê", key_prefix + "source", prefer="bbw_coffee")
        df = df[df[source_col].astype(str) == source].copy()
    if province_col in df.columns:
        province = choices(df, province_col, "Tỉnh", key_prefix + "province", prefer="DAK_LAK")
        df = df[df[province_col].astype(str) == province].copy()
    return df

def require_data(df):
    if df.empty:
        st.warning("Không có bản ghi trong phạm vi lọc.")
        st.stop()

with st.sidebar:
    st.markdown("## ☕ Robusta Lakehouse")
    st.caption("Gold Data Marts · Apache Iceberg")
    page = st.radio("Trang phân tích", list(NAMES), label_visibility="collapsed")
    st.divider()
    if st.button("↻ Tải lại Parquet", use_container_width=True):
        read_mart.clear()
        st.rerun()

if page == "Phòng biểu đồ":
    from custom_charts import render_custom_charts
    render_custom_charts(DATA_DIR, read_mart)
    st.stop()

st.title("☕ Vietnam Robusta Coffee")
st.caption(f"{page} · Nguồn: {NAMES[page]} · Dữ liệu xuất Parquet (không phải luồng realtime)")
try:
    df = read_mart(NAMES[page])
except Exception as e:
    st.error(str(e))
    st.stop()

if page == "Tổng quan":
    df = time_filter(df)
    require_data(df)
    # This mart has one market price per date and province; avoid summing prices.
    province = choices(df, "province", "Tỉnh", "overview_province", prefer="DAK_LAK")
    x = numeric(df[df.province.astype(str) == province], "coffee_price_vnd_kg", "coffee_change_pct", "coffee_ma7_vnd_kg", "diesel_price_vnd_l", "temperature_mean_c", "rain_mm", "news_article_count")
    require_data(x)
    x = x.sort_values("market_date")
    latest = x.iloc[-1]
    a,b,c,d = st.columns(4)
    a.metric("Giá cà phê mới nhất", f"{n(latest.coffee_price_vnd_kg)} đ/kg")
    b.metric("Biến động ngày", f"{n(latest.coffee_change_pct, 3)}%")
    c.metric("MA7", f"{n(latest.coffee_ma7_vnd_kg)} đ/kg")
    d.metric("Ngày dữ liệu", latest.market_date.strftime("%d/%m/%Y"))
    price = x.groupby("market_date", as_index=False).agg(coffee_price_vnd_kg=("coffee_price_vnd_kg", "mean"), coffee_ma7_vnd_kg=("coffee_ma7_vnd_kg", "mean"))
    view_chart(px.line(price, x="market_date", y=["coffee_price_vnd_kg", "coffee_ma7_vnd_kg"], title="Giá cà phê và MA7", labels={"value":"VND/kg", "market_date":"Ngày", "variable":"Chỉ số"}))
    left,right = st.columns(2)
    with left:
        w = x.groupby("market_date", as_index=False)[["rain_mm", "temperature_mean_c"]].mean(numeric_only=True)
        view_chart(px.bar(w, x="market_date", y="rain_mm", title="Lượng mưa theo ngày", labels={"rain_mm":"mm", "market_date":"Ngày"}))
    with right:
        f = x.dropna(subset=["diesel_price_vnd_l"])
        if not f.empty:
            view_chart(px.line(f, x="market_date", y="diesel_price_vnd_l", title="Giá dầu diesel tham chiếu", labels={"diesel_price_vnd_l":"VND/lít", "market_date":"Ngày"}))
        else: st.info("Không có dữ liệu diesel trong khoảng ngày đã chọn.")

elif page == "Xu hướng giá":
    df = time_filter(df)
    df = filters_source_province(df, key_prefix="trend_")
    require_data(df)
    series = choices(df, "series_code", "Chuỗi giá", "trend_series")
    x = numeric(df[df.series_code == series], "coffee_price_vnd_kg", "ma7_vnd_kg", "ma30_vnd_kg", "price_change_pct", "volatility30_vnd_kg").sort_values("market_date")
    require_data(x)
    last = x.iloc[-1]
    a,b,c,d = st.columns(4)
    a.metric("Giá mới nhất", f"{n(last.coffee_price_vnd_kg)} đ/kg")
    b.metric("Thay đổi", f"{n(last.price_change_pct, 3)}%")
    c.metric("MA30", f"{n(last.ma30_vnd_kg)} đ/kg")
    d.metric("Độ lệch chuẩn 30 quan sát", n(last.volatility30_vnd_kg, 1))
    view_chart(px.line(x, x="market_date", y=["coffee_price_vnd_kg", "ma7_vnd_kg", "ma30_vnd_kg"], title="Giá cà phê · MA7 và MA30 (theo số quan sát)", labels={"value":"VND/kg", "market_date":"Ngày", "variable":"Chỉ số"}))
    view_chart(px.bar(x, x="market_date", y="price_change_pct", title="Biến động giá (%)", labels={"market_date":"Ngày", "price_change_pct":"%"}))

elif page == "Nội địa & London":
    df = time_filter(df)
    df = filters_source_province(df, key_prefix="futures_")
    require_data(df)
    series_key = choices(df, "price_series_key", "Mã chuỗi giá nội địa", "futures_series")
    x = numeric(df[df.price_series_key.astype(str) == series_key], "domestic_change_pct", "futures_avg_change_pct", "domestic_price_vnd_kg", "futures_avg_last_price", "change_gap_percentage_points")
    require_data(x)
    st.info("Giá nội địa (VND/kg) và giá futures sử dụng đơn vị báo giá khác nhau. Chỉ so sánh biến động %, không lấy hiệu giá tuyệt đối.")
    a,b,c = st.columns(3)
    last = x.sort_values("market_date").iloc[-1]
    a.metric("Giá nội địa", f"{n(last.domestic_price_vnd_kg)} đ/kg")
    b.metric("Biến động nội địa", f"{n(last.domestic_change_pct, 3)}%")
    c.metric("Biến động futures", f"{n(last.futures_avg_change_pct, 3)}%")
    view_chart(px.line(x.sort_values("market_date"), x="market_date", y=["domestic_change_pct", "futures_avg_change_pct"], title="So sánh phần trăm biến động", labels={"value":"%", "market_date":"Ngày", "variable":"Thị trường"}))
    view_chart(px.line(x.sort_values("market_date"), x="market_date", y="change_gap_percentage_points", title="Chênh lệch biến động (điểm phần trăm)", labels={"market_date":"Ngày", "change_gap_percentage_points":"Điểm %"}))

elif page == "Sản xuất":
    df = numeric(df, "coffee_area_thousand_ha", "coffee_production_thousand_tons", "yield_tons_per_ha", "production_growth_pct")
    if df.geography_code.nunique() > 1:
        geo = choices(df, "geography_name", "Khu vực", "production_geo")
        df = df[df.geography_name == geo]
    df = df.sort_values("production_year")
    require_data(df)
    last = df.iloc[-1]
    a,b,c = st.columns(3)
    a.metric("Sản lượng năm mới nhất", f"{n(last.coffee_production_thousand_tons, 1)} nghìn tấn")
    b.metric("Diện tích", f"{n(last.coffee_area_thousand_ha, 1)} nghìn ha")
    c.metric("Năng suất", f"{n(last.yield_tons_per_ha, 2)} tấn/ha")
    view_chart(px.line(df, x="production_year", y=["coffee_production_thousand_tons", "coffee_area_thousand_ha"], markers=True, title="Sản lượng (nghìn tấn) và diện tích (nghìn ha) — hai chỉ số khác đơn vị", labels={"production_year":"Năm", "value":"Nghìn đơn vị", "variable":"Chỉ số"}))
    view_chart(px.bar(df, x="production_year", y="yield_tons_per_ha", title="Năng suất cà phê", labels={"production_year":"Năm", "yield_tons_per_ha":"Tấn/ha"}))
    st.caption("Lưu ý trạng thái số liệu sơ bộ/chính thức và các năm không có quan sát.")
    st.dataframe(df[["production_year", "area_publication_status", "production_publication_status", "production_quality_status"]], hide_index=True, use_container_width=True)

elif page == "Xuất khẩu":
    df = numeric(df, "export_value_usd_thousand", "export_growth_yoy_pct", "destination_share_pct")
    yr = st.sidebar.selectbox("Năm xuất khẩu", sorted(df.export_year.dropna().unique().tolist(), reverse=True), key="export_year")
    df = df[df.export_year == yr]
    product = choices(df, "product_code", "Mã HS", "export_product")
    df = df[df.product_code == product].copy()
    require_data(df)
    # World aggregates must not be summed together with partner rows; select one reporter/source slice.
    reporter = choices(df, "reporter_code", "Reporter", "export_reporter")
    df = df[df.reporter_code == reporter]
    source = choices(df, "source_key", "Nguồn dữ liệu", "export_source")
    df = df[df.source_key.astype(str) == source]
    world = df[df.is_world_aggregate.fillna(False)]
    partners = df[~df.is_world_aggregate.fillna(False)].copy()
    st.info("Giá trị xuất khẩu tính bằng **nghìn USD**. Không cộng dòng World vào các thị trường đối tác để tránh đếm hai lần.")
    a,b,c = st.columns(3)
    a.metric("Tổng xuất khẩu (World)", f"{n(world.export_value_usd_thousand.iloc[0], 1)} nghìn USD" if not world.empty else "—")
    b.metric("Số thị trường", f"{partners.partner_code.nunique():,}")
    c.metric("Năm dữ liệu", str(yr))
    top = partners.groupby("partner_name", as_index=False)["export_value_usd_thousand"].sum().nlargest(15, "export_value_usd_thousand")
    if not top.empty:
        view_chart(px.bar(top.sort_values("export_value_usd_thousand"), y="partner_name", x="export_value_usd_thousand", orientation="h", title="Top thị trường xuất khẩu", labels={"export_value_usd_thousand":"Nghìn USD", "partner_name":"Thị trường"}))
    st.dataframe(partners[["partner_name", "export_value_usd_thousand", "destination_share_pct", "export_growth_yoy_pct", "quality_status"]].sort_values("export_value_usd_thousand", ascending=False), hide_index=True, use_container_width=True)

elif page == "Thời tiết":
    df = time_filter(df)
    df = filters_source_province(df, key_prefix="weather_")
    require_data(df)
    series_key = choices(df, "price_series_key", "Mã chuỗi giá", "weather_series")
    x = numeric(df[df.price_series_key.astype(str) == series_key], "rain_sum_mm", "temperature_mean_c", "price_change_pct", "rain_price_corr30", "temperature_price_corr30").sort_values("market_date")
    require_data(x)
    a,b,c = st.columns(3)
    a.metric("Ngày có thời tiết", f"{x.rain_sum_mm.notna().sum():,}/{len(x):,}")
    b.metric("Nhiệt độ TB", f"{n(x.temperature_mean_c.mean(), 1)} °C")
    c.metric("Tổng lượng mưa quan sát", f"{n(x.rain_sum_mm.sum(min_count=1), 1)} mm")
    view_chart(px.line(x, x="market_date", y=["rain_price_corr30", "temperature_price_corr30"], title="Tương quan trượt 30 quan sát với biến động giá", labels={"value":"Hệ số tương quan", "market_date":"Ngày", "variable":"Chỉ số"}))
    view_chart(px.scatter(x, x="rain_sum_mm", y="price_change_pct", color="temperature_mean_c", title="Lượng mưa và biến động giá", labels={"rain_sum_mm":"Mưa (mm)", "price_change_pct":"Biến động giá (%)", "temperature_mean_c":"Nhiệt độ (°C)"}))
    st.caption("Tương quan không chứng minh quan hệ nhân quả. Những ngày thiếu thời tiết vẫn được giữ trong chuỗi giá.")

elif page == "Nhiên liệu":
    # Filter 177k cross-joined rows before any aggregation.
    source = choices(df, "fuel_source_family", "Nguồn nhiên liệu", "fuel_source", prefer="bbw_fuel")
    df = df[df.fuel_source_family == source]
    fuel_series = st.sidebar.selectbox("Chuỗi nhiên liệu", sorted(df.fuel_series_key.dropna().unique().tolist()), format_func=lambda k: f"{df.loc[df.fuel_series_key == k, 'fuel_series_code'].iloc[0]} [{int(k)}]", key="fuel_series")
    df = df[df.fuel_series_key == fuel_series]
    df = time_filter(df)
    df = filters_source_province(df, source_col="coffee_source_family", key_prefix="fuel_")
    if "price_series_key" in df.columns:
        series = choices(df, "price_series_key", "Mã chuỗi giá cà phê", "fuel_coffee_series")
        df = df[df.price_series_key.astype(str) == series]
    x = numeric(df, "fuel_price_vnd_per_unit", "coffee_price_vnd_kg", "fuel_change_pct", "coffee_change_pct", "fuel_age_days").sort_values("market_date")
    require_data(x)
    last = x.iloc[-1]
    a,b,c,d = st.columns(4)
    a.metric("Giá nhiên liệu", f"{n(last.fuel_price_vnd_per_unit)} {last.fuel_unit}")
    b.metric("Giá cà phê", f"{n(last.coffee_price_vnd_kg)} đ/kg")
    c.metric("Tuổi báo giá nhiên liệu", f"{n(last.fuel_age_days)} ngày")
    d.metric("Số ngày dữ liệu", str(x.market_date.nunique()))
    view_chart(px.line(x, x="market_date", y="fuel_price_vnd_per_unit", title="Giá nhiên liệu hiệu lực theo ngày", labels={"market_date":"Ngày", "fuel_price_vnd_per_unit":str(last.fuel_unit)}))
    updates = x[x.is_fuel_update_day.fillna(False)]
    if not updates.empty:
        view_chart(px.bar(updates, x="market_date", y=["fuel_change_pct", "coffee_change_pct"], barmode="group", title="Ngày cập nhật nhiên liệu: so sánh biến động (%)", labels={"market_date":"Ngày", "value":"%", "variable":"Chỉ số"}))
    else: st.info("Không có ngày thay đổi giá nhiên liệu trùng ngày quan sát cà phê trong phạm vi lọc.")
    st.caption("Các chuỗi nhiên liệu thuộc nguồn/khu vực khác nhau không được gộp. Dữ liệu này phản ánh đồng biến động, không phải tác động nhân quả.")

elif page == "Tin tức":
    df = time_filter(df)
    require_data(df)
    df = numeric(df, "article_count", "primary_article_count", "daily_unique_article_count", "article_ma7", "topic_share_pct")
    topic = choices(df, "topic_name", "Chủ đề", "news_topic")
    daily = df[["market_date", "daily_unique_article_count"]].drop_duplicates(subset=["market_date"])
    x = df[df.topic_name == topic].sort_values("market_date")
    a,b,c,d = st.columns(4)
    a.metric("Tổng bài duy nhất (mọi chủ đề)", f"{int(daily.daily_unique_article_count.sum()):,}")
    b.metric("Bài thuộc chủ đề", f"{int(x.article_count.sum()):,}")
    c.metric("Bài chủ đề chính", f"{int(x.primary_article_count.sum()):,}")
    d.metric("Ngày có tin", f"{int((daily.daily_unique_article_count > 0).sum()):,}")
    view_chart(px.line(x, x="market_date", y=["article_count", "article_ma7"], title=f"Lượng tin: {topic}", labels={"value":"Số bài", "market_date":"Ngày", "variable":"Chỉ số"}))
    totals = df.groupby("topic_name", as_index=False)["article_count"].sum().sort_values("article_count", ascending=False)
    view_chart(px.bar(totals, x="article_count", y="topic_name", orientation="h", title="Số lượt bài theo chủ đề", labels={"article_count":"Lượt bài", "topic_name":"Chủ đề"}))
    st.caption("Một bài có thể thuộc nhiều chủ đề, nên tổng các cột chủ đề có thể lớn hơn số bài duy nhất. Ngày không có bài: topic_share_pct là NULL.")

st.divider()
with st.expander("Xem dữ liệu đang lọc"):
    st.dataframe(df.head(1000), use_container_width=True, hide_index=True)
st.caption("Dữ liệu đang đọc từ bản xuất Parquet tại dashboard/data. Để cập nhật, chạy export_marts.py rồi nhấn 'Tải lại Parquet'.")
