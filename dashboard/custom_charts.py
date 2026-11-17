"""Interactive chart studio for Vietnam Robusta Lakehouse.

Read-only: uses dashboard/data Parquet exports through the existing loader.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

MARTS = {
    'Tổng quan thị trường': 'mart_coffee_market_daily',
    'Xu hướng giá': 'mart_coffee_price_trends',
    'Nội địa & London': 'mart_futures_domestic_spread',
    'Sản xuất': 'mart_coffee_production_annual',
    'Xuất khẩu': 'mart_coffee_export_annual',
    'Thời tiết': 'mart_weather_price_impact',
    'Nhiên liệu': 'mart_fuel_price_impact',
    'Tin tức': 'mart_market_news_analytics',
}

DATE_COLS = ('market_date', 'production_year', 'export_year')
ID_COLS = {
    'price_fact_key', 'production_fact_key', 'export_fact_key', 'news_topic_fact_key',
    'date_key', 'price_series_key', 'fuel_series_key', 'source_key',
    'destination_key', 'hs_product_key', 'year_key', 'topic_key',
    'raw_observation_count', 'source_key', 'fuel_fact_key',
}
DIMENSIONS = (
    'province', 'source_family', 'coffee_source_family', 'series_code',
    'fuel_source_family', 'fuel_series_code', 'fuel_region', 'fuel_unit',
    'topic_name', 'partner_name', 'is_world_aggregate', 'geography_name',
    'reporter_code', 'product_code', 'quality_status',
)


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in DATE_COLS:
        if col in df.columns and col == 'market_date':
            df[col] = pd.to_datetime(df[col], errors='coerce')
    for col in df.columns:
        if col in DATE_COLS or col in ID_COLS or pd.api.types.is_bool_dtype(df[col]):
            continue
        if df[col].dtype == object:
            observed = df[col].dropna()
            if len(observed) and hasattr(observed.iloc[0], 'as_tuple'):
                df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def _metrics(df):
    return [c for c in df.select_dtypes(include='number').columns
            if c not in ID_COLS and c not in DATE_COLS
            and not c.endswith('_key') and not c.endswith('_fact_key')]


def _filters(df, namespace):
    with st.expander('Bộ lọc dữ liệu', expanded=True):
        available = [c for c in DIMENSIONS if c in df.columns]
        selected_cols = st.multiselect('Chiều lọc', available,
                                       default=[c for c in ('province', 'source_family') if c in available],
                                       key=namespace + '_columns')
        for col in selected_cols:
            vals = sorted(df[col].dropna().unique().tolist(), key=str)
            if not vals:
                continue
            # Single choice by default where several distinct fuel/source series would be misleading.
            default = []
            if col in ('source_family', 'coffee_source_family', 'fuel_source_family'):
                preferred = {'source_family': 'bbw_coffee', 'coffee_source_family': 'bbw_coffee',
                             'fuel_source_family': 'bbw_fuel'}[col]
                if preferred in vals:
                    default = [preferred]
            chosen = st.multiselect(col, vals, default=default,
                                     key=namespace + '_filter_' + col)
            if chosen:
                df = df[df[col].isin(chosen)]
        if 'market_date' in df.columns and not df.empty:
            date_vals = df['market_date'].dropna()
            if not date_vals.empty:
                lo, hi = date_vals.min().date(), date_vals.max().date()
                if lo < hi:
                    dates = st.date_input('Ngày quan sát', value=(lo, hi),
                                          min_value=lo, max_value=hi,
                                          key=namespace + '_dates')
                    if isinstance(dates, (tuple, list)) and len(dates) == 2:
                        df = df[df.market_date.between(pd.Timestamp(dates[0]), pd.Timestamp(dates[1]))]
    return df


def _aggregate(df, x, metrics, method):
    return (df.groupby(x, dropna=False, observed=True)[metrics]
              .agg({'Trung bình': 'mean', 'Tổng': 'sum', 'Nhỏ nhất': 'min',
                    'Lớn nhất': 'max', 'Trung vị': 'median'}[method])
              .reset_index().sort_values(x))


def _plot(series, df, x_col, metrics, chart, dual_axis):
    if chart == 'Đường':
        if dual_axis and len(metrics) > 1:
            fig = make_subplots(specs=[[{'secondary_y': True}]])
            for i, col in enumerate(metrics):
                fig.add_trace(go.Scatter(x=series[x_col], y=series[col], name=col,
                                         mode='lines+markers'), secondary_y=i > 0)
            fig.update_yaxes(title_text=metrics[0], secondary_y=False)
            fig.update_yaxes(title_text=' / '.join(metrics[1:]), secondary_y=True)
            return fig
        return px.line(series, x=x_col, y=metrics, markers=True)
    if chart == 'Cột':
        return px.bar(series, x=x_col, y=metrics, barmode='group')
    if chart == 'Miền':
        fig = px.area(series, x=x_col, y=metrics)
        fig.update_traces(stackgroup=None, fill='tozeroy')
        return fig
    if chart == 'Điểm (Scatter)':
        return (px.scatter(df, x=metrics[0], y=metrics[1], opacity=.55,
                           hover_data=[x_col]) if len(metrics) > 1
                else px.scatter(series, x=x_col, y=metrics[0]))
    if chart == 'Hộp (Box)':
        return px.box(df, x=x_col, y=metrics[0])
    if chart == 'Phân phối (Histogram)':
        return px.histogram(df, x=metrics[0], nbins=40)
    if chart == 'Tròn':
        positives = series[series[metrics[0]] >= 0].nlargest(20, metrics[0])
        return px.pie(positives, names=x_col, values=metrics[0])
    if chart == 'Tương quan (Heatmap)':
        if len(metrics) < 2:
            st.info('Chọn ít nhất hai chỉ số để hiển thị ma trận tương quan.')
            return None
        corr = df[metrics].corr(numeric_only=True)
        return px.imshow(corr, text_auto='.2f', zmin=-1, zmax=1,
                         color_continuous_scale='RdBu_r')
    return None


def _one_mart(read_mart):
    label = st.selectbox('Data Mart', list(MARTS), key='studio_mart')
    df = _prepare(read_mart(MARTS[label]))
    df = _filters(df, 'single')
    if df.empty:
        st.warning('Không có dữ liệu trong bộ lọc này.')
        return
    metrics = _metrics(df)
    if not metrics:
        st.warning('Không có cột chỉ số để vẽ.')
        return
    dimensions = [c for c in DATE_COLS + DIMENSIONS if c in df.columns]
    x = st.selectbox('Trục X / nhóm dữ liệu', dimensions, key='studio_x')
    recommended = [c for c in ('coffee_price_vnd_kg', 'ma7_vnd_kg', 'ma30_vnd_kg',
                                'article_count', 'export_value_usd_thousand') if c in metrics]
    chosen = st.multiselect('Chỉ số Y (chọn nhiều)', metrics,
                             default=(recommended[:3] or metrics[:1]), key='studio_metrics')
    c1, c2, c3 = st.columns(3)
    chart = c1.selectbox('Loại biểu đồ', ['Đường', 'Cột', 'Miền', 'Điểm (Scatter)',
                                         'Hộp (Box)', 'Phân phối (Histogram)',
                                         'Tròn', 'Tương quan (Heatmap)'], key='studio_chart')
    agg = c2.selectbox('Tổng hợp', ['Trung bình', 'Tổng', 'Trung vị', 'Nhỏ nhất', 'Lớn nhất'],
                        key='studio_agg')
    dual = c3.checkbox('Hai trục Y', key='studio_dual',
                        disabled=(chart != 'Đường' or len(chosen) < 2))
    if not chosen:
        st.info('Chọn ít nhất một chỉ số.')
        return
    if agg == 'Tổng' and any(any(t in c for t in ('price', 'ma7', 'ma30', 'pct', 'corr', 'yield', 'temperature')) for c in chosen):
        st.warning('Phép Tổng có thể không phù hợp với giá, phần trăm, hệ số tương quan hoặc MA.')
    if len(df) > 50000 and chart in ('Điểm (Scatter)', 'Hộp (Box)', 'Phân phối (Histogram)'):
        st.caption('Để biểu đồ phản hồi nhanh hơn, hiển thị mẫu tối đa 50.000 dòng.')
        plot_df = df.sample(n=50000, random_state=12)
    else:
        plot_df = df
    series = _aggregate(df, x, chosen, agg)
    fig = _plot(series, plot_df, x, chosen, chart, dual)
    if fig is not None:
        fig.update_layout(height=540, template='plotly_white', margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)
    st.caption(f'{len(df):,} dòng sau lọc; {len(series):,} nhóm. Tương quan không hàm ý quan hệ nhân quả.')
    with st.expander('Dữ liệu dùng để vẽ'):
        st.dataframe(series.head(1000), use_container_width=True, hide_index=True)
    st.download_button('Tải CSV biểu đồ', series.to_csv(index=False).encode('utf-8-sig'),
                       'robusta_custom_chart.csv', 'text/csv', key='studio_download')


def _series_per_mart(read_mart, caption, mart, key):
    df = _prepare(read_mart(mart))
    if 'market_date' not in df.columns:
        st.warning(f'{caption}: không có ngày ở độ hạt hàng ngày, không thể ghép trực tiếp.')
        return None
    with st.expander('Bộ lọc ' + caption, expanded=True):
        for col in ('province', 'source_family', 'coffee_source_family', 'fuel_source_family',
                    'fuel_series_key', 'price_series_key', 'topic_name'):
            if col in df.columns:
                values = sorted(df[col].dropna().unique().tolist(), key=str)
                if not values:
                    continue
                prefer = next((v for v in values if str(v) in ('DAK_LAK', 'bbw_coffee', 'bbw_fuel')), values[0])
                value = st.selectbox(col, values, index=values.index(prefer), key=key + col,
                                     format_func=str)
                df = df[df[col] == value]
    columns = _metrics(df)
    if not columns:
        return None
    metric = st.selectbox('Chỉ số ' + caption, columns, key=key + 'metric')
    # Each chosen series is collapsed to one value per calendar day BEFORE joining.
    # Missing-day records are never filled with zero.
    aggregation = st.selectbox('Tổng hợp theo ngày ' + caption,
                                ['Trung bình', 'Tổng', 'Trung vị'], key=key + 'agg')
    if aggregation == 'Tổng' and any(t in metric for t in ('price', 'pct', 'corr', 'ma', 'temperature', 'yield')):
        st.warning(f'{caption}: cộng chỉ số này theo ngày có thể gây hiểu nhầm.')
    daily = (_aggregate(df.dropna(subset=['market_date']), 'market_date', [metric], aggregation)
              .rename(columns={metric: caption + ': ' + metric}))
    return daily


def _cross_mart(read_mart):
    st.info('Ghép các chuỗi **theo ngày**, sau khi lọc riêng nguồn/tỉnh và tổng hợp từng chuỗi. '
            'Chỉ hỗ trợ Data Mart có `market_date`; không ghép sản xuất/xuất khẩu theo năm với số liệu ngày.')
    allowed = {label: mart for label, mart in MARTS.items()
               if mart not in ('mart_coffee_production_annual', 'mart_coffee_export_annual')}
    left, right = st.columns(2)
    with left:
        label_a = st.selectbox('Nguồn chỉ số A', list(allowed), index=1, key='cross_A_mart')
        a = _series_per_mart(read_mart, 'A', allowed[label_a], 'cross_A_')
    with right:
        label_b = st.selectbox('Nguồn chỉ số B', list(allowed), index=4, key='cross_B_mart')
        b = _series_per_mart(read_mart, 'B', allowed[label_b], 'cross_B_')
    if a is None or b is None:
        return
    merged = a.merge(b, on='market_date', how='inner', validate='one_to_one').sort_values('market_date')
    if merged.empty:
        st.warning('Hai chuỗi không có ngày quan sát trùng nhau.')
        return
    cols = [c for c in merged.columns if c != 'market_date']
    chart = st.radio('Cách hiển thị', ['Hai trục Y', 'Chỉ số chuẩn hóa (gốc = 100)', 'Scatter'],
                     horizontal=True, key='cross_style')
    if chart == 'Hai trục Y':
        fig = make_subplots(specs=[[{'secondary_y': True}]])
        fig.add_trace(go.Scatter(x=merged.market_date, y=merged[cols[0]], name=cols[0]), secondary_y=False)
        fig.add_trace(go.Scatter(x=merged.market_date, y=merged[cols[1]], name=cols[1]), secondary_y=True)
        fig.update_yaxes(title_text=cols[0], secondary_y=False)
        fig.update_yaxes(title_text=cols[1], secondary_y=True)
    elif chart == 'Chỉ số chuẩn hóa (gốc = 100)':
        tmp = merged[['market_date'] + cols].copy()
        for col in cols:
            valid = tmp[col].dropna()
            if valid.empty or valid.iloc[0] == 0:
                st.warning(f'Không thể chuẩn hóa {col}: giá trị gốc rỗng hoặc bằng 0.')
                return
            tmp[col] = 100 * tmp[col] / valid.iloc[0]
        fig = px.line(tmp, x='market_date', y=cols, labels={'value': 'Chỉ số (gốc = 100)'})
    else:
        fig = px.scatter(merged, x=cols[0], y=cols[1], hover_data=['market_date'])
    fig.update_layout(height=540, template='plotly_white', margin=dict(l=10, r=10, t=35, b=10))
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f'{len(merged):,} ngày có dữ liệu ở cả hai chuỗi. Hai trục Y khác đơn vị chỉ để so sánh xu hướng; '
               'không suy ra quan hệ nhân quả.')
    with st.expander('Dữ liệu ghép'):
        st.dataframe(merged.head(1000), use_container_width=True, hide_index=True)
    st.download_button('Tải CSV dữ liệu ghép', merged.to_csv(index=False).encode('utf-8-sig'),
                       'robusta_cross_mart.csv', 'text/csv', key='cross_download')


def render_custom_charts(data_dir, read_mart):
    st.title('📊 Phòng phân tích biểu đồ')
    st.caption('Tùy chọn biểu đồ và kết hợp chỉ số từ 8 Data Mart • Nguồn: bản xuất Parquet')
    tab1, tab2 = st.tabs(['Thiết kế biểu đồ', 'Kết hợp chỉ số từ hai Data Mart'])
    with tab1:
        _one_mart(read_mart)
    with tab2:
        _cross_mart(read_mart)
