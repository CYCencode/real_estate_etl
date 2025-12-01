# v2
#pipelines/visualization.py
"""視覺化圖表生成模組"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import contextily as cx
from pyproj import Transformer
from io import BytesIO, StringIO

from config import (
    COLORS, ZONE_ORDER, BUILDING_TYPE_ORDER, CITY_BOUNDS, MAX_POINTS,
    REPORT_CITIES, CITY_DISPLAY_TO_CODE,
    FIGURE_SIZE_WIDE, FIGURE_SIZE_MEDIUM, FIGURE_SIZE_LARGE, FIGURE_SIZE_MAP, SUBPLOT_LEFT, SUBPLOT_RIGHT,
    LEGEND_RIGHT, LEGEND_RIGHT_LOWER, CHART_DPI, LABEL_THRESHOLD,
    PRICE_COLORS, SIZE_MAPPING,
    AGE_BINS, AGE_LABELS, AREA_BINS, AREA_LABELS,
    PRICE_BINS_USED, PRICE_LABELS_USED, PRICE_BINS_NEW, PRICE_LABELS_NEW
)
from utils import log_to_gcs, upload_chart_to_gcs, check_charts_exist, load_existing_charts

# ==================== 共用輔助函數 ====================
def create_dual_axis_figure():
    """創建左右雙子圖的 Figure"""
    fig = plt.figure(figsize=FIGURE_SIZE_WIDE)
    ax1 = fig.add_axes(SUBPLOT_LEFT)
    ax2 = fig.add_axes(SUBPLOT_RIGHT)
    return fig, ax1, ax2

def setup_stacked_bar_axis(ax, title, is_left=True):
    """設定堆疊橫條圖的軸屬性"""
    ax.set_xlabel('百分比 (%)', fontsize=12)
    ax.set_ylabel('區域' if is_left else '', fontsize=12)
    ax.set_title(title, fontsize=13, pad=10, weight='bold')
    ax.set_xlim(0, 100)
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.invert_yaxis()
    if not is_left:
        ax.set_yticklabels([])

def add_percentage_labels(ax, threshold=LABEL_THRESHOLD):
    """為堆疊橫條圖添加百分比標籤"""
    for container in ax.containers:
        labels = [f'{value:.0f}%' if value >= threshold else '' 
                 for value in container.datavalues]
        ax.bar_label(container, labels=labels, label_type='center', fontsize=9, weight='bold')

def add_dual_legends(fig, labels_left, labels_right, title_left, title_right):
    """添加左右兩個圖例"""
    legend_ax1 = fig.add_axes(LEGEND_RIGHT)
    legend_ax1.axis('off')
    handles_left = [plt.Rectangle((0,0),1,1, fc=COLORS[i]) 
                   for i in range(len(labels_left))]
    legend_ax1.legend(handles_left, labels_left, loc='upper center', title=title_left, frameon=True, fontsize=9, title_fontsize=10)
    
    legend_ax2 = fig.add_axes(LEGEND_RIGHT_LOWER)
    legend_ax2.axis('off')
    handles_right = [plt.Rectangle((0,0),1,1, fc=COLORS[i]) 
                    for i in range(len(labels_right))]
    legend_ax2.legend(handles_right, labels_right, loc='upper center', title=title_right, frameon=True, fontsize=9, title_fontsize=10)


def save_and_upload(chart_name, year, season):
    """儲存並上傳圖表（加入解析度追蹤）"""
    buffer = BytesIO()
    
    # 取得當前 figure
    fig = plt.gcf()
    
    # ✅ 埋點 1: 記錄 Figure 尺寸（英吋）
    fig_width, fig_height = fig.get_size_inches()
    log_to_gcs('INFO', f"📊 [{chart_name}] Figure 尺寸: {fig_width:.2f}\" x {fig_height:.2f}\" (DPI={CHART_DPI})")
    
    # ✅ 埋點 2: 計算實際像素尺寸
    pixel_width = int(fig_width * CHART_DPI)
    pixel_height = int(fig_height * CHART_DPI)
    log_to_gcs('INFO', f"📊 [{chart_name}] 預期像素: {pixel_width} x {pixel_height} px")
    
    # 儲存圖表到 buffer
    plt.savefig(buffer, format='png', dpi=CHART_DPI, bbox_inches='tight')
    plt.close()
    
    # ✅ 埋點 3: 記錄 buffer 大小（儲存後）
    buffer_size_kb = buffer.getbuffer().nbytes / 1024
    log_to_gcs('INFO', f"📊 [{chart_name}] Buffer 大小: {buffer_size_kb:.2f} KB")
    
    # ✅ 埋點 4: 使用 PIL 驗證實際圖片解析度
    try:
        from PIL import Image
        buffer.seek(0)
        img = Image.open(buffer)
        actual_width, actual_height = img.size
        img_format = img.format
        img_mode = img.mode
        
        log_to_gcs('INFO', f"📊 [{chart_name}] PIL 驗證 - 實際解析度: {actual_width} x {actual_height} px")
        log_to_gcs('INFO', f"📊 [{chart_name}] PIL 驗證 - 格式: {img_format}, 模式: {img_mode}")
        
        # 檢查解析度是否符合預期
        if actual_width != pixel_width or actual_height != pixel_height:
            log_to_gcs('WARNING', f"⚠️ [{chart_name}] 解析度不符！預期 {pixel_width}x{pixel_height}, 實際 {actual_width}x{actual_height}")
        else:
            log_to_gcs('INFO', f"✅ [{chart_name}] 解析度符合預期")
            
    except Exception as e:
        log_to_gcs('ERROR', f"❌ [{chart_name}] PIL 驗證失敗: {str(e)}")
    
    # 上傳到 GCS
    buffer.seek(0)  # 重置 buffer 位置
    result_url = upload_chart_to_gcs(buffer, chart_name, year, season, compress=False)
    
    # ✅ 埋點 5: 記錄上傳結果
    log_to_gcs('INFO', f"📊 [{chart_name}] 上傳完成，URL 長度: {len(result_url)} 字元")
    
    return result_url


def create_stacked_bar_plot(df, bins, labels, category_name='category'):
    """通用的堆疊橫條圖資料處理"""
    df_copy = df.copy()
    df_copy[category_name] = pd.cut(df_copy['value'], bins=bins, labels=labels)
    pivot = pd.crosstab(df_copy['city_display'], df_copy[category_name],  normalize='index') * 100
    return pivot.reindex([zone for zone in ZONE_ORDER if zone in pivot.index])
    
# ==================== 圖表生成函數（Airflow DAG 使用）====================
def create_stacked_bar_charts(**context):
    """生成四種堆疊橫條圖：總價、建坪、建物類型、屋齡（Airflow 版本）"""
    log_to_gcs('INFO', "開始生成堆疊橫條圖...")
    
    ti = context['task_instance']
    df_used_json = ti.xcom_pull(task_ids='load_data', key='df_used')
    df_new_json = ti.xcom_pull(task_ids='load_data', key='df_new')
    year = ti.xcom_pull(task_ids='load_data', key='year')
    season = ti.xcom_pull(task_ids='load_data', key='season')
    
    df_used = pd.read_json(StringIO(df_used_json), orient='split')
    df_new = pd.read_json(StringIO(df_new_json), orient='split')
    
    # 檢查圖表是否已存在
    check_result = check_charts_exist(year, season)
    
    if check_result['exists']:
        log_to_gcs('INFO', f"圖表已存在，跳過生成，直接載入: {check_result['folder']}")
        # 載入現有圖表
        chart_urls = {}
        for chart_type in ['total_price', 'building_area', 'building_type', 'building_age']:
            chart_name = f"{chart_type}_stacked"
            if chart_name in check_result['charts']:
                from utils import get_chart_url_from_gcs
                chart_urls[chart_type] = get_chart_url_from_gcs(year, season, chart_name)
        
        ti.xcom_push(key='stacked_charts', value=chart_urls)
        return chart_urls
    
    # 生成新圖表
    chart_urls = {}
    
    # 1. 總價分級堆疊圖
    if not df_used.empty and not df_new.empty:
        chart_urls['total_price'] = _plot_total_price_distribution(df_used, df_new, year, season)
        chart_urls['building_area'] = _plot_building_area_distribution(df_used, df_new, year, season)
        chart_urls['building_type'] = _plot_building_type_distribution(df_used, df_new, year, season)
    
    # 2. 屋齡分級堆疊圖（僅中古屋）
    if not df_used.empty:
        chart_urls['building_age'] = _plot_building_age_distribution(df_used, year, season)
    
    ti.xcom_push(key='stacked_charts', value=chart_urls)
    log_to_gcs('INFO', "所有堆疊橫條圖已完成")
    return chart_urls

def _plot_total_price_distribution(df_used, df_new, year, season):
    """繪製總價分級堆疊圖"""
    log_to_gcs('INFO', "繪製總價分級堆疊圖...")
    
    # 處理資料
    df_used_copy = df_used.copy()
    df_new_copy = df_new.copy()
    
    df_used_copy['price_range'] = pd.cut(df_used_copy['total_price'], bins=PRICE_BINS_USED, labels=PRICE_LABELS_USED)
    df_new_copy['price_range'] = pd.cut(df_new_copy['total_price'], bins=PRICE_BINS_NEW, labels=PRICE_LABELS_NEW)
    
    pivot_used = pd.crosstab(df_used_copy['city_display'],  df_used_copy['price_range'], normalize='index') * 100
    pivot_new = pd.crosstab(df_new_copy['city_display'],  df_new_copy['price_range'], normalize='index') * 100
    
    pivot_used = pivot_used.reindex([z for z in ZONE_ORDER if z in pivot_used.index])
    pivot_new = pivot_new.reindex([z for z in ZONE_ORDER if z in pivot_new.index])
    
    # 繪製圖表
    fig, ax1, ax2 = create_dual_axis_figure()
    
    # 左側 - 中古屋
    pivot_used.plot(kind='barh', stacked=True, ax=ax1, 
                   color=COLORS[:len(pivot_used.columns)], 
                   width=0.7, legend=False)
    setup_stacked_bar_axis(ax1, '中古屋 - 總價比例分布', is_left=True)
    add_percentage_labels(ax1)
    
    # 右側 - 新成屋
    pivot_new.plot(kind='barh', stacked=True, ax=ax2, 
                  color=COLORS[:len(pivot_new.columns)], 
                  width=0.7, legend=False)
    setup_stacked_bar_axis(ax2, '新成屋 - 總價比例分布', is_left=False)
    add_percentage_labels(ax2)
    
    # 添加圖例
    add_dual_legends(fig, PRICE_LABELS_USED, PRICE_LABELS_NEW,
                    '中古屋價格區間', '新成屋價格區間')
    
    return save_and_upload('total_price_stacked', year, season)
    
def _plot_building_area_distribution(df_used, df_new, year, season):
    """繪製建坪分級堆疊圖"""
    log_to_gcs('INFO', "繪製建坪分級堆疊圖...")
    
    df_used_copy = df_used.copy()
    df_new_copy = df_new.copy()
    
    # 轉換為坪數
    df_used_copy['area_range'] = pd.cut(
        df_used_copy['building_total_sqm']/3.30579, 
        bins=AREA_BINS, labels=AREA_LABELS
    )
    df_new_copy['area_range'] = pd.cut(
        df_new_copy['building_total_sqm']/3.30579, 
        bins=AREA_BINS, labels=AREA_LABELS
    )
    
    pivot_used = pd.crosstab(df_used_copy['city_display'], df_used_copy['area_range'], normalize='index') * 100
    pivot_new = pd.crosstab(df_new_copy['city_display'], df_new_copy['area_range'], normalize='index') * 100
    
    pivot_used = pivot_used.reindex([z for z in ZONE_ORDER if z in pivot_used.index])
    pivot_new = pivot_new.reindex([z for z in ZONE_ORDER if z in pivot_new.index])
    
    # 繪製圖表
    fig, ax1, ax2 = create_dual_axis_figure()
    
    pivot_used.plot(kind='barh', stacked=True, ax=ax1, color=COLORS[:len(AREA_LABELS)], width=0.7, legend=False)
    setup_stacked_bar_axis(ax1, '中古屋 - 建坪比例分布', is_left=True)
    add_percentage_labels(ax1)
    
    pivot_new.plot(kind='barh', stacked=True, ax=ax2, color=COLORS[:len(AREA_LABELS)], width=0.7, legend=False)
    setup_stacked_bar_axis(ax2, '新成屋 - 建坪比例分布', is_left=False)
    add_percentage_labels(ax2)
    
    # 單一圖例（建坪級距相同）
    legend_ax = fig.add_axes(LEGEND_RIGHT)
    legend_ax.axis('off')
    handles = [plt.Rectangle((0,0),1,1, fc=COLORS[i]) 
              for i in range(len(AREA_LABELS))]
    legend_ax.legend(handles, AREA_LABELS, loc='center', title='建坪級距',frameon=True, fontsize=10, title_fontsize=11)
    
    return save_and_upload('building_area_stacked', year, season)
    
def _plot_building_type_distribution(df_used, df_new, year, season):
    """繪製建物類型堆疊圖"""
    log_to_gcs('INFO', "繪製建物類型堆疊圖...")
    
    pivot_used = pd.crosstab(df_used['city_display'], df_used['building_type'], normalize='index') * 100
    pivot_new = pd.crosstab(df_new['city_display'], df_new['building_type'], normalize='index') * 100
    
    all_types = [bt for bt in BUILDING_TYPE_ORDER 
                if bt in pivot_used.columns or bt in pivot_new.columns]
    
    pivot_used = pivot_used.reindex(columns=all_types, fill_value=0)
    pivot_new = pivot_new.reindex(columns=all_types, fill_value=0)
    pivot_used = pivot_used.reindex([z for z in ZONE_ORDER if z in pivot_used.index])
    pivot_new = pivot_new.reindex([z for z in ZONE_ORDER if z in pivot_new.index])
    
    # 繪製圖表
    fig, ax1, ax2 = create_dual_axis_figure()
    
    pivot_used.plot(kind='barh', stacked=True, ax=ax1, color=COLORS[:len(pivot_used.columns)], width=0.7, legend=False)
    setup_stacked_bar_axis(ax1, '中古屋 - 建物類型比例分布', is_left=True)
    add_percentage_labels(ax1)
    
    pivot_new.plot(kind='barh', stacked=True, ax=ax2, color=COLORS[:len(pivot_new.columns)], width=0.7, legend=False)
    setup_stacked_bar_axis(ax2, '新成屋 - 建物類型比例分布', is_left=False)
    add_percentage_labels(ax2)
    
    # 單一圖例
    legend_ax = fig.add_axes(LEGEND_RIGHT)
    legend_ax.axis('off')
    handles = [plt.Rectangle((0,0),1,1, fc=COLORS[i]) 
              for i, _ in enumerate(all_types) if i < len(COLORS)]
    legend_ax.legend(handles, all_types, loc='center', title='建物類型',frameon=True, fontsize=10, title_fontsize=11)
    
    return save_and_upload('building_type_stacked', year, season)

def _plot_building_age_distribution(df_used, year, season):
    """繪製屋齡分級堆疊圖（僅中古屋）"""
    log_to_gcs('INFO', "繪製屋齡分級堆疊圖...")
    
    df_used_copy = df_used.copy()
    df_used_copy['building_age'] = df_used_copy['transaction_year'] - df_used_copy['build_year']
    df_used_copy['age_range'] = pd.cut(df_used_copy['building_age'], bins=AGE_BINS, labels=AGE_LABELS)
    
    pivot = pd.crosstab(df_used_copy['city_display'], df_used_copy['age_range'], normalize='index') * 100
    pivot = pivot.reindex([z for z in ZONE_ORDER if z in pivot.index])
    
    fig = plt.figure(figsize=FIGURE_SIZE_MEDIUM)
    ax = fig.add_axes([0.15, 0.15, 0.65, 0.75])
    
    pivot.plot(kind='barh', stacked=True, ax=ax, color=COLORS[:len(AGE_LABELS)], width=0.7, legend=False)
    
    ax.set_xlabel('百分比 (%)', fontsize=12)
    ax.set_ylabel('區域', fontsize=12)
    ax.set_title('中古屋 - 屋齡比例分布', fontsize=14, pad=10, weight='bold')
    ax.set_xlim(0, 100)
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.invert_yaxis()
    
    add_percentage_labels(ax)
    
    legend_ax = fig.add_axes([0.82, 0.35, 0.08, 0.35])
    legend_ax.axis('off')
    handles = [plt.Rectangle((0,0),1,1, fc=COLORS[i]) 
              for i in range(len(AGE_LABELS))]
    legend_ax.legend(handles, AGE_LABELS, loc='center', title='屋齡級距',frameon=True, fontsize=10, title_fontsize=11)
    
    return save_and_upload('building_age_stacked', year, season)

def create_summary_section(**context):
    """生成統計摘要區塊（包含表格和成交件數堆疊圖）"""
    log_to_gcs('INFO', "生成統計摘要區塊...")
    
    ti = context['task_instance']
    
    df_used_json = ti.xcom_pull(task_ids='load_data', key='df_used')
    df_new_json = ti.xcom_pull(task_ids='load_data', key='df_new')
    year = ti.xcom_pull(task_ids='load_data', key='year')
    season = ti.xcom_pull(task_ids='load_data', key='season')
    
    df_used = pd.read_json(StringIO(df_used_json), orient='split')
    df_new = pd.read_json(StringIO(df_new_json), orient='split')
    
    # 檢查圖表是否已存在
    check_result = check_charts_exist(year, season)
    chart_name = 'transaction_count_stacked'
    
    if chart_name in check_result['charts']:
        log_to_gcs('INFO', f"成交件數圖已存在，直接載入")
        from utils import get_chart_url_from_gcs
        transaction_count_url = get_chart_url_from_gcs(year, season, chart_name)
    else:
        # 生成成交件數堆疊橫條圖
        log_to_gcs('INFO', "生成成交件數堆疊橫條圖...")
        transaction_count_url = _generate_transaction_count_chart(df_used, df_new, year, season)
    
    # 生成統計表格 HTML
    summary_data = []
    
    if not df_used.empty and 'city_display' in df_used.columns:
        for city in df_used['city_display'].unique():
            city_data = df_used[df_used['city_display'] == city]
            summary_data.append({
                'city': city,
                'type': '中古屋',
                'count': len(city_data),
                'avg_total_price': city_data['total_price'].mean() / 10000 if 'total_price' in city_data.columns else 0
            })
    
    if not df_new.empty and 'city_display' in df_new.columns:
        for city in df_new['city_display'].unique():
            city_data = df_new[df_new['city_display'] == city]
            summary_data.append({
                'city': city,
                'type': '新成屋',
                'count': len(city_data),
                'avg_total_price': city_data['total_price'].mean() / 10000 if 'total_price' in city_data.columns else 0
            })
    
    summary_df = pd.DataFrame(summary_data)
    
    html = '<table style="width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 14px;">'
    html += '<thead><tr style="background-color: #F3F4F6;">'
    html += '<th style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">縣市</th>'
    html += '<th style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">類型</th>'
    html += '<th style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">成交件數</th>'
    html += '<th style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">平均總價(萬)</th>'
    html += '</tr></thead><tbody>'
    
    for _, row in summary_df.iterrows():
        html += f'<tr><td style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">{row["city"]}</td>'
        html += f'<td style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">{row["type"]}</td>'
        html += f'<td style="border: 1px solid #D1D5DB; padding: 8px; text-align: right; color: #000000;">{row["count"]:,}</td>'
        html += f'<td style="border: 1px solid #D1D5DB; padding: 8px; text-align: right; color: #000000;">{row["avg_total_price"]:.1f}</td></tr>'
    
    html += '</tbody></table>'
    
    ti.xcom_push(key='transaction_count_url', value=transaction_count_url)
    ti.xcom_push(key='summary_table_html', value=html)
    
    log_to_gcs('INFO', "統計摘要區塊生成完成")
    return {'table': html, 'chart_url': transaction_count_url}

def _generate_transaction_count_chart(df_used, df_new, year, season):
    """生成成交件數圖"""
    used_counts = []
    new_counts = []
    
    for city in ZONE_ORDER:
        if city == '新竹市/竹北市':
            used_count = len(df_used[df_used['city_display'] == city]) if not df_used.empty else 0
            new_count = len(df_new[df_new['city_display'] == city]) if not df_new.empty else 0
        else:
            used_count = len(df_used[df_used['city'] == city]) if not df_used.empty else 0
            new_count = len(df_new[df_new['city'] == city]) if not df_new.empty else 0
        
        used_counts.append(used_count)
        new_counts.append(new_count)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    y_pos = range(len(ZONE_ORDER))
    
    ax.barh(y_pos, used_counts, label='中古屋', color='#3B82F6', alpha=0.8)
    ax.barh(y_pos, new_counts, left=used_counts, label='新成屋', color='#F59E0B', alpha=0.8)
    
    for i, (used, new) in enumerate(zip(used_counts, new_counts)):
        if used > 0:
            ax.text(used/2, i, f'{used:,}', ha='center', va='center', fontsize=10, weight='bold')
        if new > 0:
            ax.text(used + new/2, i, f'{new:,}', ha='center', va='center', fontsize=10, weight='bold')
        
        total = used + new
        if total > 0:
            ax.text(total + max(used_counts + new_counts) * 0.02, i, 
                   f'總計: {total:,}', ha='left', va='center', fontsize=9)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(ZONE_ORDER)
    ax.set_xlabel('成交件數', fontsize=12, weight='bold')
    ax.set_title('各縣市中古屋/新成屋成交件數分布', fontsize=14, pad=20, weight='bold')
    ax.legend(loc='lower right', fontsize=11)
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=CHART_DPI, bbox_inches='tight')
    plt.close()
    
    return upload_chart_to_gcs(buffer, 'transaction_count_stacked', year, season, compress=False)

def create_city_boxplots_combined(**context):
    """為每個縣市建立並排箱型圖"""
    log_to_gcs('INFO', "生成箱型圖...")
    
    ti = context['task_instance']
    
    df_used_json = ti.xcom_pull(task_ids='load_data', key='df_used')
    df_new_json = ti.xcom_pull(task_ids='load_data', key='df_new')
    year = ti.xcom_pull(task_ids='load_data', key='year')
    season = ti.xcom_pull(task_ids='load_data', key='season')
    
    df_used = pd.read_json(StringIO(df_used_json), orient='split')
    df_new = pd.read_json(StringIO(df_new_json), orient='split')
    
    # 檢查圖表是否已存在
    check_result = check_charts_exist(year, season)
    
    # 檢查是否所有箱型圖都存在
    required_boxplot_charts = [f"boxplot_{CITY_DISPLAY_TO_CODE[city]}_combined" 
                               for city, _ in REPORT_CITIES]
    all_boxplots_exist = all(chart in check_result['charts'] for chart in required_boxplot_charts)
    
    if all_boxplots_exist:
        log_to_gcs('INFO', "所有箱型圖已存在，直接載入")
        boxplot_urls = []
        from utils import get_chart_url_from_gcs
        
        for display_name, _ in REPORT_CITIES:
            chart_name = f"boxplot_{CITY_DISPLAY_TO_CODE[display_name]}_combined"
            url = get_chart_url_from_gcs(year, season, chart_name)
            boxplot_urls.append({
                'city': display_name,
                'url': url
            })
        
        ti.xcom_push(key='boxplot_urls', value=boxplot_urls)
        return boxplot_urls
    
    # 生成新圖表
    city_groups = {}
    
    for display_name, city_name in REPORT_CITIES:
        zones_used = df_used.loc[df_used['city'] == city_name, 'zip_zone'].unique().tolist() if not df_used.empty else []
        zones_new = df_new.loc[df_new['city'] == city_name, 'zip_zone'].unique().tolist() if not df_new.empty else []
        city_groups[display_name] = list(set(zones_used + zones_new))
    
    boxplot_urls = []
    
    for city_name, zones in city_groups.items():
        city_df_used = df_used[df_used['zip_zone'].isin(zones)].copy() if not df_used.empty else pd.DataFrame()
        city_df_new = df_new[df_new['zip_zone'].isin(zones)].copy() if not df_new.empty else pd.DataFrame()
        
        if len(city_df_used) == 0 and len(city_df_new) == 0:
            log_to_gcs('INFO', f"{city_name} 無資料，跳過")
            continue
        
        zone_counts_used = city_df_used['zip_zone'].value_counts() if not city_df_used.empty else pd.Series()
        zone_counts_new = city_df_new['zip_zone'].value_counts() if not city_df_new.empty else pd.Series()
        
        sorted_zones = sorted(zones, 
                            key=lambda x: (zone_counts_new.get(x, 0), zone_counts_used.get(x, 0)), 
                            reverse=True)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=FIGURE_SIZE_LARGE)
        
        def plot_boxplot(ax, city_df, sorted_zones, zone_counts, title_suffix):
            available_zones = [zone for zone in sorted_zones if zone in zone_counts.index]
            
            if len(available_zones) == 0:
                ax.text(0.5, 0.5, '無資料', ha='center', va='center', 
                       fontsize=20, transform=ax.transAxes)
                ax.set_title(f'{city_name}{title_suffix} 房價分布與成交量',
                           fontsize=16, pad=20, weight='bold')
                return
            
            city_df['zip_zone'] = pd.Categorical(city_df['zip_zone'],
                                                 categories=available_zones,
                                                 ordered=True)
            
            positions = range(len(available_zones))
            bp = ax.boxplot(
                [city_df[city_df['zip_zone'] == zone]['total_price'].values
                 for zone in available_zones],
                positions=positions,
                widths=0.6,
                patch_artist=True,
                showfliers=False,
                medianprops=dict(color='red', linewidth=2),
                boxprops=dict(facecolor='lightblue', alpha=0.7),
                whiskerprops=dict(linewidth=1.5),
                capprops=dict(linewidth=1.5)
            )
            
            ax.set_xticks(positions)
            ax.set_xticklabels(available_zones, rotation=45, ha='right')
            
            y_min_current, y_max_current = ax.get_ylim()
            
            all_min_values = []
            for zone in available_zones:
                zone_data = city_df[city_df['zip_zone'] == zone]['total_price'].values
                if len(zone_data) > 0:
                    all_min_values.append(zone_data.min())
            
            if all_min_values:
                global_min = min(all_min_values)
                y_range = y_max_current - y_min_current
                label_space = y_range * 0.08
                new_y_min = min(global_min - label_space, y_min_current)
                ax.set_ylim(new_y_min, y_max_current)
                
                for i, zone in enumerate(available_zones):
                    count = zone_counts[zone]
                    zone_data = city_df[city_df['zip_zone'] == zone]['total_price'].values
                    if len(zone_data) > 0:
                        zone_min = zone_data.min()
                        y_pos = (zone_min + new_y_min) / 2
                        
                        ax.text(i, y_pos, f'n={count}',
                               ha='center', va='center', fontsize=9,
                               bbox=dict(boxstyle='round,pad=0.3',
                                       facecolor='yellow', alpha=0.5))
            
            ax.yaxis.set_major_formatter(
                plt.FuncFormatter(lambda x, p: f'{int(x/10000)}萬'))
            
            ax.set_xlabel('區域', fontsize=12, weight='bold')
            ax.set_ylabel('總價 (萬元)', fontsize=12, weight='bold')
            ax.set_title(f'{city_name}{title_suffix} 房價分布與成交量',
                        fontsize=16, pad=20, weight='bold')
            
            ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            total_transactions = len(city_df[city_df['zip_zone'].isin(available_zones)])
            median_price = city_df[city_df['zip_zone'].isin(available_zones)]['total_price'].median()
            mean_price = city_df[city_df['zip_zone'].isin(available_zones)]['total_price'].mean()
            
            stats_text = (f'總成交數: {total_transactions:,}筆\n'
                         f'中位數: {median_price/10000:.0f}萬\n'
                         f'平均數: {mean_price/10000:.0f}萬')
            
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                   fontsize=10, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        plot_boxplot(ax1, city_df_used, sorted_zones, zone_counts_used, '中古屋')
        plot_boxplot(ax2, city_df_new, sorted_zones, zone_counts_new, '新成屋')
        
        plt.tight_layout()
        
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=CHART_DPI, bbox_inches='tight')
        plt.close()
        
        chart_url = upload_chart_to_gcs(buffer, f'boxplot_{CITY_DISPLAY_TO_CODE[city_name]}_combined', year, season, compress=False)
        boxplot_urls.append({
            'city': city_name,
            'url': chart_url
        })
        
        log_to_gcs('INFO', f"{city_name} 並排箱型圖已完成")
    
    ti.xcom_push(key='boxplot_urls', value=boxplot_urls)
    
    log_to_gcs('INFO', "所有並排箱型圖已完成")
    return boxplot_urls

def convert_to_web_mercator(df):
    """將 WGS84 轉換為 Web Mercator"""
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    x, y = transformer.transform(df['longitude'].values, df['latitude'].values)
    df = df.copy()
    df['x'] = x
    df['y'] = y
    return df


def classify_price_quantile(df, price_col='total_price'):
    """依據總價四分位距分類"""
    price_q1 = df[price_col].quantile(0.25)
    price_q2 = df[price_col].quantile(0.50)
    price_q3 = df[price_col].quantile(0.75)
    price_max = df[price_col].max()
    price_min = df[price_col].min()
    
    def classify(price):
        if pd.isna(price):
            return '未知', 0
        if price <= price_q1:
            return f'{price_min/10000:,.0f}~{price_q1/10000:,.0f}萬', 1
        elif price <= price_q2:
            return f'{price_q1/10000:,.0f}~{price_q2/10000:,.0f}萬', 2
        elif price <= price_q3:
            return f'{price_q2/10000:,.0f}~{price_q3/10000:,.0f}萬', 3
        else:
            return f'{price_q3/10000:,.0f}~{price_max/10000:,.0f}萬', 4
    
    result = df[price_col].apply(lambda x: pd.Series(classify(x)))
    result.columns = ['price_category', 'price_order']
    
    return result, (price_min, price_q1, price_q2, price_q3, price_max)


def classify_size_quantile(df, size_col='building_total_sqm'):
    """依據坪數四分位距分類"""
    size_q1 = df[size_col].quantile(0.25)
    size_q2 = df[size_col].quantile(0.50)
    size_q3 = df[size_col].quantile(0.75)
    size_max = df[size_col].max()
    size_min = df[size_col].min()
    
    def classify(sqm):
        if pd.isna(sqm):
            return '未知', 0
        if sqm <= size_q1:
            return f'{size_min:.1f}~{size_q1:.1f}坪', 1
        elif sqm <= size_q2:
            return f'{size_q1:.1f}~{size_q2:.1f}坪', 2
        elif sqm <= size_q3:
            return f'{size_q2:.1f}~{size_q3:.1f}坪', 3
        else:
            return f'{size_q3:.1f}~{size_max:.1f}坪', 4
    
    result = df[size_col].apply(lambda x: pd.Series(classify(x)))
    result.columns = ['size_category', 'size_order']
    
    return result, (size_min, size_q1, size_q2, size_q3, size_max)


def create_city_scatter_map(df_city, city_name, property_type, year, season):
    """建立單一縣市的散點圖"""
    
    if len(df_city) == 0:
        log_to_gcs('WARNING', f"{city_name} {property_type} 沒有資料")
        return None
    
    log_to_gcs('INFO', f"處理 {city_name} {property_type} {len(df_city)} 筆資料...")
    
    # 過濾異常座標
    if city_name in CITY_BOUNDS:
        bounds = CITY_BOUNDS[city_name]
        original_len = len(df_city)
        
        df_city = df_city[
            (df_city['latitude'] >= bounds['lat'][0]) &
            (df_city['latitude'] <= bounds['lat'][1]) &
            (df_city['longitude'] >= bounds['lon'][0]) &
            (df_city['longitude'] <= bounds['lon'][1])
        ].copy()
        
        filtered_count = original_len - len(df_city)
        if filtered_count > 0:
            log_to_gcs('INFO', f"過濾 {filtered_count} 筆異常座標 ({filtered_count/original_len*100:.1f}%)")
    
    if len(df_city) == 0:
        log_to_gcs('WARNING', f"{city_name} {property_type} 過濾後無資料")
        return None

    # 記錄原始資料筆數（用於標題註記）
    original_data_count = len(df_city)
    is_sampled = False
    
    # 如果資料量太大，進行採樣
    if len(df_city) > MAX_POINTS:
        log_to_gcs('INFO', f"資料量 {len(df_city)} 超過 {MAX_POINTS}，進行採樣...")
        df_city = df_city.sample(n=MAX_POINTS, random_state=42)
        is_sampled = True

    # 座標轉換
    df_city = convert_to_web_mercator(df_city.copy())
    
    # 總價分類
    price_result, price_stats = classify_price_quantile(df_city)
    df_city[['price_category', 'price_order']] = price_result
    
    # 坪數分類
    size_result, size_stats = classify_size_quantile(df_city)
    df_city[['size_category', 'size_order']] = size_result
    
    log_to_gcs('INFO', f"{city_name} {property_type} 總價四分位距: {[f'{x/10000:.0f}萬' for x in price_stats]}")
    log_to_gcs('INFO', f"{city_name} {property_type} 坪數四分位距: {[f'{x:.1f}坪' for x in size_stats]}")
    
    # 建立圖表
    fig, ax = plt.subplots(figsize=FIGURE_SIZE_MAP, dpi=80)
    
    # 設定顯示範圍
    x_margin = (df_city['x'].max() - df_city['x'].min()) * 0.1
    y_margin = (df_city['y'].max() - df_city['y'].min()) * 0.1
    
    ax.set_xlim(df_city['x'].min() - x_margin, df_city['x'].max() + x_margin)
    ax.set_ylim(df_city['y'].min() - y_margin, df_city['y'].max() + y_margin)
    
    # 加入地圖底圖
    log_to_gcs('INFO', f"正在載入 {city_name} 地圖底圖...")
    try:
        cx.add_basemap(
            ax,
            crs='EPSG:3857',
            source=cx.providers.OpenStreetMap.Mapnik,
            zoom='auto',
            alpha=0.85,
            zorder=1
        )
        log_to_gcs('INFO', "地圖底圖載入成功")
    except Exception as e:
        log_to_gcs('WARNING', f"地圖底圖載入失敗: {str(e)}")
    
    # 繪製散點圖（從高價到低價，讓低價在最上層）
    for price_order in [4, 3, 2, 1]:  # 反向繪製，讓低價（深色小點）在上層
        df_subset = df_city[df_city['price_order'] == price_order]
        
        if len(df_subset) > 0:
            # 取得該分類的顏色和大小
            colors = [PRICE_COLORS[price_order]] * len(df_subset)
            sizes = df_subset['size_order'].map(SIZE_MAPPING).fillna(100)
            
            # 取得分類標籤
            category_label = df_subset['price_category'].iloc[0]
            
            ax.scatter(
                df_subset['x'],
                df_subset['y'],
                c=colors,
                s=sizes,
                alpha=0.5,
                edgecolors='white',
                linewidth=1,
                zorder=2 + (5 - price_order),  # 低價 zorder 更高
                label=f'{category_label} ({len(df_subset)}筆)'
            )
    
    # 設定主標題
    ax.set_title(
        f'{city_name} {property_type} 分布圖(依總價與坪數四分位距分類)',fontsize=22,fontweight='bold',pad=30
    )
    
    # 添加資料筆數說明（副標題位置）
    if is_sampled:
        subtitle_text = f'繪圖限制，從 {original_data_count:,} 筆數據中隨機採樣 {MAX_POINTS:,} 筆製圖'
    else:
        subtitle_text = f'資料筆數: {len(df_city):,}'
    
    # 在標題下方添加副標題
    ax.text(0.5, 1.01, subtitle_text,ha='center', va='bottom', transform=ax.transAxes,fontsize=11, color='#333333',linespacing=1.5)
    
    # 創建圖例 - 總價（四分位距，從低到高排列）
    price_categories = df_city[df_city['price_order'] > 0].sort_values('price_order')['price_category'].unique()
    price_legend_elements = [
        Patch(facecolor=PRICE_COLORS[i+1], edgecolor='white',
              label=cat, alpha=0.75)
        for i, cat in enumerate(price_categories)
    ]
    
    legend1 = ax.legend(
        handles=price_legend_elements,
        title='總價區間(四分位距)\n深色=低價，淺色=高價',
        loc='upper left',
        fontsize=10,
        title_fontsize=10,
        framealpha=0.95,
        edgecolor='black'
    )
    ax.add_artist(legend1)
    
    # 創建圖例 - 坪數（四分位距）
    size_categories = df_city[df_city['size_order'] > 0].sort_values('size_order')['size_category'].unique()
    size_legend_elements = [
        ax.scatter([], [], s=SIZE_MAPPING[i+1], c='#808080',
                  alpha=0.75, edgecolors='white', linewidth=1,
                  label=cat)
        for i, cat in enumerate(size_categories)
    ]
    
    legend2 = ax.legend(
        handles=size_legend_elements,
        title='房屋坪數(四分位距)',
        loc='upper right',
        fontsize=10,
        title_fontsize=11,
        framealpha=0.95,
        edgecolor='black'
    )
    
    # 移除座標軸刻度
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlabel('')
    ax.set_ylabel('')
    
    # 調整佈局
    plt.tight_layout()
    
    # 儲存圖片
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=80, bbox_inches='tight', facecolor='white')
    plt.close()
    
    log_to_gcs('INFO', f"{city_name} {property_type} 散點圖已完成" + (f" (採樣: {original_data_count:,} -> {MAX_POINTS:,})" if is_sampled else ""))
    
    return buffer

def create_heat_maps(**context):
    """生成各縣市區域熱度地圖"""
    log_to_gcs('INFO', "開始生成區域熱度地圖...")
    
    ti = context['task_instance']
    
    df_used_json = ti.xcom_pull(task_ids='load_data', key='df_used')
    df_new_json = ti.xcom_pull(task_ids='load_data', key='df_new')
    year = ti.xcom_pull(task_ids='load_data', key='year')
    season = ti.xcom_pull(task_ids='load_data', key='season')
    
    df_used = pd.read_json(StringIO(df_used_json), orient='split')
    df_new = pd.read_json(StringIO(df_new_json), orient='split')
    
    # 確保有經緯度資料
    if not df_used.empty:
        df_used = df_used.dropna(subset=['latitude', 'longitude'])
    if not df_new.empty:
        df_new = df_new.dropna(subset=['latitude', 'longitude'])
    
    log_to_gcs('INFO', f"中古屋有效座標資料: {len(df_used)} 筆")
    log_to_gcs('INFO', f"新成屋有效座標資料: {len(df_new)} 筆")
    
    # 檢查圖表是否已存在
    check_result = check_charts_exist(year, season)
    
    # 檢查是否所有熱度地圖都存在
    required_heatmap_charts = []
    for display_name, _ in REPORT_CITIES:
        code = CITY_DISPLAY_TO_CODE[display_name]
        required_heatmap_charts.append(f"heatmap_{code}_used")
        required_heatmap_charts.append(f"heatmap_{code}_presale")
    
    all_heatmaps_exist = all(chart in check_result['charts'] for chart in required_heatmap_charts)
    
    if all_heatmaps_exist:
        log_to_gcs('INFO', "所有熱度地圖已存在，直接載入")
        heat_map_urls = []
        from utils import get_chart_url_from_gcs
        
        for display_name, _ in REPORT_CITIES:
            code = CITY_DISPLAY_TO_CODE[display_name]
            for map_type in ['used', 'presale']:
                chart_name = f"heatmap_{code}_{map_type}"
                try:
                    url = get_chart_url_from_gcs(year, season, chart_name)
                    heat_map_urls.append({
                        'city': display_name,
                        'type': '中古屋' if map_type == 'used' else '新成屋',
                        'url': url
                    })
                except FileNotFoundError:
                    log_to_gcs('WARNING', f"找不到 {display_name} {map_type} 的熱度地圖")
        
        ti.xcom_push(key='heat_map_urls', value=heat_map_urls)
        return heat_map_urls
    
    # 生成新圖表
    heat_map_urls = []
    
    for display_name, city_name in REPORT_CITIES:
        # 中古屋地圖
        city_df_used = df_used[df_used['city'] == city_name].copy() if not df_used.empty else pd.DataFrame()
        if len(city_df_used) > 0:
            buffer = create_city_scatter_map(city_df_used, display_name, '中古屋', year, season)
            if buffer:
                # 修正：buffer 已經包含圖片資料，直接上傳
                chart_url = upload_chart_to_gcs(
                    buffer, 
                    f'heatmap_{CITY_DISPLAY_TO_CODE[display_name]}_used', 
                    year, 
                    season, 
                    compress=False
                )
                heat_map_urls.append({
                    'city': display_name,
                    'type': '中古屋',
                    'url': chart_url
                })
        
        # 新成屋地圖
        city_df_new = df_new[df_new['city'] == city_name].copy() if not df_new.empty else pd.DataFrame()
        if len(city_df_new) > 0:
            buffer = create_city_scatter_map(city_df_new, display_name, '新成屋', year, season)
            if buffer:
                # 修正：buffer 已經包含圖片資料，直接上傳
                chart_url = upload_chart_to_gcs(
                    buffer, 
                    f'heatmap_{CITY_DISPLAY_TO_CODE[display_name]}_presale', 
                    year, 
                    season, 
                    compress=False
                )
                heat_map_urls.append({
                    'city': display_name,
                    'type': '新成屋',
                    'url': chart_url
                })
    
    ti.xcom_push(key='heat_map_urls', value=heat_map_urls)
    
    log_to_gcs('INFO', f"所有區域熱度地圖已完成，共 {len(heat_map_urls)} 張")
    return heat_map_urls

# ==================== Standalone 函數（API 使用）====================

def create_stacked_bar_charts_standalone(df_used, df_new, year, season):
    """生成堆疊橫條圖（不依賴 Airflow）"""
    # 檢查圖表是否已存在
    check_result = check_charts_exist(year, season)
    
    if check_result['exists']:
        log_to_gcs('INFO', f"圖表已存在，直接載入: {check_result['folder']}")
        chart_urls = {}
        for chart_type in ['total_price', 'building_area', 'building_type', 'building_age']:
            chart_name = f"{chart_type}_stacked"
            if chart_name in check_result['charts']:
                from utils import get_chart_url_from_gcs
                chart_urls[chart_type] = get_chart_url_from_gcs(year, season, chart_name)
        return chart_urls
    
    # 生成新圖表
    chart_urls = {}
    
    if not df_used.empty and not df_new.empty:
        chart_urls['total_price'] = _plot_total_price_distribution(df_used, df_new, year, season)
        chart_urls['building_area'] = _plot_building_area_distribution(df_used, df_new, year, season)
        chart_urls['building_type'] = _plot_building_type_distribution(df_used, df_new, year, season)
    
    if not df_used.empty:
        chart_urls['building_age'] = _plot_building_age_distribution(df_used, year, season)
    
    return chart_urls


def create_summary_section_standalone(df_used, df_new, year, season):
    """生成統計摘要（不依賴 Airflow）"""
    # 檢查圖表是否已存在
    check_result = check_charts_exist(year, season)
    chart_name = 'transaction_count_stacked'
    
    if chart_name in check_result['charts']:
        log_to_gcs('INFO', f"成交件數圖已存在，直接載入")
        from utils import get_chart_url_from_gcs
        transaction_count_url = get_chart_url_from_gcs(year, season, chart_name)
    else:
        transaction_count_url = _generate_transaction_count_chart(df_used, df_new, year, season)
    
    # 生成統計表格 HTML
    summary_data = []
    
    if not df_used.empty and 'city_display' in df_used.columns:
        for city in df_used['city_display'].unique():
            city_data = df_used[df_used['city_display'] == city]
            summary_data.append({
                'city': city,
                'type': '中古屋',
                'count': len(city_data),
                'avg_total_price': city_data['total_price'].mean() / 10000 if 'total_price' in city_data.columns else 0
            })
    
    if not df_new.empty and 'city_display' in df_new.columns:
        for city in df_new['city_display'].unique():
            city_data = df_new[df_new['city_display'] == city]
            summary_data.append({
                'city': city,
                'type': '新成屋',
                'count': len(city_data),
                'avg_total_price': city_data['total_price'].mean() / 10000 if 'total_price' in city_data.columns else 0
            })
    
    summary_df = pd.DataFrame(summary_data)
    
    html = '<table style="width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 14px;">'
    html += '<thead><tr style="background-color: #F3F4F6;">'
    html += '<th style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">縣市</th>'
    html += '<th style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">類型</th>'
    html += '<th style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">成交件數</th>'
    html += '<th style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">平均總價(萬)</th>'
    html += '</tr></thead><tbody>'
    
    for _, row in summary_df.iterrows():
        html += f'<tr><td style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">{row["city"]}</td>'
        html += f'<td style="border: 1px solid #D1D5DB; padding: 8px; color: #000000;">{row["type"]}</td>'
        html += f'<td style="border: 1px solid #D1D5DB; padding: 8px; text-align: right; color: #000000;">{row["count"]:,}</td>'
        html += f'<td style="border: 1px solid #D1D5DB; padding: 8px; text-align: right; color: #000000;">{row["avg_total_price"]:.1f}</td></tr>'
    
    html += '</tbody></table>'
    
    return {
        'transaction_count_url': transaction_count_url,
        'table_html': html
    }


def create_city_boxplots_combined_standalone(df_used, df_new, year, season):
    """生成箱型圖（不依賴 Airflow）"""
    # 檢查圖表是否已存在
    check_result = check_charts_exist(year, season)
    required_boxplot_charts = [f"boxplot_{CITY_DISPLAY_TO_CODE[city]}_combined" 
                               for city, _ in REPORT_CITIES]
    all_boxplots_exist = all(chart in check_result['charts'] for chart in required_boxplot_charts)
    
    if all_boxplots_exist:
        log_to_gcs('INFO', "所有箱型圖已存在，直接載入")
        boxplot_urls = []
        from utils import get_chart_url_from_gcs
        
        for display_name, _ in REPORT_CITIES:
            chart_name = f"boxplot_{CITY_DISPLAY_TO_CODE[display_name]}_combined"
            url = get_chart_url_from_gcs(year, season, chart_name)
            boxplot_urls.append({
                'city': display_name,
                'url': url
            })
        return boxplot_urls
    
    # 生成新圖表
    city_groups = {}
    
    for display_name, city_name in REPORT_CITIES:
        zones_used = df_used.loc[df_used['city'] == city_name, 'zip_zone'].unique().tolist() if not df_used.empty else []
        zones_new = df_new.loc[df_new['city'] == city_name, 'zip_zone'].unique().tolist() if not df_new.empty else []
        city_groups[display_name] = list(set(zones_used + zones_new))
    
    boxplot_urls = []
    
    for city_name, zones in city_groups.items():
        city_df_used = df_used[df_used['zip_zone'].isin(zones)].copy() if not df_used.empty else pd.DataFrame()
        city_df_new = df_new[df_new['zip_zone'].isin(zones)].copy() if not df_new.empty else pd.DataFrame()
        
        if len(city_df_used) == 0 and len(city_df_new) == 0:
            log_to_gcs('INFO', f"{city_name} 無資料，跳過")
            continue
        
        zone_counts_used = city_df_used['zip_zone'].value_counts() if not city_df_used.empty else pd.Series()
        zone_counts_new = city_df_new['zip_zone'].value_counts() if not city_df_new.empty else pd.Series()
        
        sorted_zones = sorted(zones, 
                            key=lambda x: (zone_counts_new.get(x, 0), zone_counts_used.get(x, 0)), 
                            reverse=True)
        
        # 繪圖邏輯
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=FIGURE_SIZE_LARGE)
        
        def plot_boxplot(ax, city_df, sorted_zones, zone_counts, title_suffix):
            available_zones = [zone for zone in sorted_zones if zone in zone_counts.index]
            
            if len(available_zones) == 0:
                ax.text(0.5, 0.5, '無資料', ha='center', va='center', 
                       fontsize=20, transform=ax.transAxes)
                ax.set_title(f'{city_name}{title_suffix} 房價分布與成交量',
                           fontsize=16, pad=20, weight='bold')
                return
            
            city_df['zip_zone'] = pd.Categorical(city_df['zip_zone'],
                                                 categories=available_zones,
                                                 ordered=True)
            
            positions = range(len(available_zones))
            bp = ax.boxplot(
                [city_df[city_df['zip_zone'] == zone]['total_price'].values
                 for zone in available_zones],
                positions=positions,
                widths=0.6,
                patch_artist=True,
                showfliers=False,
                medianprops=dict(color='red', linewidth=2),
                boxprops=dict(facecolor='lightblue', alpha=0.7),
                whiskerprops=dict(linewidth=1.5),
                capprops=dict(linewidth=1.5)
            )
            
            ax.set_xticks(positions)
            ax.set_xticklabels(available_zones, rotation=45, ha='right')
            
            y_min_current, y_max_current = ax.get_ylim()
            
            all_min_values = []
            for zone in available_zones:
                zone_data = city_df[city_df['zip_zone'] == zone]['total_price'].values
                if len(zone_data) > 0:
                    all_min_values.append(zone_data.min())
            
            if all_min_values:
                global_min = min(all_min_values)
                y_range = y_max_current - y_min_current
                label_space = y_range * 0.08
                new_y_min = min(global_min - label_space, y_min_current)
                ax.set_ylim(new_y_min, y_max_current)
                
                for i, zone in enumerate(available_zones):
                    count = zone_counts[zone]
                    zone_data = city_df[city_df['zip_zone'] == zone]['total_price'].values
                    if len(zone_data) > 0:
                        zone_min = zone_data.min()
                        y_pos = (zone_min + new_y_min) / 2
                        
                        ax.text(i, y_pos, f'n={count}',
                               ha='center', va='center', fontsize=9,
                               bbox=dict(boxstyle='round,pad=0.3',
                                       facecolor='yellow', alpha=0.5))
            
            ax.yaxis.set_major_formatter(
                plt.FuncFormatter(lambda x, p: f'{int(x/10000)}萬'))
            
            ax.set_xlabel('區域', fontsize=12, weight='bold')
            ax.set_ylabel('總價 (萬元)', fontsize=12, weight='bold')
            ax.set_title(f'{city_name}{title_suffix} 房價分布與成交量',
                        fontsize=16, pad=20, weight='bold')
            
            ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            total_transactions = len(city_df[city_df['zip_zone'].isin(available_zones)])
            median_price = city_df[city_df['zip_zone'].isin(available_zones)]['total_price'].median()
            mean_price = city_df[city_df['zip_zone'].isin(available_zones)]['total_price'].mean()
            
            stats_text = (f'總成交數: {total_transactions:,}筆\n'
                         f'中位數: {median_price/10000:.0f}萬\n'
                         f'平均數: {mean_price/10000:.0f}萬')
            
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                   fontsize=10, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        plot_boxplot(ax1, city_df_used, sorted_zones, zone_counts_used, '中古屋')
        plot_boxplot(ax2, city_df_new, sorted_zones, zone_counts_new, '新成屋')
        
        plt.tight_layout()
        
        # 儲存並上傳
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=CHART_DPI, bbox_inches='tight')
        plt.close()
        
        chart_url = upload_chart_to_gcs(
            buffer, 
            f'boxplot_{CITY_DISPLAY_TO_CODE[city_name]}_combined', 
            year, 
            season, 
            compress=False
        )
        boxplot_urls.append({
            'city': city_name,
            'url': chart_url
        })
        
        log_to_gcs('INFO', f"{city_name} 並排箱型圖已完成")
    
    return boxplot_urls


def create_heat_maps_standalone(df_used, df_new, year, season):
    """生成熱度地圖（不依賴 Airflow）"""
    # 檢查圖表是否已存在
    check_result = check_charts_exist(year, season)
    required_heatmap_charts = []
    for display_name, _ in REPORT_CITIES:
        code = CITY_DISPLAY_TO_CODE[display_name]
        required_heatmap_charts.append(f"heatmap_{code}_used")
        required_heatmap_charts.append(f"heatmap_{code}_presale")
    
    all_heatmaps_exist = all(chart in check_result['charts'] for chart in required_heatmap_charts)
    
    if all_heatmaps_exist:
        log_to_gcs('INFO', "所有熱度地圖已存在，直接載入")
        heat_map_urls = []
        from utils import get_chart_url_from_gcs
        
        for display_name, _ in REPORT_CITIES:
            code = CITY_DISPLAY_TO_CODE[display_name]
            for map_type in ['used', 'presale']:
                chart_name = f"heatmap_{code}_{map_type}"
                try:
                    url = get_chart_url_from_gcs(year, season, chart_name)
                    heat_map_urls.append({
                        'city': display_name,
                        'type': '中古屋' if map_type == 'used' else '新成屋',
                        'url': url
                    })
                except FileNotFoundError:
                    log_to_gcs('WARNING', f"找不到 {display_name} {map_type} 的熱度地圖")
        
        return heat_map_urls
    
    # 生成新圖表
    heat_map_urls = []
    
    # 確保有經緯度資料
    if not df_used.empty:
        df_used = df_used.dropna(subset=['latitude', 'longitude'])
    if not df_new.empty:
        df_new = df_new.dropna(subset=['latitude', 'longitude'])
    
    for display_name, city_name in REPORT_CITIES:
        # 中古屋地圖
        city_df_used = df_used[df_used['city'] == city_name].copy() if not df_used.empty else pd.DataFrame()
        if len(city_df_used) > 0:
            buffer = create_city_scatter_map(city_df_used, display_name, '中古屋', year, season)
            if buffer:
                # 修正
                chart_url = upload_chart_to_gcs(
                    buffer, 
                    f'heatmap_{CITY_DISPLAY_TO_CODE[display_name]}_used', 
                    year, 
                    season, 
                    compress=False
                )
                heat_map_urls.append({
                    'city': display_name,
                    'type': '中古屋',
                    'url': chart_url
                })
        
        # 新成屋地圖
        city_df_new = df_new[df_new['city'] == city_name].copy() if not df_new.empty else pd.DataFrame()
        if len(city_df_new) > 0:
            buffer = create_city_scatter_map(city_df_new, display_name, '新成屋', year, season)
            if buffer:
                # 修正
                chart_url = upload_chart_to_gcs(
                    buffer, 
                    f'heatmap_{CITY_DISPLAY_TO_CODE[display_name]}_presale', 
                    year, 
                    season, 
                    compress=False
                )
                heat_map_urls.append({
                    'city': display_name,
                    'type': '新成屋',
                    'url': chart_url
                })
    
    return heat_map_urls