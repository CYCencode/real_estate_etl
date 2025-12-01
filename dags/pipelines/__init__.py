"""Pipeline 模組 - 地址轉換、視覺化與報告生成"""

# HERE Geocoding
from pipelines.here_geocoding import (
    geocode_address,
    check_and_add_columns,
    process_table_geocoding
)

# Google Geocoding
from pipelines.google_geocoding import (
    geocode_address_google,
    check_and_add_columns_google,
    process_table_google_geocoding
)

# Visualization - Airflow 版本
from pipelines.visualization import (
    create_stacked_bar_charts,
    create_summary_section,
    create_city_boxplots_combined,
    create_heat_maps,
    create_stacked_bar_charts_standalone,
    create_summary_section_standalone,
    create_city_boxplots_combined_standalone,
    create_heat_maps_standalone,
)

# Reporting - Standalone (for API)
from pipelines.reporting import (
    generate_report_content,
    send_email_to_recipients,
)

# Reporting - Airflow (for DAG)
from pipelines.reporting import (
    generate_html_report,
    send_report_email,
)

__all__ = [
    # HERE Geocoding
    'geocode_address',
    'check_and_add_columns',
    'process_table_geocoding',
    
    # Google Geocoding
    'geocode_address_google',
    'check_and_add_columns_google',
    'process_table_google_geocoding',
    
    # Visualization - Airflow
    'create_stacked_bar_charts',
    'create_summary_section',
    'create_city_boxplots_combined',
    'create_heat_maps',
    
    # Visualization - Standalone (for API)
    'create_stacked_bar_charts_standalone',
    'create_summary_section_standalone',
    'create_city_boxplots_combined_standalone',
    'create_heat_maps_standalone',
    
    # Reporting - Standalone (for API)
    'generate_report_content',
    'send_email_to_recipients',
    
    # Reporting - Airflow (for DAG)
    'generate_html_report',
    'send_report_email',
]