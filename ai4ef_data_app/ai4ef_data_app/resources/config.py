from dagster import ConfigurableResource
from datetime import datetime

class WeatherMeteo(ConfigurableResource):
    global_url: str = "https://videscentrs.lvgmc.lv/media/reports/station-report.xls"
    stacija_id: int = 30004
    raditaja_id: int = 4001                 
    initial_date: str = '31.05.2023'        # This is the first date with data 
    current_date: str = datetime.now().strftime("%d.%m.%Y")        # This is the current date

class DBConfig(ConfigurableResource):
    schema_name: str = "latvian_meteo"
    table_name: str = "ainazi_atmosferas_spiediens"
    store2db: bool = True

class DSConfig(ConfigurableResource):
    pilot: str = "Pilot7"
    endpoint: str = "efcomp"
    store2db: bool = True
    current_date: str = datetime.now().strftime("%d.%m.%Y")        # This is the current date
