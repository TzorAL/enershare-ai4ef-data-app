from dagster import Definitions
from .resources.config import WeatherMeteo, DBConfig, DSConfig
from .assets.meteo_assets import latvian_meteo_op
from .assets.ds_assets import get_pilot_data, get_all_pilot_data
from .jobs import latvian_meteo_job, pilot_data_job
from .schedules import latvian_meteo_job_schedule, pilot_data_job_schedule
from .resources.csv_io_manager import CsvIOManager

defs = Definitions(
    assets=[latvian_meteo_op, get_pilot_data, get_all_pilot_data],
    jobs=[latvian_meteo_job, pilot_data_job],
    resources={
        "meteo_config": WeatherMeteo(),
        'db_config': DBConfig(),
        'ds_config': DSConfig(),
        "csv_io_manager": CsvIOManager(path_prefix="storage_data/")
    },
    schedules=[latvian_meteo_job_schedule, pilot_data_job_schedule]
)