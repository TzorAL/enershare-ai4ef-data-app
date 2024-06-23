from dagster import define_asset_job
from .assets.meteo_assets import latvian_meteo_op
from .assets.ds_assets import get_all_pilot_data

latvian_meteo_job = define_asset_job(name="latvian_meteo_job", selection=[latvian_meteo_op])

pilot_data_job = define_asset_job(name="pilot_data_job", selection=[get_all_pilot_data])