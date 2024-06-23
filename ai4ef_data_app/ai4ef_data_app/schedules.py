from dagster import (
    ScheduleDefinition, 
    RunRequest, 
    ScheduleEvaluationContext, 
    schedule
)
from .jobs import latvian_meteo_job, pilot_data_job
from datetime import datetime, timedelta

# we do this to change the current date at each run it scedules 
@schedule(job=latvian_meteo_job, cron_schedule="0 6 * * *") # every 24 hours at 6 am
def latvian_meteo_job_schedule(context: ScheduleEvaluationContext):

    scheduled_date = context.scheduled_execution_time.strftime("%d.%m.%Y")
    print(f'Scheduled date: {scheduled_date}')
    
    return RunRequest(
        run_key=None,
        run_config={
            "resources": {
                "meteo_config": {
                    "config": {
                        "global_url": "https://videscentrs.lvgmc.lv/media/reports/station-report.xls",
                        "stacija_id": 30004,
                        "raditaja_id": 4001,
                        "initial_date": "31.05.2023",
                        "current_date": scheduled_date,
                    }
                },
                "db_config": {
                    "config": {
                        "schema_name": "latvian_meteo",
                        "store2db": True,
                        "table_name": "ainazi_atmosferas_spiediens"
                    }
                },
                "csv_io_manager": {
                    "config": {
                        "path_prefix": "storage_data/"
                    }
                },
            },
        },
        tags={"date": scheduled_date},
    )

# we do this to change the current date at each run it scedules 
@schedule(job=pilot_data_job, cron_schedule="0 6 * * *") # every 24 hours at 6 am
def pilot_data_job_schedule(context: ScheduleEvaluationContext):

    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    
    return RunRequest(
        run_key=None,
        run_config={
            "resources": {
                "config": {
                    "config": {
                        "pilot": "pilot4",
                        'endpoint':  "desfa_nominations_daily",
                        'store2db': True,
                        'current_date': scheduled_date,        # This is the current date
                    }
                },
                "csv_io_manager": {
                    "config": {
                        "path_prefix": "storage_data/"
                    }
                },
            },
        },
        tags={"date": scheduled_date},
    )