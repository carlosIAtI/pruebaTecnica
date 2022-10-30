from dagster import schedule
from datetime import datetime, time, date

@schedule(
	cron_schedule= "0 23 * * *",
	pipeline_name= "etl_relaciones_actores_schedule",
	execution_timezone="US/Central"
)

def etl_relaciones_actores_schedule(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {
        "solids": {
            "load_relaciones_actores_solid": {"config": {"date": date}},

        }
    }   

