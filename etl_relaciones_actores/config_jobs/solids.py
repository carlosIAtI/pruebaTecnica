from etl_relaciones_actores.load_relacion_actores import *
from dagster import op, job, Nothing

@op(description="load relaciones actores")
def load_relaciones_actores_solid () -> Nothing:
    load_relaciones_actores()



@job()
def load_relaciones_actores_pipeline():
    load_relaciones_actores_solid()
