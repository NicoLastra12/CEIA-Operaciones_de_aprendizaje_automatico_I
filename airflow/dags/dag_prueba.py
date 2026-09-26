"""
DAG de prueba: lee el iris.csv del bucket `data`, calcula un resumen
y lo guarda de vuelta en el mismo bucket.

Sirve para verificar que Airflow (dentro de Docker) puede leer y escribir en RustFS.
Requisito: se debe haber corrido antes la notebook 00_prueba_conexion.ipynb (sube iris.csv).
"""
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="prueba_s3",
    schedule=None,                                   # solo se ejecuta a mano
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["prueba"],
)
def prueba_s3():

    @task
    def leer():
        """Descarga iris.csv de RustFS y devuelve cuántas filas tiene."""
        import boto3
        import pandas as pd

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket="data", Key="prueba/iris.csv")
        df = pd.read_csv(obj["Body"])
        print(f"Leí {df.shape[0]} filas y {df.shape[1]} columnas")
        return df.shape[0]                            

    @task
    def resumir_y_guardar(n_filas: int):
        import io
        import boto3
        import pandas as pd

        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket="data", Key="prueba/iris.csv")
        df = pd.read_csv(obj["Body"])

        resumen = df.groupby("target").mean()
        print(resumen)

        buffer = io.StringIO()
        resumen.to_csv(buffer)
        s3.put_object(Bucket="data", Key="prueba/resumen.csv", Body=buffer.getvalue())
        print(f"Guardé s3://data/prueba/resumen.csv (a partir de {n_filas} filas)")

    resumir_y_guardar(leer())


prueba_s3()
