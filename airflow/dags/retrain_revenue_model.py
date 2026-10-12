"""
Reentrenamiento del modelo de recaudación con el esquema champion/challenger.

Se ejecuta solo cada vez que etl_tmdb_movies actualiza los datos (asset TMDB_DATA).
Entrena un challenger con los hiperparámetros del champion, compara los dos sobre el
mismo set de testeo y promueve al challenger solo si tiene menor error.

Acá solo se define el flujo; la lógica está en utils/model.py.
"""
import datetime

from airflow.sdk import Asset, dag, task

from utils import config

# Argumentos de las tareas que corren en el virtualenv de ML, sin Airflow
# (ver dockerfiles/airflow/Dockerfile). Solo se ejecuta el código de la función, por eso
# utils se importa adentro. El decorador tiene que quedar escrito como
# @task.external_python: Airflow lo busca por ese nombre para sacarlo del código.
ML_VENV = {"python": config.ML_PYTHON, "expect_airflow": False}

# Lo actualiza etl_tmdb_movies; cuando cambia, corre este DAG
TMDB_DATA = Asset(config.TMDB_ASSET_URI)

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}


@dag(
    dag_id="retrain_revenue_model",
    description="Reentrena el modelo y lo pone en producción si mejora al champion",
    default_args=default_args,
    schedule=[TMDB_DATA],
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30),
    tags=["Re-Train", "TMDB"],
)
def retrain_revenue_model():

    @task.external_python(task_id="train_challenger", multiple_outputs=True, **ML_VENV)
    def train_challenger():
        """Entrena con los hiperparámetros del champion y lo registra como challenger."""
        from utils import model

        return model.train_and_register(model.champion_params())

    # Sin multiple_outputs a propósito: champion_mae puede ser None, y Airflow 3.0.2 falla al
    # guardar en XCom un valor None suelto. Dentro de un diccionario no hay problema.
    @task.external_python(task_id="evaluate_models", **ML_VENV)
    def evaluate_models(challenger_run_id):
        """Calcula el MAE del challenger y del champion sobre el mismo set de testeo.

        Devuelve {"challenger_mae": float, "champion_mae": float o None si no hay champion}.
        """
        from utils import config, model

        _, _, x_test, y_test = model.load_split()
        client = model.mlflow_client()

        challenger_mae = model.evaluate(model.load_alias(config.CHALLENGER), x_test, y_test)
        client.log_metric(challenger_run_id, "test_mae_challenger", challenger_mae)
        print(f"🧪 MAE challenger: {challenger_mae:,.0f}")

        champion = model.load_alias(config.CHAMPION)
        if champion is None:
            print("ℹ️ No hay champion para comparar")
            return {"challenger_mae": challenger_mae, "champion_mae": None}

        champion_mae = model.evaluate(champion, x_test, y_test)
        client.log_metric(challenger_run_id, "test_mae_champion", champion_mae)
        print(f"🏆 MAE champion: {champion_mae:,.0f}")
        return {"challenger_mae": challenger_mae, "champion_mae": champion_mae}

    @task.branch(task_id="compare_models")
    def compare_models(maes):
        """Elige el camino: promover si el challenger tiene menor error o no hay champion."""
        challenger_mae, champion_mae = maes["challenger_mae"], maes["champion_mae"]
        if champion_mae is None or challenger_mae < champion_mae:
            return "promote_challenger"
        return "discard_challenger"

    @task.external_python(task_id="promote_challenger", **ML_VENV)
    def promote_challenger(version):
        """El challenger pasa a ser el champion, que es el modelo que usa la API."""
        from utils import model

        model.set_champion(version)
        print(f"🚀 Versión {version} en producción como champion")

    @task.external_python(task_id="discard_challenger", **ML_VENV)
    def discard_challenger(version):
        """El challenger no mejoró al champion: se le saca el alias."""
        from utils import model

        model.drop_challenger()
        print(f"🛑 Versión {version} descartada, el champion sigue igual")

    # 🧩 Encadenamiento
    challenger = train_challenger()
    maes = evaluate_models(challenger["run_id"])
    compare_models(maes) >> [
        promote_challenger(challenger["version"]),
        discard_challenger(challenger["version"]),
    ]


retrain_revenue_model()
