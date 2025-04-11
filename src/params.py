from dotenv import load_dotenv
import os
from pathlib import Path

class Params:
    """
    Parameters class.

    This class centralizes all configurable parameters in the codebase.

    To use a parameter in other parts of your code, instantiate this class:
    `params = Params()`

    Then pass the `params` object into functions and access parameters as attributes.

    For example, to use the `url` parameter in a function:

    ```
    def func(params):
        url = params.url
    ```
    """

    # Load environment variables from .env
    BASE_DIR = Path("/home/bb-8/Dags/ws_002")
    load_dotenv(BASE_DIR / ".env")

    # Data directories (absolute paths)
    DATA_DIR = BASE_DIR / "data"
    raw_data = DATA_DIR / "raw"
    external_data = DATA_DIR / "external"
    processed_data = DATA_DIR / "processed"
    intermediate_data = DATA_DIR / "intermediate"

    # Specific dataset paths
    GRAMMYS_CSV = intermediate_data / "grammys.csv"
    SPOTIFY_DATASET_PATH = external_data / "spotify_dataset.csv"
    FINAL_DATA = processed_data / "final_data.csv"

    # Logs
    log_name = BASE_DIR / "log" / "dump.log"

    # Control flags
    force_execution = True

    # Database credentials from .env
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_DATABASE")
    port = os.getenv("DB_PORT")
