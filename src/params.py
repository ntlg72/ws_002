from dotenv import load_dotenv 
import os
import tempfile
from pathlib import Path

class Params:
    """
    Parameters class.

    This file centralizes anything that can be 
    parametrized in the code.

    If you want to use a parameter later in the code, you
    should instantiate params as:
    `params = Params()`, 
    send the object `params` within functions and, inside the 
    function, call the parameter as an attribute of the `params` object.

    For instance, if you have a parameter called `url` created here, you'd 
    call it in a function called `func` as:

    ```
    def func(params):
        ...
        url = params.url
        ...
    ```
    """

    # Load environment variables
    BASE_DIR = Path(__file__).resolve().parent.parent
    load_dotenv(BASE_DIR / '.env')

    # Use system temp directory for data paths
    TMP_BASE_DIR = Path(tempfile.gettempdir()) / "reccobeats_tmp"
    TMP_BASE_DIR.mkdir(exist_ok=True)

    raw_data = TMP_BASE_DIR / 'raw'
    external_data = TMP_BASE_DIR / 'external'
    processed_data = TMP_BASE_DIR / 'processed'
    intermediate_data = TMP_BASE_DIR / 'intermediate'

    for d in [raw_data, external_data, processed_data, intermediate_data]:
        d.mkdir(parents=True, exist_ok=True)

    log_name = TMP_BASE_DIR / 'log' / 'dump.log'
    log_name.parent.mkdir(parents=True, exist_ok=True)

    # ReccoBeats input audio directory (not temporary)
    DATA_DIR = BASE_DIR / 'data'
    AUDIO_DIR = DATA_DIR / 'audio_files'

    SPOTIFY_DATASET_PATH = external_data / 'spotify_dataset.csv'

    # Force execution
    force_execution = True 

    # DB parameters
    user = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    database = os.getenv('DB_DATABASE')
    port = os.getenv('DB_PORT')