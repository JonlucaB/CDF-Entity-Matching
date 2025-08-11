import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

import json
from services.StateCoordinator import StateCoordinator
from utils.config import Config
from datetime import datetime
from dependencies import (
    create_logger_service,
    create_config_service,
    create_write_logger_service
)

def handle(client, function_call_info: dict, data):
    logger_instance = create_logger_service(log_level=data.get("log_level", "DEBUG"))
    
    start_time = datetime.now()
    logger_instance.info(
        message=f"Initiating local entity matching job. Process started at {start_time.strftime('%y/%m/%d %H:%M:%S')} CDT.",
        section='START'
    )

    try:
        state_coordinator=StateCoordinator(
            client=client,
            logger=logger_instance,
            start_time=start_time
        )

        states_handled=state_coordinator.run_main_loop()

        logger_instance.info(f"Exited main loop before function timeout; handled {states_handled} states...")

    except Exception as e:
        logger_instance.error(
            message=f"Ran into the following error during handle run: \n{e}",
            section="END",
        )
    finally:
        logger_instance.close()

def run_locally(config_data: dict, log_path: str | None = None):
    """
    Runs the entity matching process locally, mimicking the function handler environment.

    Args:
        config_data (dict): A dictionary containing the configuration parameters
                            that can be directly used to instantiate the Config class.
        log_path (str | None): Optional path to write logs to a file.
    """
    log_level = config_data.get("log_level", "DEBUG")

    config_instance, client = create_config_service(function_data=config_data)

    if log_path:
        logger_instance = create_write_logger_service(
            log_level=log_level, filepath=log_path
        )
    else:
        logger_instance = create_logger_service(log_level=log_level)
    
    start_time = datetime.now()
    logger_instance = create_logger_service(log_level=log_level)
    
    start_time = datetime.now()
    logger_instance.info(
        message=f"Initiating local entity matching job. Process started at {start_time.strftime('%y/%m/%d %H:%M:%S')} CDT.",
        section='START'
    )
    
    logger_instance.info("Config file is in correct format, continuing with contextualization service...")

    try:
        while True:
            state_coordinator=StateCoordinator(
                client=client,
                logger=logger_instance,
                start_time=start_time
            )

            states_handled=state_coordinator.run_main_loop()

            logger_instance.info(f"Exited main loop before function timeout; handled {states_handled} states...")

    except Exception as e:
        logger_instance.error(
            message=f"Ran into the following error during handle run: \n{e}",
            section="END",
        )
    finally:
        logger_instance.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python handler.py <path_to_config_json> [log_path]")
        sys.exit(1)

    config_path = sys.argv[1]
    log_path = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        with open(config_path, 'r') as f:
            config_file_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {config_path}. Ensure it's a valid JSON file.")
        sys.exit(1)

    run_locally(config_data=config_file_data, log_path=log_path)