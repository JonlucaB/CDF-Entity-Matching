from services.LoggerService import CogniteFunctionLogger
from cognite.client import CogniteClient
from utils.util import elapsed_time
from utils.config import Config
from datetime import datetime
from cognite.client.data_classes.contextualization import EntityMatchingModel
import pandas as pd
import abc

class IApplyService(abc.ABC):
    """This service orchestrates the application of match results from a job id(s) to RAW, views, etc..."""
    def __init__(
            self,
            client: CogniteClient,
            config: Config,
            logger: CogniteFunctionLogger
    ):
        self.client=client
        self.config=config
        self.logger=logger
        self.name="APPLY SERVICE"

class ApplyServiceRAW(IApplyService):
    def __init__(
            self,
            client: CogniteClient,
            config: Config,
            logger: CogniteFunctionLogger
    ):
        self.client=client
        self.config=config
        self.logger=logger
        self.name="APPLY SERVICE"

    def refit_model(self, match_results_df: pd.DataFrame, model_id: int) -> EntityMatchingModel:
        """Refits the model"""
        if len(match_results_df) == 0:
            self.logger.info(message=f"No true matches fed to refit the model, skipping the refit step")
            return True

        try:           
            true_match_for_training=[
                {"sourceExternalId" : row['external_id_source'], "targetExternalId" : row['external_id_target']}
                for i, row in match_results_df.iterrows()
                if not row.empty
            ]
        except Exception as e:
            self.logger.error(
                message=f"There was an error in parsing the true matches, quitting Apply service\n{e}"
            )
            return None
        
        # re-fit the model based on the true matches :)
        try:
            new_model=self.client.entity_matching.refit(
                true_matches=true_match_for_training,
                id=model_id,
                # new_external_id = "echo_model"
            )
            self.logger.debug(message=f"Successfully refit model with {len(match_results_df)} true matches")

            return new_model
        except Exception as e:
            self.logger.error(
                message=f"Failed to retrain model with id: {model_id} due to exception:\n{e}",
                section="START"
            )

    def write_match_results_to_raw(self, results: pd.DataFrame, job_id: int=None, true_matches: bool=False) -> bool:
        """
        Writes the match results of a contextualizaton job to RAW with an optional job id to include and a flag for true matches
        """
        if len(results) == 0:
            self.logger.info(message=f"No results to write to table, skipping writeback process")
            return True

        start_ts=datetime.now()
        true_table=self.config.contextualization_config.true_matches_table
        default_table=self.config.contextualization_config.match_result_table

        result_table=default_table if not true_matches else true_table
        if result_table.table_name is None:
            new_name=str(job_id) if not true_matches else f'{job_id}_true_matches'
            self.logger.info(
                message=f"No result table name given in configuration, creating new table based off of job ID: {new_name}",
                section="START"
                )
            result_table.table_name=str(new_name)
        if result_table.database_name is None:
            self.logger.error(
                message=f"No database provided for writing results, quitting Apply service. Please check configuration",
                section="START"
            )
            return False

        self.logger.info(
            message=f"Applying {len(results)} match results to {result_table.table_name}",
            section="START"
        )

        if result_table.table_name not in self.client.raw.tables.list(db_name=result_table.database_name, limit=None).as_names():
            self.logger.info(
                message=f"Table with name \'{result_table.table_name}\' not found in database \'{result_table.database_name}\', creating new table with that name",
                section="START"
            )
            try:
                new_table=self.client.raw.tables.create(
                    db_name=result_table.database_name,
                    name=result_table.table_name)
                
                if new_table is not None:
                    self.logger.info(
                        message=f"Created new table {result_table.table_name} in database {result_table.database_name} successfully",
                        section="START"
                    )
                else:
                    return False

            except Exception as e:
                self.logger.error(
                    message=f"There was an error when creating the new table with name \'{result_table.table_name}\'\nPlease double check the configuration and re-run the function\nException: {e}",
                    section="START"
                    )
                return False
        # If we're writing true matches, clear everything that exists
        else:
            self.logger.debug(
                message="Clearing existing rows from table...",
                section="START"
                )
            
            all_keys=self.client.raw.rows.retrieve_dataframe(
                db_name=result_table.database_name,
                table_name=result_table.table_name,
                columns=[],
                limit=-1
            ).index

            if not true_matches:
                all_keys=all_keys.intersection(results.index)
                self.logger.debug(message=f"Writing to matches table {result_table.table_name}, overwriting old match scores with new ones")
            else:
                self.logger.debug(message=f"Writing true matches to {result_table.table_name}, clearing all training data")

            self.logger.debug(
                message=f"Deleting {len(all_keys)} from {result_table}...",
                section="START"
                )
            
            self.client.raw.rows.delete(
                db_name=result_table.database_name,
                table_name=result_table.table_name,
                key=all_keys.to_list()
            )

            self.logger.debug(
                message=f"Deleted {len(all_keys)} rows from {result_table.table_name}, proceeding to upload results..."
            )

        try:
            # Upload the results to the table
            self.client.raw.rows.insert_dataframe(
                db_name=result_table.database_name,
                table_name=result_table.table_name,
                dataframe=results
            )
        except Exception as e:
            self.logger.error(
                message=f"Failed to upload {len(results)} to table {result_table.table_name}\n{e}",
                section="END"
            )
            return False

        self.logger.info(
            message=f"Uploaded {len(results)} rows to {result_table.table_name} in {elapsed_time(start=start_ts)}",
            section="START")

        return True