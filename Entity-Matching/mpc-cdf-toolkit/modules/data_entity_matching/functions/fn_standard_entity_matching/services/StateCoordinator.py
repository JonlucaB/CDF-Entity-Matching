from utils.util import MatchingState, EntityMatchingStatus, should_cron_run
from cognite.client import CogniteClient
from services.LoggerService import CogniteFunctionLogger
from utils.Constants import *
from utils.util import elapsed_time
from datetime import datetime, timedelta
from services.LaunchService import LaunchMatchingService
from services.FinalizeService import FinalizeService
from cognite.client.data_classes.contextualization import ContextualizationJob
import pandas as pd

class StateCoordinator:
    """"Serves as a coordinator for handling states and kicking them off appropriately, and for appropriately handling the timeout of the function"""

    def __init__(
            self,
            client: CogniteClient,
            logger: CogniteFunctionLogger,
            start_time: datetime
    ):
        self.name="STATE COORDINATOR"
        self.client=client
        self.logger=logger
        self.states: list[MatchingState]=[]

        # the exit_processing is a reccomended timestamp to signify when we shouldn't try querying CDF for any more entities or RAW results
        self.exit_processing=start_time + timedelta(minutes=8)

        # the exit_time is a reccomended timestamp to signify when we should wrap up all handlers, finalize our states for the next function call, and exit the function
        self.exit_time=start_time + timedelta(minutes=9)

        # we'll need a retrieve service and an apply service fo sho
        self.retrieve_service=None
        self.apply_service=None

    def prepare(self) -> bool:
        # check if the states database exists, create it if it doesn't
        self.logger.debug(f"Checking CDF for state store database", section='START')
        if STATE_DB not in self.client.raw.databases.list().as_names():
            self.logger.debug(f"Database {STATE_DB} not found, creating database")
            if self.client.raw.databases.create(name=STATE_DB) is not None:
                self.logger.debug(f"Created database {STATE_DB} successfully")
            else:
                self.logger.error(f"Failed to create database {STATE_DB}, exiting function...")
                return False

        # check if the states table exists, create it if it doesn't
        self.logger.debug(f"Checking CDF for state store table", section='START')
        if STATE_TABLE not in self.client.raw.tables.list(db_name=STATE_DB, limit=None).as_names():
            self.logger.debug(f"Table {STATE_TABLE} not found, creating table")
            if self.client.raw.tables.create(db_name=STATE_DB, name=STATE_TABLE) is not None:
                self.logger.debug(f"Created table {STATE_DB} successfully")
            else:
                self.logger.error(f"Failed to create table {STATE_DB}, exiting function...")
                return False

        self.logger.info(f"Retrieving states from {STATE_TABLE} in {STATE_DB}...")
        if self.get_states():
            self.logger.info(f"Succesfully retrieved {len(self.states)} states from state store.")
        else:
            self.logger.error(f"Failed to retrieve states from states store, exiting functioin")
            return False
        
        timeout = self.config.contextualization_config.contextualization_model_config.timeout if self.config.contextualization_config.contextualization_model_config else None
        if timeout is not None:
            client_config = self.client.config
            client_config.timeout = timeout
            self.client = CogniteClient(client_config)

        return True
    
    def get_states(self) -> list[MatchingState]:
        """Gets all the states from CDF RAW and filters out states that are finalized or in error."""
        try:
            states_df=self.client.raw.rows.retrieve_dataframe(
                db_name=STATE_DB,
                table_name=STATE_TABLE,
                limit=None
            )
            
            if states_df.empty:
                return []

            interval_states=states_df[states_df[STATE_STATUS] == EntityMatchingStatus.FINALIZED]

            # Don't get the states that have been completed, and don't need to be run again!
            states_df=states_df[states_df[STATE_STATUS] != EntityMatchingStatus.FINALIZED]
            states_df=states_df[states_df[STATE_STATUS] != EntityMatchingStatus.ERROR]

            if not interval_states.empty:
                interval_states=interval_states[interval_states.apply(lambda r: should_cron_run(cron_string=r[STATE_INTERVAL], last_run_iso=r[STATE_SOURCE_UPDATED_TIME]), axis=1)]
                interval_states[STATE_STATUS] = EntityMatchingStatus.NEW
                states_df=pd.concat([states_df, interval_states])

            # turn all of these states into NEW in RAW as they are being held by this function call
            self.client.raw.rows.insert_dataframe(
                db_name=STATE_DB,
                table_name=STATE_TABLE,
                dataframe=states_df
            )

            states=[MatchingState.from_row(row) for i, row in states_df.iterrows()]
            return states

        except Exception as e:
            self.logger.error(f"There was an error retrieveing the states from RAW: {e}")
            return []
        
    def update_state(self, state: MatchingState) -> bool:
        state.active=False
        state.source_updated_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        state_df=state.to_pandas()

        try:
            self.client.raw.rows.insert_dataframe(
                db_name=STATE_DB,
                table_name=STATE_TABLE,
                dataframe=state_df
            )

            self.logger.debug(f"Updated state with id {state.id} to new state: {state.model_dump_json()}")

            return True
        except Exception as e:
            self.logger.error(f"Failed to update state with id {state.id}: {e}")
            return False

    # the main loop to query states, kick off jobs, and finalize them :)
    def run_main_loop(self) -> int:
        states_handled=0
        last_state_retrieval=datetime.now()

        while datetime.now() < self.exit_time:
            try:
                # get the states again incase another instance has done something :)
                while datetime.now() < self.exit_processing and self.states != []:
                    # process those states woo hoo!
                    current_state: MatchingState=self.states.pop()

                    # There was an error reading the config for this state
                    if current_state.config == None:
                        self.logger.error(f"There was an error parsing the config for state with ID {current_state.id}")
                        current_state.matching_status=EntityMatchingStatus.ERROR
                        continue

                    # need to make sure this state is not being consumed right now
                    self.logger.debug(f"Handling state with ID {current_state.id}:", section='START')

                    match current_state.matching_status:
                        case EntityMatchingStatus.NEW:
                            print("new")
                            launch_service=LaunchMatchingService(
                                config=current_state.config,
                                client=self.client,
                                logger=self.logger
                            )

                            launch_service.prepare()
                            self.logger.info("Pulling instances from source and target views...")

                            sources_ts = datetime.now()
                            source_instances = launch_service.get_retrieve_service.pull_instances(type=SOURCE)
                            self.logger.debug(f"Pulled {len(source_instances)} instances from source view: {current_state.config.source_config.view_id}. Elapsed time: {elapsed_time(sources_ts)}")

                            targets_ts = datetime.now()
                            target_instances = launch_service.get_retrieve_service.pull_instances(type=TARGET)
                            self.logger.debug(f"Pulled {len(target_instances)} instances from target view: {current_state.config.target_config.view_id}. Elapsed time: {elapsed_time(targets_ts)}")
                            
                            self.logger.info(f"Finished pulling instances from CDF, proceeding with retrieving the model")

                            try:
                                model_id, model_external_id=launch_service.get_matching_service._get_model_ids(source_instances, target_instances)
                            except Exception as e:
                                self.logger.error(f"There was an error while preparing the model: {e}")
                                current_state.matching_status=EntityMatchingStatus.ERROR
                                continue

                            self.logger.info(f"Successfully retrieved the model with id {model_id}, running predict job...")

                            job=launch_service.get_matching_service.run_matching_job(
                                model_id=model_id,
                                model_external_id=model_external_id
                            )
                            # We have to wait for this thread to upload the sources and entities to the model as well... and if we need to refit it!

                            # as soon as its done uploading, we kick off the job
                            # upsert the state's model_id, status -> PROCESSING, and active -> FALSE
                            current_state.matching_job_id=job.job_id
                            current_state.matching_status=EntityMatchingStatus.PROCESSING
                            current_state.model_id=model_id
                            current_state.source_updated_user=launch_service.name
                            # push back to RAW

                        case EntityMatchingStatus.PROCESSING:
                            print("processing") 
                            # Check the matching job to see if its done
                            # if matching job is not done, update its active to FALSE in RAW and move on

                            # .get(id) doesn't work for some reason, have to do this other met
                            job=None
                            try:
                                jobs_df=self.client.entity_matching.list_jobs().to_pandas()
                                job_r=jobs_df[jobs_df["job_id"] == current_state.model_id].iloc[0]
                                job=ContextualizationJob(**job_r.to_dict(), status_path='/context/entitymatching/jobs/', cognite_client=self.client)
                                job.job_id=current_state.matching_job_id
                            except:
                                self.logger.error(f"The job with id {current_state.matching_job_id} could not be found in CDF...")
                                current_state.matching_status=EntityMatchingStatus.ERROR
                                continue

                            # Check if job has completed, there may be a nicer way to do this syntactically, but I don't really care right now
                            if job != None and job.status == 'Completed':
                                self.logger.info(f"Job with ID {current_state.matching_job_id} has finished, proceeding to finalization")

                                finalize_service = FinalizeService(
                                    client=self.client,
                                    config=current_state.config,
                                    logger=self.logger,
                                    job=job
                                )

                                if finalize_service.prepare():
                                    if finalize_service.finalize_job():
                                        current_state.matching_status=EntityMatchingStatus.FINALIZED
                                        self.logger.info(f"Successfully completed state with ID {current_state.id}, proceeding to next state.")
                                    else:
                                        self.logger.error(f"There was an error when finalizing the job. Setting state to Error")
                                        current_state.matching_status=EntityMatchingStatus.ERROR
                                else:
                                    self.logger.error(f"There was an error when finalizing the job. Setting state to Error")
                                    current_state.matching_status=EntityMatchingStatus.ERROR

                            else:
                                self.logger.info(f"Job with ID {current_state.matching_job_id} has not finished, moving on...")
                            # if matching job is DONE
                            # use our services to write the results back to the thing after pulling them, and upsert the state to FINALIZED
                            current_state.source_updated_user=finalize_service.name

                        case EntityMatchingStatus.FINALIZED:
                            print("Finalized")
                            # Match results have been written to RAW, we honestly shouldn't even encounter this state lol

                        case _:
                            self.logger.error(f"Unexpected state status received, moving on...")

                    # Put state back into RAW with new stuff lol
                    if not self.update_state(current_state):
                        self.logger.error(f"Failed to update state -- Check state in RAW with ID {current_state.id}")

                    states_handled+=1
            except Exception as e:
                self.logger.error(f"There was an error while working through the states.\nPushing states back to RAW in their current states.")
                for state in self.states:
                    self.update_state(state)

            # Refresh the states with those in RAW
            if datetime.now() - last_state_retrieval > timedelta(minutes=1):
                last_state_retrieval=datetime.now()
                self.states=self.get_states()
            elif datetime.now() > self.exit_processing:
                # Function is out of time to process items, exit
                for state in self.states:
                    self.update_state(state)
                self.states=[]
            
        for state in self.states:
            self.update_state(state)
        return states_handled