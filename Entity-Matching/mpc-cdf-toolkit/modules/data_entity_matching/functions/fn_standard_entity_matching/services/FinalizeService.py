import abc
from cognite.client import CogniteClient
from cognite.client.data_classes.contextualization import ContextualizationJob
from utils.config import Config
from services.LoggerService import CogniteFunctionLogger
from services.RetrieveService import RetrieveServiceRAW, IRetrieveService
from services.ApplyService import ApplyServiceRAW, IApplyService

class IFinalizeService(abc.ABC):
    """Orchestrates the finalize process for contextualization job ids"""

    def __init__(
            self,
            client: CogniteClient,
            config: Config,
            logger: CogniteFunctionLogger,
            job: ContextualizationJob,
            retrieve_service: IRetrieveService=None,
            apply_service: IApplyService=None
    ):
        self.client=client
        self.config=config
        self.logger=logger
        self.job=job
        self.retrieve_service=retrieve_service
        self.apply_service=apply_service
        self.prepared=False

    

class FinalizeService(IFinalizeService):
    def __init__(
        self,
        client: CogniteClient,
        config: Config,
        logger: CogniteFunctionLogger,
        job: ContextualizationJob,
        retrieve_service: IRetrieveService=None,
        apply_service: IApplyService=None
    ):
        self.client=client
        self.config=config
        self.logger=logger
        self.job=job
        self.retrieve_service=retrieve_service
        self.apply_service=apply_service        
        self.is_supervised=self.config.is_supervised
        self.prepared=False
        self.name='FINALIZE_SERVICE'

    def prepare(self) -> bool:
        """Prepares the service by starting the retrieve and apply services"""
        if not self.retrieve_service:
            self.retrieve_service=RetrieveServiceRAW(
                client=self.client,
                config=self.config,
                logger=self.logger
            )

        if not self.apply_service:
            self.apply_service=ApplyServiceRAW(
                client=self.client,
                config=self.config,
                logger=self.logger
            )

        return self.retrieve_service and self.apply_service
        
    def finalize_job(self) -> bool:
        """The entry point for the finalize function to write results and retrain the model with true matches if necessary"""
        res=True

        # -------- Write results to RAW --------
            
        # get the matches and convert the data to a dataframe
        match_results_df=self.retrieve_service.get_matches(self.job).to_pandas()
        if len(match_results_df) == 0:
            self.logger.info(
                message=f"There were no match results for the job with id: {self.job.job_id}",
                section="START"
            )
            return res

        # use the RAW apply service to write it all back to RAW
        if not self.apply_service.write_match_results_to_raw(match_results_df, self.job.job_id):
            self.logger.error(
                message=f"There was an error writing the match results for job with id: {self.job.job_id}",
                section="START"
            )
            res=False
        # --------------------------------------

        # -------- Write true results to RAW if configured to do so ---
        if res:
            if self.config.contextualization_config.write_true_matches:
                # use the apply service to write it all back to RAW in the special TRUE MATCHES table
                true_match_results_df=self.retrieve_service.get_matches(self.job, true_matches=True).to_pandas()
                
                if len(true_match_results_df) == 0:
                    self.logger.info(
                        message=f"There were no true match results for job with id: {self.job.job_id}"
                    )

                    return True
                
                if not self.apply_service.write_match_results_to_raw(true_match_results_df, self.job.job_id, true_matches=True):
                    self.logger.error(
                        message=f"There was an error writing the true match results for job with id: {self.job.job_id}",
                        section="START"
                    )

                    return False
                
            return True

        elif not res:
            self.logger.error(
                message=f"Stopping Finalize service before true match logic due to unsuccessful retrieval of match data..."
            )
            return False
        else:
            return res