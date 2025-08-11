from utils.config import (
    Config
)
from services.ApplyService import ApplyServiceRAW
from services.RetrieveService import RetrieveServiceRAW
from services.ContextualizationService import BaseMatchingService, SupervisedMatchingService, UnsupervisedMatchingService
from services.LoggerService import CogniteFunctionLogger
from cognite.client import CogniteClient

class LaunchMatchingService:
    """
    Service responsible for launching either the supervised or unsupervised matching service
    based on the provided configuration. This replaces the top-level conditional logic
    in the original run_matching_job.
    """
    def __init__(self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger, retrieve_service: RetrieveServiceRAW=None, apply_service: ApplyServiceRAW=None):
        self.config=config
        self.client=client
        self.logger=logger
        self.matching_service: BaseMatchingService=None
        self.retrieve_service: RetrieveServiceRAW=retrieve_service
        self.apply_service: ApplyServiceRAW=apply_service
        self.name="LAUNCH SERVICE"

    @property
    def get_matching_service(self) -> BaseMatchingService:
        return self.matching_service
    
    @property
    def get_apply_service(self) -> ApplyServiceRAW:
        return self.apply_service

    @property
    def get_retrieve_service(self) -> RetrieveServiceRAW:
        return self.retrieve_service

    def prepare(self):
        """Determines which concrete matching service to instantiate and returns the appropriate IContextualizationService implementation based on the configuration."""
        self.logger.info("Determining which matching service to launch based on configuration...")

        contextualization_config=self.config.contextualization_config

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

        if self.config.is_supervised: # Check if supervised_config exists and has a non-None 'm' attribute
            self.logger.info("Supervised model ID found in configuration. Launching SupervisedMatchingService.")
            

            self.matching_service=SupervisedMatchingService(self.config, self.client, self.logger, self.get_retrieve_service, self.get_apply_service)
        
        # Fallback to unsupervised if no supervised config, but ensure basic match config exists (corresponds to original 'elif self.contextualization_config:')
        elif contextualization_config is not None:
            self.logger.info("No supervised model ID found. Launching Unsupervised Matching Service.")
            self.matching_service=UnsupervisedMatchingService(self.config, self.client, self.logger)
        else:
            self.logger.error("No valid configuration found for any contextualization job. Neither supervised model ID nor basic contextualization_config parameters are sufficient.")
            raise ValueError("No valid configuration found to launch a contextualization service.")
