import abc
from typing import Optional
from cognite.client import CogniteClient
from cognite.client.data_classes.contextualization import ContextualizationJob
from services.LoggerService import CogniteFunctionLogger
from services.RetrieveService import RetrieveServiceRAW
import datetime
from services.ApplyService import ApplyServiceRAW
from utils.util import EntityList
from utils.config import (
    Config,
    ContextualizationConfig,
    ContextualizationModelConfig,
    SupervisedConfig
)

class IContextualizationService(abc.ABC):
    """Interface for running contextualization matching jobs."""
    @abc.abstractmethod
    def _perform_predict_call(self) -> ContextualizationJob:
        """Hits the contextualization endpoint in cdf"""
        pass

    @abc.abstractmethod
    def run_matching_job(
        self, sources: Optional[EntityList], targets: Optional[EntityList]
    ) -> ContextualizationJob:
        """Queues source and target entities for entity matching and returns a job ID."""
        pass

class BaseMatchingService(IContextualizationService):
    """Base class for common initialization and shared logic for START supervised and unsupervised matching services. It takes over parts of the original MatchingMatchingService."""
    def __init__(self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger):
        self.client: CogniteClient=client
        self.config: Config=config
        self.logger: CogniteFunctionLogger=logger

        if not hasattr(self.config, 'contextualization_config') or self.config.contextualization_config is None:
            self.logger.error("config.contextualization_config is missing, cannot proceed with matching parameters.")
            raise ValueError("Match configuration (num_matches, score_threshold) is required.")
        else:
            self.contextualization_config: ContextualizationConfig=config.contextualization_config
        
        # Ensure contextualization_model_config is always available, defaulting if needed
        self.contextualization_model_config: ContextualizationModelConfig=self._get_contextualization_model_config()
        self.num_matches=self.config.contextualization_config.num_matches
        self.score_threshold=self.config.contextualization_config.score_threshold
        self.model_id=None
        self.model_external_id=None
        self.name="BASE MATCHING SERVICE"

    def _get_contextualization_model_config(self) -> ContextualizationModelConfig:
        """Helper to ensure contextualization_model_config is always initialized."""
        if not self.contextualization_config.contextualization_model_config:
            self.logger.info(
                message="Configuration for contextualization model not found, using default parameters {'feature_type' : 'bigram', 'match_fields' : ['aliases'], 'timeout' : None}..."
            )
            return ContextualizationModelConfig(
                feature_type="bigram",
                timeout=None
            )
        return self.contextualization_config.contextualization_model_config

    @abc.abstractmethod
    def _get_model_ids(self, sources: Optional[EntityList]=None, targets: Optional[EntityList]=None) -> tuple[Optional[int], Optional[int]]: 
        """Abstract method to be implemented by concrete services for generating the model id and model external id"""
        pass

    def _perform_predict_call(self, model_id: Optional[int], model_external_id: Optional[int]) -> ContextualizationJob:
        """Calls the CDF Entity Matching predict endpoint with sources and targets. Returns the contextualization job object."""
        #TODO refactor this so that the _get_model_ids can be called from the state coordinator and we can split up the process of making the model and running the model :)

        self.logger.debug(message=f"Calling entity matching predict API with modelId ({model_id}, {model_external_id})...")
        return self.client.entity_matching.predict(
            num_matches=self.contextualization_config.num_matches,
            score_threshold=self.contextualization_config.score_threshold,
            id=model_id
        )
    
    def run_matching_job(
        self, model_id: Optional[int], model_external_id: Optional[int]
    ) -> ContextualizationJob:
        """Executes the contextualization job based on the concrete service's implementation. This contains the common logging and job ID return logic from the original run_matching_job."""
        self.logger.info(
            message="Beginning contextualization job",
            section='START'
        )

        contextualization_job: ContextualizationJob=self._perform_predict_call(model_id, model_external_id)

        if contextualization_job.job_id:
            self.logger.info(f"Contextualization job started successfully with ID: {contextualization_job.job_id}", section="START")
            return contextualization_job
        else:
            self.logger.error("Contextualization job ID was not created successfully by the API.")
            raise Exception("404 ---- No job Id was created")

class SupervisedMatchingService(BaseMatchingService):
    """Service for running supervised entity matching jobs using a pre-trained model ID. This handles the `if self.contextualization_config and self.supervisedConfig:` part of the original logic."""
    def __init__(self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger, retrieve_service: RetrieveServiceRAW, apply_service: ApplyServiceRAW):
        super().__init__(config, client, logger)

        # Supervised service explicitly requires a model ID.
        # This check is duplicated in LaunchService for clarity, but essential here too.
        if not config.is_supervised: # Safely check for 'm' attribute
            self.logger.error("Supervised configuration is missing or invalid for SupervisedMatchingService.")
            raise ValueError("Supervised model ID is required for SupervisedMatchingService.")
        else:
            self.supervised_config: SupervisedConfig=self.contextualization_config.supervised_config
            self.retrieve_service=retrieve_service
            self.apply_service=apply_service
            self.name="SUPERVISED MATCH SERVICE"

    def _get_model_ids(self, sources: Optional[EntityList]=None, targets: Optional[EntityList]=None) -> tuple[Optional[int], Optional[int]]: 
        """
        Gets the existing model from CDF
        """

        true_matches_df=self.retrieve_service.get_matches_raw(true_matches=True)
        if self.supervised_config.id:
            self.logger.debug(f"Found model id {self.supervised_config.id} in config. Retrieving model from CDF")

            if true_matches_df.empty:
                self.logger.warning(f"No true matches found for this run. Proceeding with old model.")
                return (self.supervised_config.id, None)
            else:
                new_model=self.apply_service.refit_model(
                    match_results_df=true_matches_df,
                    model_id=self.supervised_config.id
                )

                return (new_model.id, new_model.external_id)
        else:
            self.logger.debug(f"No model proivded in supervised config, creating a new one with true matches.")

            true_matches_list=[
                {"sourceExternalId" : row['external_id_source'], "targetExternalId" : row['external_id_target']}
                for i, row in true_matches_df.iterrows()
                if not row.empty 
            ]

            self.logger.debug(f"Found {len(true_matches_list)} true matches to train with, applying them to the model...")

            if true_matches_list == []:
                true_matches_list = None

            combinations=sources.property_product(
                targets, 
                self.config.source_config.fields_to_contextualize,
                self.config.target_config.fields_to_contextualize
            )
            sources_dump=sources.explode_and_dump()
            targets_dump=targets.explode_and_dump()
            feature_type=self.config.contextualization_config.contextualization_model_config.feature_type
            model = self.client.entity_matching.fit(
                sources=sources_dump,
                targets=targets_dump,
                match_fields=combinations,
                feature_type=feature_type,
                true_matches=true_matches_list,
                ignore_missing_fields=True,
                external_id=f"model_{datetime.datetime.now()}_{self.config.source_config.view_id.external_id}_{self.config.target_config.view_id.external_id}"
            )

            if model is None:
                self.logger.error("There was an error making the model")
                raise Exception("There was an error making the model")

            return (model.id, model.external_id)

class UnsupervisedMatchingService(BaseMatchingService):
    """
    Service for running unsupervised entity matching jobs.
    This handles the `elif self.contextualization_config:` part of the original logic.
    It uses the general contextualization model configuration (feature_type, match_fields, timeout).
    """
    def __init__(self, config: Config, client: CogniteClient, logger: CogniteFunctionLogger):
        super().__init__(config, client, logger)
        self.name="UNSUPERVISED MATCHING SERVICE"
        # Unsupervised doesn't need a specific supervised_config, it relies on general contextualization_model_config
        # which is handled by the BaseMatchingService.

    def _get_model_ids(self, sources: EntityList, targets: EntityList) -> tuple[Optional[int], Optional[int]]: 
        """Creates a new unsupervised model based off of the contextualization config"""
        combinations=sources.property_product(
            targets, 
            self.config.source_config.fields_to_contextualize,
            self.config.target_config.fields_to_contextualize
        )
        sources_dump=sources.explode_and_dump()
        targets_dump=targets.explode_and_dump()
        feature_type=self.config.contextualization_config.contextualization_model_config.feature_type
        model = self.client.entity_matching.fit(
            sources=sources_dump,
            targets=targets_dump,
            match_fields=combinations,
            feature_type=feature_type,
            ignore_missing_fields=True,
            external_id=f"model_{datetime.datetime.now()}_{self.config.source_config.view_id.external_id}_{self.config.target_config.view_id.external_id}"
        )

        if model is None:
            self.logger.error("There was an error making the unsupervised model")
            raise Exception("There was an error making the unsupervised model")

        return (model.id, model.external_id)