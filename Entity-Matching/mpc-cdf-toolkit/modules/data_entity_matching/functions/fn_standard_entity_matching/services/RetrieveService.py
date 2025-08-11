import abc
from typing import Literal
from cognite.client import CogniteClient, data_modeling as dm
from cognite.client.data_classes.contextualization import ContextualizationJob
from services.LoggerService import CogniteFunctionLogger
from utils.config import Config, InstanceConfig, Table
from utils.Constants import *
from utils.util import MatchItem, MatchResult, MatchResultList, EntityList
from typing import Optional
import pandas as pd
import math
import numpy as np

class IRetrieveService(abc.ABC):
    """
    This service orcherstrates the retrieval of (true) matches from a data source (RAW, views, etc...)
    """

    def __init__(
            self,
            client: CogniteClient,
            config: Config,
            logger: CogniteFunctionLogger
    ):
        self.client=client
        self.config=config
        self.logger=logger
        self.name="RETRIEVE SERVICE"

    @abc.abstractmethod
    def get_matches(self, job_id: int, true_matches: bool=False) -> list[MatchResult]:
        """Gets the matches"""
        pass

    def get_instance_config(self, type: Literal["SOURCE", "TARGET"]) -> InstanceConfig:
        """Returns the instance config for the specified entity type"""
        match type:
            case "SOURCE":
                return self.config.source_config
            case "TARGET":
                return self.config.target_config
            case _:
                self.logger.error(
                    message=f"Unknown instance type: {type} recieved in RetrieveService pull_instances",
                    section='START'
                )
                raise ValueError()

    def _create_query(self, type: Literal['SOURCE', 'TARGET'], name: str, cursor: Optional[str]) -> dm.query.Query:
        """Creates a paginated query for instances, using an optional cursor."""
        instance_config=self.get_instance_config(type)
        view_id=instance_config.view_id.as_view_id

        is_selected=dm.filters.And(
            dm.filters.In(["node", "space"], [instance_config.instance_space]), 
            dm.filters.HasData(views=[view_id])
        )

        if instance_config.filter_tags is not None:
            is_selected=dm.filters.And(is_selected, dm.filters.In(view_id.as_property_ref("tags"), instance_config.filter_tags))
        
        return dm.query.Query(
            with_={
                name: dm.query.NodeResultSetExpression(
                    filter=is_selected,
                    limit=1000
                )
            },
            select={name: dm.query.Select(
                [dm.query.SourceSelector(
                    source=view_id, 
                    properties=instance_config.fields_to_pull
                    )
                ])
            },
            cursors={name: cursor}
        )
    
    def pull_instances(self, type: Literal['SOURCE', 'TARGET']) -> EntityList:
        """Queries the CDF endpoint for the instances using the config provided"""
        instance_config=self.get_instance_config(type)
        all_nodes: EntityList=[]
        name=f"nodes_{type}"
        cursor: Optional[str]=None
        
        try:
            self.logger.info(
                message=f"Attempting to query CDF instances endpoint with viewId: {instance_config.view_id.as_view_id}",
                section="START"
            )

            while True:
                query=self._create_query(type=type, name=name, cursor=cursor)
                
                # Use the .query() method for paginated requests
                query_result=self.client.data_modeling.instances.query(query=query)

                nodes_page=query_result.get_nodes(name)
                
                if nodes_page:
                    all_nodes.extend(EntityList.from_nodes(nodes=nodes_page))
                
                # Get the cursor for the next page
                cursor=nodes_page.cursor
                
                # Exit the loop if there are no more pages
                if not cursor:
                    break
                
                self.logger.debug(f"Fetched {len(all_nodes)} instances so far. Requesting next page...")

            self.logger.info(
                message=f"Successfully fetched a total of {len(all_nodes)} instances.",
                section="END",
            )
            # Assuming your EntityList class can be initialized from a list of nodes
            return EntityList(all_nodes)

        except Exception as e:
            self.logger.error(
                message=f"An error occurred while querying for instances: {e}",
                section="END",
            )
            return EntityList()  # Return an empty list on error
    
class RetrieveServiceRAW(IRetrieveService):
    def __init__(
            self,
            client: CogniteClient,
            config: Config,
            logger: CogniteFunctionLogger
    ):
        self.client=client
        self.config=config
        self.logger=logger

    def get_matches(self, job: ContextualizationJob, true_matches: bool=False) -> MatchResultList:
        """Gets the matches from the job_id and returns a MatchResult list. True matches to flag the filter on true matches"""

        results=job.result['items']
        match_results: list[MatchResult]=[]

        if results is not None:
            for result in results:
                try:
                    match_result=MatchResult.load(result)
                    valid_matches: list[MatchItem]=[]

                    if true_matches:
                        for match in match_result.matches:
                            if match.score >= self.config.contextualization_config.true_match_threshold:
                                valid_matches.append(match)
                    else:
                        valid_matches.extend(match_result.matches)
                    
                    match_results.append(MatchResult(
                        source=match_result.source,
                        matches=valid_matches
                    ))
                except:
                    print(f"Unable to load Match Result with data: {result}")
            
        return MatchResultList(match_results)
    
    def get_matches_raw(self, true_matches: bool=False) -> pd.DataFrame:
        """Gets matches from a table in CDF RAW"""
        table: Table=self.config.contextualization_config.match_result_table if not true_matches else self.config.contextualization_config.true_matches_table


        if table.table_name not in self.client.raw.tables.list(db_name=table.database_name, limit=None).as_names():
            self.logger.info(
                message=f"Table with name \'{table.table_name}\' not found in database \'{table.database_name}\', returning empty list"
            )
            
            return pd.DataFrame()
        else:
            rows=self.client.raw.rows.retrieve_dataframe(db_name=table.database_name, table_name=table.table_name, limit=None)
            return rows.dropna().replace([-np.inf, np.inf, np.nan, math.nan], '')