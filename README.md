# fn_standard_entity_matching
This function performs entity matching between two sets of instances (sources and targets) within Cognite Data Fusion (CDF). It leverages CDF's contextualization services to find matches based on specified fields and can operate in both unsupervised and supervised modes. The results (matches and true_matches) of the matching job are stored in their respective specified CDF Raw table.

## Streamlit Apps

### Entity Matching Results Analyzer
This streamlit app is under the name 'Entity Matching Results Display' in CDF. It serves as a front-end for viewing match results of a previous entity matching call, and adding a select number of those results to another table, usually a table that holds true matches for a future run. This is to assist in 'training' the model, allowing the operator to decide which matches are true if previous matching runs missed any. More information and instructions can be found on the app itself.

### State Generator
This streamlit app is under the name 'Entity Matching Create Run' in CDF. It serves as a front-end for creating states with valid configs and adding them to the state store that this function pulls states from. More details on this app can be found on the app itself.

## Configuration
The function is controlled by a main ```Config``` object that nests several other configuration models. Below is a detailed breakdown of each configuration class and its parameters.

If a table name is configured (for ```matches``` or ```true_matches```) and the table name does not exist within the CDF database yet, the function will create a table with the configured name in the configured database. If no name is provided, the function will create a table with the name of the ```job id``` the function creates during the contextualization process (```<job_id>_true_matches``` for the table holding true matches).

### Config
The main configuration object for the entity matching function.

* **source_config** (```InstanceConfig```): The configuration for retrieving source instances from CDF.
* **target_config** (```InstanceConfig```): The configuration for retrieving target instances from CDF.
* **contextualization_config** (```ContextualizationConfig```): The configuration for the matching endpoint.
* **log_level** (```str```): The log level. Defaults to "INFO".

### InstanceConfig
Defines how to query for source or target instances within CDF.

* **instance_type** (```str```): Whether to query for nodes or edges. You can also pass a custom typed node (or edge class) inheriting from TypedNode (or ```TypedEdge```). Defaults to "node" (this is usually going to be "node").
* **view_id** (```ViewId```): The ID of the view to pull instances from.
* **instance_space** (```str```): The instance space to retrieve from (only supports one space for memory limitations).
* **fields_to_contextualize** (```list[str]```): The fields to match on. This property on source and target config will be used to create a cross product of properties that will be fed to the fit() endpoint. Defaults to ```["aliases]```.
* **fields_to_pull** (```list[str]```): The fields to include in the query for instances. This list is auto populated with ```fields_to_contextualize```
* **Tags to filter** (```list[str]```): The tags that should exist on the instance for it to be pulled. Leave this field as ```None``` to disable filtering

### ContextualizationConfig
Configures the behavior of the entity matching model in CDF.

* **num_matches** (```int```): The number of matches the model should predict for each source. Defaults to 1.
* **score_threshold** (```float```): The minimum score a match must have to be considered a match. Defaults to 0.5.
* **supervised_config** (```SupervisedConfig```): The configuration for supervised learning. If None, the model runs in unsupervised mode.
* **contextualization_model_config** (```ContextualizationModelConfig```): The configuration for the underlying model.
* **match_result_table** (```Table```): The table to hold the match results.
* **true_matches_table** (```Table```): The table that holds the true matches, which is used for supervised learning.
* **true_match_threshold** (```float```): The true score threshold for the model. Defaults to 1.0.
* **write_true_matches** (```bool```): Whether to write true matches back to CDF RAW. If set to true, the function deletes all existing true matches in the true_matches_table before it writes the new ones

### ContextualizationModelConfig
Defines the parameters for the contextualization model itself.

* **feature_type** (```str```): The feature type to use when matching tokens. Defaults to "bigram".
* **timeout** (```int```): The timeout to set in minutes for API responses. This is helpful for large matching jobs. Optional

### SupervisedConfig
Configures a supervised entity matching model, allowing for training and refitting.
**NOTE:** The function will run in supervised mode **if** and only **if** the **id** or **external_id** of the model is populated.

* **id** (```int```): The supervised model's internal ID. Optional
* **external_id** (```str```): The supervised model's external ID. Optional

### Table
Defines a table in CDF Raw.

* **database_name** (```str```): The name of the database. Defaults to "db_entity_matching_job_result".
* **table_name** (```str```): The name of the table. Optional

### ViewId
Defines a View in the CDF Data Model.

* **space** (```str```): The space of the view to retrieve.
* **external_id** (```str```): The external ID of the view to retrieve.
* **version** (```str```): The version of the view to retrieve.

## Configuration Examples
See the following YAML files for examples on supervised and unsupervised learning

### Supervised
Notice that ```supervised_config``` is populated:
```
target_config:
  instance_type: node
  view_id:
    space: {{target_view_space}}
    external_id: {{target_view_external_id}}
    version: {{target_view_version}}
  instance_space: {{target_instance_space}}
  fields_to_pull: {{target_pull_fields}},
  fields_to_contextualize : {{target_contextualize_fields}}

source_config:
  instance_type: node
  view_id:
    space: {{source_view_space}}
    external_id: {{source_view_external_id}}
    version: {{source_view_version}}
  instance_space: {{source_instance_space}}
  fields_to_pull: {{source_pull_fields}},
  fields_to_contextualize : {{source_contextualize_fields}}

contextualization_config:
  num_matches: {{num_matches}}
  score_threshold: {{match_score_threshold}}
  true_match_threshold: {{true_match_score_threshold}}
  write_true_matches: {{write_true_matches}}
  true_matches_table:
    database_name: {{true_matches_database_name}}
    table_name: {{true_matches_table_name}}
  supervised_config:
    id: {{supervised_model_id}}
    external_id: {{supervised_model_external_id}}

  contextualization_model_config:
    feature_type: {{feature_type}}
    match_fields: {{match_fields}}
    timeout: {{model_timeout}}

  match_result_table:
    database_name: {{matches_database_name}}
    table_name: {{matches_table_name}}

log_level: {{log_level}}
```

### Unsupervised
Notice that ```supervised_config``` is absent:
```
source_config:
  instance_type: node
  view_id:
    space: {{target_view_space}}
    external_id: {{target_view_external_id}}
    version: {{target_view_version}}
  instance_space: {{target_instance_space}}
  fields_to_pull: {{target_pull_fields}},
  fields_to_contextualize : {{target_contextualize_fields}}

target_config:
  instance_type: node
  view_id:
    space: {{source_view_space}}
    external_id: {{source_view_external_id}}
    version: {{source_view_version}}
  instance_space: {{source_instance_space}}
  fields_to_pull: {{source_pull_fields}},
  fields_to_contextualize : {{source_contextualize_fields}}

contextualization_config:
  num_matches: {{num_matches}}
  score_threshold: {{match_score_threshold}}
  true_match_threshold: {{true_match_score_threshold}}
  write_true_matches: {{write_true_matches}}
  true_matches_table:
    database_name: {{true_matches_database_name}}
    table_name: {{true_matches_table_name}}
  
  contextualization_model_config:
    feature_type: {{feature_type}}
    timeout: {{model_timeout}}
  
  match_result_table:
    database_name: {{matches_database_name}}
    table_name: {{matches_table_name}}

log_level: {{log_level}}
```

## Usage
This function's primary utility is to be implemented in a schedule or a data workflow. You may configure it to re-use an existing model that is trained over time via schedule or workflow.

### Set Up
Simply add the ```fn_standard_entity_matching``` module to your ```./modules``` directory in your toolkit repo, add ```fn_standard_entity_matching``` to the ```selected``` section of your config file, and run the following command ```cdf build | cdf deploy``` (only if you know what that does).

### Transformations
See the following example for a SQL query that pulls from a table and upserts instances based on the match:
```
WITH ParsedMatches AS (
    SELECT
        external_id_source,
        name_source,
  		space_source,
  		aliases_source,
  		external_id_target,
  		name_target,
  		space_target,
  		aliases_target,
  		score
    FROM
        `{{contextualization_config.match_database_name}}`.`{{contextualization_config.match_table_name}}`
)
  
SELECT
    external_id_source 								 AS externalId,
    node_reference(space_target, external_id_target) AS {{writeback_direct_link}},
  	space_source									 AS space
FROM
    ParsedMatches
WHERE
	score == {{writeback_threshold}} -- ONLY APPLY MATCHES THAT REACH THIS THRESHOLD
```

One reccomendation is to have a ```true_match_threshold``` variable in the ```fn_standard_entity_matching``` module in the config file. You can then replace ```0.9``` with ```true_match_threshold``` in the configurations for the function and transformations that consume the results

### Match Visualizer

The ***Entity Matching Results Display*** Streamlit App in CDF

There is a streamlit application included in this module that allows the user to view the results from a table and migrate them to another table (typically a true matches table that will be ingested by a future model). Use this app to categorize matches as 'true' without relying on a score threshold to be reached. Instructions for this app can be found on the app itself.

### Toolkit
#### Config File
See below for an example of toolkit variables in the config file for the  ```fn_standard_entity_matching``` module:
```
# ------ For the fn_standard_entity_matching call in workflow ------

  # target_instance_config
  target_view_space: 
  target_view_external_id: 
  target_view_version: 
  target_instance_space: 
  target_fields_to_pull:
      - description
  target_ields_to_contextualize:
      - name
      - aliases

  # source_instance_config
  source_view_space: 
  source_view_external_id: 
  source_view_version: 
  source_instance_space: 
  source_fields_to_pull:
      - description
  source_fields_to_contextualize:
      - name
      - aliases
  
  # contextualization_config
  num_matches: 1
  match_score_threshold: 0.7
  true_matches_database_name: 
  true_matches_table_name: 
  true_match_score_threshold: 0.9
  write_true_matches: true

  # supervised_config
  model_id: 123456789

  # contextualization_model_config
  feature_type: 'bigram'
  model_timeout: 120 

  matches_database_name: 
  matches_table_name: 
  log_level: DEBUG

  writeback_direct_link: 
  writeback_threshold: 0.9

  # ------------------------------------------------------------------
```
#### Workflow Version
```
externalId: ???
    dependsOn: 
      - externalId: ???
    parameters:
      function:
        externalId: fn_standard_entity_matching
        data : {
          "source_config": {
            "instance_type": "node",
            "view_id": {
              "space": {{source_view_space}},
              "external_id": {{source_view_external_id}},
              "version": {{source_view_version}}
            },
            "instance_space": {{source_instance_space}},
            "fields_to_pull": {{source_pull_fields}},
            "fields_to_contextualize" : {{source_contextualize_fields}}
          },
          
          "target_config": {
            "instance_type": "node",
            "view_id": {
              "space": {{target_view_space}},
              "external_id": {{target_view_external_id}},
              "version": {{target_view_version}}
            },
            "instance_space": {{target_instance_space}},
            "fields_to_pull": {{target_pull_fields}},
            "fields_to_contextualize" : {{target_contextualize_fields}}
          },

          "contextualization_config": {
            "num_matches": {{num_matches}},
            "score_threshold": {{match_score_threshold}},
            "true_matches_table":{
              "database_name": {{true_matches_database_name}},
              "table_name": {{true_matches_table_name}}
            },

            "true_match_threshold": {{true_match_score_threshold}},
            "write_true_matches": {{write_true_matches}},
            "supervised_config": {
              "id": {{supervised_model_id}},
            },

            "contextualization_model_config": {
              "feature_type": {{feature_type}},
              "timeout": {{model_timeout}}
            },

            "match_result_table": {
              "database_name": {{matches_database_name}},
              "table_name": {{matches_table_name}}
            }
          },
          "log_level": {{log_level}}
        }
        isAsyncComplete: false
    name: standard_entity_matching_task
    description: The task that runs the function for the entity matching in cdf
    timeout: 3600
    retries: 3
    onFailure: skipTask
    type: function
```
#### Schedule
```
source_config:
  instance_type: node
  view_id:
    space: {{target_view_space}}
    external_id: {{target_view_external_id}}
    version: {{target_view_version}}
  instance_space: {{target_instance_space}}
  fields_to_pull: {{target_pull_fields}},
  fields_to_contextualize : {{target_contextualize_fields}}

target_config:
  instance_type: node
  view_id:
    space: {{source_view_space}}
    external_id: {{source_view_external_id}}
    version: {{source_view_version}}
  instance_space: {{source_instance_space}}
  fields_to_pull: {{source_pull_fields}},
  fields_to_contextualize : {{source_contextualize_fields}}

contextualization_config:
  num_matches: {{num_matches}}
  score_threshold: {{match_score_threshold}}
  true_matches_table:
    database_name: {{true_matches_database_name}}
    table_name: {{true_matches_table_name}}
  true_match_threshold: {{true_match_score_threshold}}
  write_true_matches: {{write_true_matches}}
  supervised_config:
    id: {{supervised_model_id}}

  contextualization_model_config:
    feature_type: {{feature_type}}
    timeout: {{model_timeout}}

  match_result_table:
    database_name: {{matches_database_name}}
    table_name: {{matches_table_name}}

log_level: {{log_level}}
```