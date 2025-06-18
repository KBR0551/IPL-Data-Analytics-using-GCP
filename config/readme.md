In the SQL files I used Jinja templating syntax, ```"{{ params.project_id }}.{{ params.tgt_dataset_name }}``` which will allow us to dynamically pass the dataset/databasename from air flow,
so that one change what ever name they want(avoid hardcoding) in org's the dataset names change based on the environments (DEV/SIT/UAT/PROD etc) see jinja templating for air flow.
