# Intended usage


## Local

 To write out db to folder as formatted create scripts: 
 `gitsnow db-to-folder --scripts-dir schemas --db-name MyDatabase`

To create a script to deploy changes in dependency order:
 `gitsnow folder-to-script --scripts-dir schemas --db-name MyDatabase --output-file last_deployment.sql`

 ## Snowflake - todo

To push a commit with any db drift: `gitsnow_push_drift('@your_git_stage/branches/main/schemas')`
To generate a script to apply the changes from main: `gitsnow_script_pull('@your_git_stage/branches/main/schemas')`