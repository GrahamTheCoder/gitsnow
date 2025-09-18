# gitsnow

Easily sync objects from git to snowflake (deployment), and from snowflake to git (drift detection and reintegration).
This tool does not preserve case, and hence will not work correctly when quoted_identifiers_ignore_case is set to false.

## Usage

All commands require a `--scripts-dir` option that specifies the directory containing your SQL scripts. The sqlfluff configuration will be automatically read from the parent directory of the scripts directory (looking for `.sqlfluff` file).

### Basic structure
```
project/
├── .sqlfluff              # Configuration file (automatically detected)
└── schemas/               # Scripts directory (passed to --scripts-dir)
    └── my_schema/
        ├── tables/
        │   └── customers.sql
        └── views/
            └── customer_summary.sql
```

### Commands

All commands follow this pattern:
```bash
gitsnow --scripts-dir <path-to-scripts> <command> [command-options]
```

## Debug

To write out db to folder as formatted create scripts:
 `uv run python -m debugpy --listen localhost:5678 -m cli.cli --scripts-dir schemas db-to-folder --db-name MyDatabase`

To create a script to deploy changes in dependency order:
 `uv run python -m debugpy --listen localhost:5678 -m cli.cli --scripts-dir schemas folder-to-script --db-name MyDatabase --output-file last_deployment.sql`

## Intended usage

The primary commands to sync changes will be...

### Local

To write out db to folder as formatted create scripts:
`gitsnow --scripts-dir schemas db-to-folder --db-name MyDatabase`

To create a script to deploy changes in dependency order:
 `gitsnow --scripts-dir schemas folder-to-script --db-name MyDatabase --output-file last_deployment.sql`

### Snowflake - todo

To push a commit with any db drift: `gitsnow_push_drift('@your_git_stage/branches/main/schemas')`
To generate a script to apply the changes from main: `gitsnow_script_pull('@your_git_stage/branches/main/schemas')`