import click
from pathlib import Path

from . import db
from .db_mock import get_mock_connection
from .dependencies import get_all_dependency_information
from .format import format_sql
from .diff import get_semantic_changed_files, get_db_object_details
from .container import configure_services
from sqlfluff.core import Linter, FluffConfig

config = FluffConfig(overrides={"dialect": "snowflake"})
linter = Linter(config=config)

@click.group()
@click.option('--scripts-dir', required=True, type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
              help="Directory containing the SQL scripts. Configuration will be read from its parent directory.")
@click.pass_context
def cli(ctx, scripts_dir):
    """A CLI tool for Snowflake DevOps."""
    # Ensure that ctx.obj exists and is a dict (it will be passed to subcommands)
    ctx.ensure_object(dict)
    
    # Configure services with the parent directory of scripts_dir for config
    config_path = scripts_dir.parent / '.sqlfluff'
    configure_services(config_path)
    
    # Store scripts_dir in context for use by subcommands
    ctx.obj['scripts_dir'] = scripts_dir

@cli.command(name='db-to-folder')
@click.option('--db-name', envvar='SNOWFLAKE_DATABASE', required=True, help="Snowflake database name.")
@click.option('--schema', 'schemas', multiple=True, help="Specific schema(s) to export. Can be used multiple times. If not provided, all schemas are exported.")
@click.option('--test', is_flag=True, help="Use a mock connection for testing.")
@click.pass_context
def db_to_folder(ctx, db_name, schemas, test):
    """Export all DB objects' canonical DDL into files under the output folder."""
    scripts_dir = ctx.obj['scripts_dir']
    output_path = Path(scripts_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    conn = db.get_connection() if not test else get_mock_connection()

    try:
        if not schemas:
            schemas = db.get_all_schemas(conn, db_name)

        click.echo(f"Exporting objects from database '{db_name}' to '{scripts_dir}'...")

        for schema_name in schemas:
            objects = db.get_objects_in_schema(conn, db_name, schema_name)
            for obj in objects:
                obj_type_dir = output_path / schema_name.lower() / (obj.type.lower() + 's')
                obj_type_dir.mkdir(parents=True, exist_ok=True)
                formatted_ddl = format_sql(obj.ddl)
                file_path = obj_type_dir / f"{obj.name.lower()}.sql"
                file_path.write_text(formatted_ddl)
                click.echo(f"  - Wrote {file_path}")

        click.echo("Export complete.")
    except Exception as e:
        raise click.ClickException(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

@cli.command(name='folder-to-script')
@click.option('--db-name', envvar='SNOWFLAKE_DATABASE', required=True, help="Snowflake database name.")
@click.option('--output-file-prefix', required=True, type=click.Path(dir_okay=False), help="Prefix for the .up.sql and .down.sql files.")
@click.option('--test', is_flag=True, help="Use a mock connection for testing.")
@click.pass_context
def folder_to_script(ctx, db_name, output_file_prefix, test):
    """Generate up, down, and base SQL scripts for objects that changed compared to the folder."""
    scripts_dir = ctx.obj['scripts_dir']
    scripts_path = Path(scripts_dir)

    conn = db.get_connection() if not test else get_mock_connection()
    conn.execute_string(f"USE DATABASE {db_name}")
    try:
        ordered_objects, dependents_by_obj = get_all_dependency_information(scripts_path)
        ordered_obj_paths = [(obj_name, path) for (obj_name, path, _) in ordered_objects]
        click.echo(f"Found {len(ordered_obj_paths)} folder objects.")

        schemas = db.get_all_schemas(conn, db_name)
        db_objects = {obj.schema_qualified_name.upper(): obj for schema in schemas for obj in db.get_objects_in_schema(conn, db_name, schema)}
        click.echo(f"Found {len(db_objects)} database objects.")
                
        changed_files = get_semantic_changed_files(ordered_obj_paths, list(db_objects.values()), scripts_path)

        if not changed_files:
            click.echo("No changes detected. Database is in sync with scripts.")
            return

        click.echo(f"\nFound {len(changed_files)} changed objects to deploy.")

        # Identify dependent dynamic tables
        changed_obj_names = {change['obj_name'].upper() for change in changed_files}
        dependent_dynamic_tables = set()
        for obj_name in changed_obj_names:
            for dependent in dependents_by_obj.get(obj_name, []):
                if db_objects.get(dependent) and db_objects[dependent].type == "DYNAMIC TABLE":
                    dependent_dynamic_tables.add(dependent)

        # Fetch DDL for dependent dynamic tables
        dependent_ddls = {name: db_objects[name].ddl for name in dependent_dynamic_tables if name in db_objects}

        up_script_path = Path(output_file_prefix).with_suffix('.up.sql')
        down_script_path = Path(output_file_prefix).with_suffix('.down.sql')
        base_script_path = Path(output_file_prefix).with_suffix('.base.sql')

        # Generate .up.sql
        with open(up_script_path, 'w', encoding='utf-8') as f_up:
            f_up.write("-- UP script generated by Snowflake DevOps Tools\n")
            f_up.write("-- All changes in dependency order, scripts inlined\n\n")

            schemas = set(change['path'].parent.parent.name for change in changed_files)
            for schema in schemas:
                f_up.write(f"CREATE SCHEMA IF NOT EXISTS {schema};\n")
            f_up.write("\n")

            for change in changed_files:
                relative_path = change['path'].relative_to(scripts_path.parent)
                f_up.write(f"-- Object: {relative_path}\n")
                f_up.write(change['file_sql'])
                f_up.write("\n\n")

            if dependent_ddls:
                f_up.write("-- Refresh depending objects\n")
                for name, ddl in dependent_ddls.items():
                    f_up.write(f"-- Refreshing: {name}\n")
                    f_up.write(ddl)
                    f_up.write("\n\n")

        # Generate .down.sql
        with open(down_script_path, 'w', encoding='utf-8') as f_down:
            f_down.write("-- DOWN script generated by Snowflake DevOps Tools\n")
            f_down.write("-- Reverts changes in reverse dependency order\n\n")
            for change in reversed(changed_files):
                relative_path = change['path'].relative_to(scripts_path.parent)
                f_down.write(f"-- Object: {relative_path}\n")
                if change['db_sql']:
                    f_down.write(change['db_sql'])
                else:
                    obj_type, obj_name = get_db_object_details(change['file_sql'])
                    f_down.write(f"DROP {obj_type} IF EXISTS {obj_name};\n")
                f_down.write("\n\n")

            if dependent_ddls:
                f_down.write("-- Refresh depending objects\n")
                for name, ddl in dependent_ddls.items():
                    f_down.write(f"-- Refreshing: {name}\n")
                    f_down.write(ddl)
                    f_down.write("\n\n")

        # Generate .base.sql
        with open(base_script_path, 'w', encoding='utf-8') as f_base:
            f_base.write("-- BASE script generated by Snowflake DevOps Tools\n")
            f_base.write("-- Current DDL of changed objects from the database\n\n")
            for change in changed_files:
                relative_path = change['path'].relative_to(scripts_path.parent)
                f_base.write(f"-- Object: {relative_path}\n")
                if change['db_sql']:
                    f_base.write(change['db_sql'])
                else:
                    f_base.write("-- Object does not exist in the database\n")
                f_base.write("\n\n")

        click.echo(f"\nUp script written to '{up_script_path}'.")
        click.echo(f"Down script written to '{down_script_path}'.")
        click.echo(f"Base script written to '{base_script_path}'.")

    except Exception as e:
        raise click.ClickException(f"An error occurred: {e}")
    finally:
        if conn and not test:
            conn.close()

@cli.command(name='show-dependencies')
@click.option('--ignore-prefixes', default="", show_default=True, help="Comma-separated list of schema prefixes to ignore for no-dependencies output.")
@click.option('--upper-case', default="False", show_default=False, help="Whether to show object names in upper case.")
@click.pass_context
def show_dependencies(ctx, ignore_prefixes, upper_case):
    """
    Output the dependency graph in plain text.
    """
    scripts_dir = ctx.obj['scripts_dir']
    scripts_path = Path(scripts_dir)
    dependency_ordered_objects = get_dependency_ordered_objects(scripts_path)
    if not upper_case:
        dependency_ordered_objects = [
            (obj.lower(), path, [dep.lower() for dep in dependencies])
            for obj, path, dependencies in dependency_ordered_objects
        ]

    for obj, _, dependencies in dependency_ordered_objects:
        if dependencies:
            click.echo(f"{obj}:")
            for dep in dependencies:
                click.echo(f"  - {dep}")
            click.echo()

    schema_prefixes_to_ignore_no_dependants = tuple(prefix.strip() for prefix in ignore_prefixes.split(',') if len(prefix.strip()))
    click.echo("Unreferenced objects:")
    zero_references = set(obj for obj, _, _ in dependency_ordered_objects) - set(dep for _, _, deps in dependency_ordered_objects for dep in deps)
    for obj in zero_references:
        if not obj.startswith(schema_prefixes_to_ignore_no_dependants):
            click.echo(f"  - {obj}")

    obj_to_path = {obj: path for obj, path, _ in dependency_ordered_objects}
    referenced_deps = set(dep for _, _, deps in dependency_ordered_objects for dep in deps)

    unknown_deps = set()
    for dep in referenced_deps:
        path = obj_to_path.get(dep)
        # Treat missing mapping or falsy path (e.g. None) as unknown
        if not path:
            unknown_deps.add(dep)

    if unknown_deps:
        click.echo("\nReferenced dependencies with no known path:")
        for dep in sorted(unknown_deps):
            click.echo(f"  - {dep}")

if __name__ == '__main__':
    cli()
