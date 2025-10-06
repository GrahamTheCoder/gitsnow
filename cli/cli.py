import click
from pathlib import Path

from . import db
from .db_mock import get_mock_connection
from .dependencies import get_dependency_ordered_objects
from .format import format_sql
from .diff import get_semantic_changed_files, semantic_diff, get_objects_from_files, get_db_object_details
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

    conn = db.get_connection(db_name) if not test else get_mock_connection()

    try:
        if not schemas:
            schemas = db.get_all_schemas(conn, db_name)

        click.echo(f"Exporting objects from database '{db_name}' to '{scripts_dir}'...")

        for schema_name in schemas:
            objects = db.get_objects_in_schema(conn, db_name, schema_name)
            
            # Group objects by (type, name) to handle overloaded functions/procedures
            from collections import defaultdict
            grouped_objects = defaultdict(list)
            for obj in objects:
                key = (obj.type.lower(), obj.name.lower())
                grouped_objects[key].append(obj)
            
            # Write each group to a single file
            for (obj_type, obj_name), obj_group in grouped_objects.items():
                obj_type_dir = output_path / schema_name.lower() / (obj_type + 's')
                obj_type_dir.mkdir(parents=True, exist_ok=True)
                
                # Sort objects by their args for consistency (None/empty args first)
                obj_group.sort(key=lambda o: (o.ddl if hasattr(o, 'ddl') else '', ''))
                
                # Format and combine DDLs with triple newline separator
                formatted_ddls = [format_sql(obj.ddl) for obj in obj_group]
                combined_ddl = '\n\n\n'.join(formatted_ddls)
                
                file_path = obj_type_dir / f"{obj_name}.sql"
                file_path.write_text(combined_ddl)
                
                if len(obj_group) > 1:
                    click.echo(f"  - Wrote {file_path} ({len(obj_group)} overloads)")
                else:
                    click.echo(f"  - Wrote {file_path}")

        click.echo("Export complete.")
    except Exception as e:
        raise click.ClickException(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

@cli.command(name='folder-to-script')
@click.option('--db-name', envvar='SNOWFLAKE_DATABASE', required=True, help="Snowflake database name.")
@click.option('--output-file', required=True, type=click.Path(dir_okay=False), help="File to write the deployment SQL to.")
@click.option('--test', is_flag=True, help="Use a mock connection for testing.")
@click.pass_context
def folder_to_script(ctx, db_name, output_file, test):
    """Generate a SQL deployment script for objects that changed compared to the folder."""
    scripts_dir = ctx.obj['scripts_dir']
    scripts_path = Path(scripts_dir)

    conn = db.get_connection(db_name) if not test else get_mock_connection()
    conn.execute_string(f"USE DATABASE {db_name}")
    try:
        ordered_obj_paths = [(obj_name, path) for (obj_name, path, _) in get_dependency_ordered_objects(scripts_path)]
        click.echo(f"Found {len(ordered_obj_paths)} folder objects.")

        schemas = db.get_all_schemas(conn, db_name)
        db_objects = [obj for schema in schemas for obj in db.get_objects_in_schema(conn, db_name, schema)]
        click.echo(f"Found {len(db_objects)} database objects.")
                
        changed_files = get_semantic_changed_files(ordered_obj_paths, db_objects, scripts_path)
        # TODO: Bring back all semantically changed, and also recreate directly dependant objects
        # e.g. If a dynamic table changes, the ones depending on it sometimes need recreating

        if not changed_files:
            click.echo("No changes detected. Database is in sync with scripts.")
            return

        click.echo(f"\nFound {len(changed_files)} changed objects to deploy.")

        # Write the original deployment script (using EXECUTE IMMEDIATE FROM)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("-- Deployment script generated by Snowflake DevOps Tools\n")
            f.write("-- Changes to be applied in dependency order\n\n")
            for file_path in changed_files:
                relative_path = file_path.relative_to(scripts_path.parent)
                f.write(f"-- Deploying: {relative_path}\n")
                f.write(f"EXECUTE IMMEDIATE FROM '@/{relative_path}';\n\n")

        # Write the full inline deployment script
        full_output_file = Path(output_file).with_suffix('.full.sql')
        with open(full_output_file, 'w', encoding='utf-8') as f_full:
            f_full.write("-- FULL Deployment script generated by Snowflake DevOps Tools\n")
            f_full.write("-- All changes in dependency order, scripts inlined\n\n")
            schemas = set(file_path.parent.parent.name for file_path in changed_files)
            for schema in schemas:
                f_full.write(f"create schema if not exists {schema};\n")

            for file_path in changed_files:
                relative_path = file_path.relative_to(scripts_path.parent)
                f_full.write(f"-- Deploying: {relative_path}\n")
                script_text = file_path.read_text()
                f_full.write(script_text)
                f_full.write("\n\n")

        click.echo(f"\nDeployment script written to '{output_file}'.")
        click.echo(f"Full inline deployment script written to '{full_output_file}'.")
        click.echo("NOTE: You must upload the script files to a named stage (e.g. git stage) for the 'EXECUTE IMMEDIATE' script to work.")
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
