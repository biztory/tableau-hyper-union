import argparse
import logging, traceback
import logging.handlers # For RotatingFileHandler
import os, sys, datetime, time, glob, tempfile
from pathlib import Path
import tableauhyperapi as THA

# Timing
start_time = time.time()
start_datetime = datetime.datetime.utcnow()

# Logging
logger = logging.getLogger() # Root logger
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")
log_console_handler = logging.StreamHandler(sys.stdout)
log_console_handler.setFormatter(log_formatter)
logger.addHandler(log_console_handler)

# Command line arguments
parser = argparse.ArgumentParser(prog="tableau_hyper_union.py", description="Unions all .hyper files in the directory it's run from, into a single Hyper extract. Documentation available at: https://github.com/biztory/tableau-hyper-union")
parser.add_argument("--output-file", "-o", dest="output_file", required=False, default="union.hyper", type=str, help="The file to output to. Defaults to \"union.hyper\".")
parser.add_argument("--preserve-output-file", "-p", dest="preserve_output_file", default=False, action="store_true", help="When this argument is specified, the script will preserve the output of the output file and append to the existing contents. When not specified (i.e. the default behavior), it will first clear the contents of said output file.")
parser.add_argument("--source-file-column-name", "-c", dest="source_file_column_name", default="source_file", type=str, help="Used to add a column to each table, containing the name of the Hyper file the data was sourced from. The column can be omitted altogether by specifying an empty string here: \"\". Otherwise, the default is \"source_file\".")
parser.add_argument("--log-to-file", dest="log_to_file", default=False, action="store_true", help="Log the output of the program to a log file, and not just to the console. Useful for when the tool is used on a schedule.")
args = parser.parse_args()

# Logs directory
logs_directory = Path("logs").absolute()
if not logs_directory.exists() and args.log_to_file:
    os.makedirs(logs_directory)

# More logging, or not?
if args.log_to_file:
    log_file = logs_directory / "tableau_hyper_union.log"
    log_file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)
    log_file_handler.setFormatter(log_formatter)
    logger.addHandler(log_file_handler)

# Let's go
logger.info("Biztory tableau_hyper_union.py v0.1")
logger.info("Author: Timothy Vermeiren")
logger.info(f"Script launched using (quotes removed): { sys.executable } { sys.argv[0] } { ' '.join([a for i, a in enumerate(sys.argv[1:])]) }")

if not args.preserve_output_file:
    worklist = [hyper_file for hyper_file in glob.glob("*.hyper") if hyper_file != args.output_file]
    output_file = args.output_file
else:
    output_file = args.output_file.split(".")[-1] + "_temp.hyper"
    worklist = [hyper_file for hyper_file in glob.glob("*.hyper") if hyper_file != output_file]
    # We need a temporary output file if we're going to include it in the source itself
logger.info(f"Assimilated { len(worklist) } Hyper files to be processed.")

output_dict = {} # "Structure" is a dict of dicts of dicts going schema > table > column. Used mostly for comparing.
output_dict_definitions = {} # Same as the above, but contains the definitions to actually create the things.

if args.log_to_file:
    hyper_process_parameters = { "log_dir": str(logs_directory) }
else:
    # We need to log the Hyper output _somewhere_
    temp_log_dir = tempfile.gettempdir()
    hyper_process_parameters = { "log_dir": str(temp_log_dir) }

with THA.HyperProcess(telemetry=THA.Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=hyper_process_parameters) as hyper:

    for hyper_file in worklist:
        try:
            with THA.Connection(endpoint=hyper.endpoint, database=hyper_file) as connection:
                logger.info(f"Processing database/file { hyper_file }:")

                # Schemas are the top level
                for schema in connection.catalog.get_schema_names():
                    logger.info(f"\t- Schema { schema }:")
                    if schema not in output_dict:
                        # SchemaDefinition as key, because why not
                        output_dict[schema] = {}

                    # Then tables
                    for table in connection.catalog.get_table_names(schema=schema):
                        logger.info(f"\t\t- Table { table }")
                        if table not in output_dict[schema]:
                            # TableDefinition as key, because why not. We'll make this a list though because its contents, column, is at the lowest level
                            output_dict[schema][table] = []
                        table_definition = connection.catalog.get_table_definition(name=table)
                        logger.info(f"\t\t\t{ table_definition.column_count } columns in table.")

                        # Then columns
                        for column in table_definition.columns:
                            logger.debug(f"\t\t\t- Column {column.name} has type={column.type} and nullability={column.nullability}")
                            # If the column doesn't exist, we simply add it
                            if column.name not in [column.name for column in output_dict[schema][table]]:
                                output_dict[schema][table].append(column) # Uh, okay.
                            else:
                                # Two possibilities...
                                matching_column_in_output_dict = [column_output for column_output in output_dict[schema][table] if column_output.name == column.name][0]
                                if column.type != matching_column_in_output_dict.type or column.nullability != matching_column_in_output_dict.nullability or column.collation != matching_column_in_output_dict.collation:
                                    # If it does exist but doesn't match data type or nullability, we'll be in trouble. Simply discard it for now, we can think of alternative approaches later
                                    logger.warning(f"\t\t\tFound matching column { column }, but it doesn't have the same properties as the existing column. This might cause unexpected results in the output, such as missing data or a general failure. (Existing: { matching_column_in_output_dict.type }/{ matching_column_in_output_dict.nullability }/{matching_column_in_output_dict.collation}) != ({hyper_file}: { column.type }/{ column.nullability }/{column.collation})")
                                else:
                                    logger.debug("Column exists in output already.")
        except Exception as e:
            logger.error(f"There was a problem reading data from the file { hyper_file }. The error returned was:\n\t{e}\n\t{traceback.format_exc()}")
            input("Press Enter to continue...")

        logger.info("The connection to the Hyper file has been closed.")
    
    logger.info("Assimilated aforementioned files. Creating definitions and applying in output file.")

    try:
        with THA.Connection(endpoint=hyper.endpoint, database=output_file, create_mode=THA.CreateMode.CREATE_AND_REPLACE) as connection_output:

            for schema in output_dict:

                connection_output.catalog.create_schema_if_not_exists(schema=schema)
                
                for table in output_dict[schema]:

                    try:

                        # Add a column to this table, specifying the source file name, if we opted to do so. And if it didn't exist yet.
                        if len(args.source_file_column_name) > 0 and len([column for column in output_dict[schema][table] if THA.escape_name(column.name) == THA.escape_name(args.source_file_column_name)]) < 1:
                            output_dict[schema][table].append(THA.TableDefinition.Column(name=args.source_file_column_name, type=THA.SqlType.text(), nullability=THA.Nullability.NOT_NULLABLE))

                        table_definition = THA.TableDefinition(
                            table_name=THA.TableName(table),
                            columns=[
                                THA.TableDefinition.Column(
                                    name=column.name,
                                    type=column.type,
                                    # nullability=column.nullability,
                                    nullability=THA.Nullability.NULLABLE, # We might argue that by definition, all columns have to be nullable because we're creating a union and we don't know if columns are always present!
                                    collation=column.collation
                                ) for column in output_dict[schema][table]
                            ]
                        )

                        connection_output.catalog.create_table_if_not_exists(table_definition=table_definition)
                        logger.info(f"Table { table } created in output file. Inserting data from source files.")

                    except Exception as e:
                        logger.error(f"There was a problem defining and creating the target table { table }. The error returned was:\n\t{e}\n\t{traceback.format_exc()}")
                        input("Press Enter to continue...")

                    for hyper_file in worklist:

                        try:
                            with THA.Connection(endpoint=hyper.endpoint, database=hyper_file) as connection:
                                logger.info(f"\tIngesting database/file { hyper_file }.")
                                if schema in connection.catalog.get_schema_names() and table in connection.catalog.get_table_names(schema=schema):
                                    # Build the query in a very... "special" way. I wonder how robust this is.
                                    try:
                                        query = "SELECT "
                                        for column in table_definition.columns:
                                            if column.name in [column.name for column in connection.catalog.get_table_definition(name=table).columns] and THA.escape_name(column.name) != THA.escape_name(args.source_file_column_name):
                                                # Source extract has this column (too)
                                                query += f" { THA.escape_name(column.name) },"
                                            elif THA.escape_name(column.name) != THA.escape_name(args.source_file_column_name):
                                                # Source extract does not have this column
                                                query += f" NULL as { THA.escape_name(column.name) },"
                                        if len(args.source_file_column_name) > 0: # If we must add the file name
                                            if hyper_file != args.output_file:
                                                query += f" '{ hyper_file }' as { args.source_file_column_name },"
                                            else:
                                                query += f" { THA.escape_name(args.source_file_column_name) } as { args.source_file_column_name },"
                                        # Pinch off the last comma we added (I know, it's dumb, but it works)
                                        query = query[:-1]
                                        query += f" FROM { THA.escape_name(schema.name) }.{ THA.escape_name(table.name) }"
                                        logger.debug(f"\t\tResulting query:{ query }")
                                    except Exception as e:
                                        logger.error(f"There was a problem building the query to read the data from table { table }in file { hyper_file }. The error returned was:\n\t{e}\n\t{traceback.format_exc()}")
                                        if query in locals():
                                            logger.error(f"The query we built so far was:\n\t{ query }")
                                        input("Press Enter to continue...")
                                    try:
                                        rows_to_insert = connection.execute_list_query(query=query)
                                    except Exception as e:
                                        logger.error(f"There was a problem executing the query to read data from { table } in file { hyper_file }. The error returned was:\n\t{e}\n\t{traceback.format_exc()}")
                                        input("Press Enter to continue...")
                                    try:
                                        with THA.Inserter(connection_output, table_definition) as inserter:
                                            inserter.add_rows(rows=rows_to_insert)
                                            inserter.execute()
                                    except Exception as e:
                                        logger.error(f"There was a problem inserting data from table { table } from file { hyper_file }. The error returned was:\n\t{e}\n\t{traceback.format_exc()}")
                                        input("Press Enter to continue...")
                                    logger.info(f"\t\tInserted { len(rows_to_insert) } rows from this file.")
                                    
                                else:
                                    logger.info("Table does not appear in this Hyper file; skipping.")
                        
                        except Exception as e:
                            logger.error(f"There was a problem assimilating the file { hyper_file } into the extract. The error returned was:\n\t{e}\n\t{traceback.format_exc()}")
                            input("Press Enter to continue...")
        
        if output_file != args.output_file:
            logger.info("We wrote the output to a temporary file to also assimilate the original output file's content. Cleaning up.")
            os.remove(args.output_file)
            os.rename(output_file, args.output_file)

    except Exception as e:
        logger.error(f"There was a problem accessing/creating/approaching the output file { args.output_file }. Perhaps the file is still open in another process, potentially Tableau? The error returned was:\n\t{e}\n\t{traceback.format_exc()}")
        input("Press Enter to continue...")

logger.info(f"The Hyper process has been shut down. Hypa hypaaaa!")

# Hyper Hyper: https://www.youtube.com/watch?v=7Twnmhe948A
# But also, Hypa Hypa: https://www.youtube.com/watch?v=75Mw8r5gW8E