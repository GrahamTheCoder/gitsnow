from unittest.mock import MagicMock
import re

def get_mock_connection():
    """Returns a mock connection object for testing."""

    class MockCursor(MagicMock):
        def execute(self, query, *args, **kwargs):
            self._query = query
            # Set up fetchall and fetchone according to the query
            if "SHOW SCHEMAS" in query:
                self.fetchall.return_value = [
                    (None, 'MY_SCHEMA', None, None, None, None, None, None, None),
                ]
            elif "SHOW TABLES" in query:
                self.fetchall.return_value = [
                    (None, 'BASE_CUSTOMERS', None, None, None, None, None, None, None, None),
                    (None, 'BASE_ORDERS', None, None, None, None, None, None, None, None),
                    (None, 'RESERVED_KEYWORD_TEST', None, None, None, None, None, None, None, None),
                ]
            elif "SHOW VIEWS" in query:
                self.fetchall.return_value = [
                    (None, 'ENRICHED_ORDERS', None, None, None, None, None, None, None),
                    (None, 'AGG_CUSTOMER_ORDERS', None, None, None, None, None, None, None),
                    (None, 'FUNCTION_TEST_VIEW', None, None, None, None, None, None, None),
                ]
            elif "SHOW DYNAMIC TABLES" in query:
                self.fetchall.return_value = [
                    (None, 'DYNAMIC_TABLE_TEST', None, None, None, None, None, None, None),
                ]
            elif "SHOW PROCEDURES" in query:
                self.fetchall.return_value = []
            elif "SHOW USER FUNCTIONS" in query:
                self.fetchall.return_value = [
                    (None, 'MY_FUNCTION', None, None, None, None, None, None),
                ]
            elif "SHOW USER PROCEDURES" in query:
                self.fetchall.return_value = [
                    (None, 'MY_PROCEDURE', None, None, None, None, None, 'VARCHAR', None),
                ]
            elif "SHOW STREAMS" in query:
                self.fetchall.return_value = [
                    (None, 'MY_STREAM', None, None, None, None, None, None),
                ]
            elif "SHOW TASKS" in query:
                self.fetchall.return_value = [
                    (None, 'MY_TASK', None, None, None, None, None, None),
                ]
            elif "GET_DDL" in query:
                match = re.search(r"GET_DDL\('.*?', '(.*?)'\)", query)
                if match:
                    obj_name = match.group(1).lower()
                    if "base_customers" in obj_name:
                        # Unchanged - DDL will match the file
                        self.fetchone.return_value = ('CREATE OR REPLACE TABLE "MOCK_DB"."MY_SCHEMA"."BASE_CUSTOMERS" (ID INT, NAME VARCHAR);',)
                    elif "base_orders" in obj_name:
                        # Changed - DDL has an extra column
                        self.fetchone.return_value = ('CREATE TABLE "MOCK_DB"."MY_SCHEMA"."BASE_ORDERS" (ORDER_ID INT, CUSTOMER_ID INT, AMOUNT DECIMAL, EXTRA_COL INT);',)
                    elif "enriched_orders" in obj_name:
                        self.fetchone.return_value = ('create or replace view "MOCK_DB"."MY_SCHEMA"."ENRICHED_ORDERS"(response_set_id, response_id) as select o.*, c.name from "MOCK_DB"."MY_SCHEMA"."BASE_ORDERS" as o join "MOCK_DB"."MY_SCHEMA"."BASE_CUSTOMERS" as c on o.customer_id = c.id;',)
                    elif "agg_customer_orders" in obj_name:
                        self.fetchone.return_value = ('CREATE VIEW "MOCK_DB"."MY_SCHEMA"."AGG_CUSTOMER_ORDERS" AS SELECT CUSTOMER_ID, COUNT(*) FROM "MOCK_DB"."MY_SCHEMA"."ENRICHED_ORDERS" GROUP BY 1;',)
                    elif "function_test_view" in obj_name:
                        self.fetchone.return_value = ('CREATE VIEW "MOCK_DB"."MY_SCHEMA"."FUNCTION_TEST_VIEW" AS SELECT SUM(AMOUNT) FROM "MOCK_DB"."MY_SCHEMA"."BASE_ORDERS";',)
                    elif "reserved_keyword_test" in obj_name:
                        self.fetchone.return_value = ('CREATE TABLE "MOCK_DB"."MY_SCHEMA"."RESERVED_KEYWORD_TEST" ("ORDER" INT);',)
                    elif "dynamic_table_test" in obj_name:
                        self.fetchone.return_value = ('CREATE OR replace transient dynamic table "MOCK_DB"."MY_SCHEMA"."DYNAMIC_TABLE_TEST" LAG = \'1 MINUTE\' WAREHOUSE = \'MY_WH\' AS SELECT * FROM "MOCK_DB"."MY_SCHEMA"."BASE_TABLE";',)
                    elif "my_function" in obj_name:
                        self.fetchone.return_value = ('CREATE OR REPLACE FUNCTION "MOCK_DB"."MY_SCHEMA"."MY_FUNCTION"() RETURNS VARCHAR AS $$ SELECT \'Hello World\' $$;',)
                    elif "my_procedure" in obj_name:
                        self.fetchone.return_value = ('CREATE OR REPLACE PROCEDURE "MOCK_DB"."MY_SCHEMA"."MY_PROCEDURE"() RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN RETURN \'Hello\'; END $$;',)
                    elif "my_stream" in obj_name:
                        self.fetchone.return_value = ('CREATE OR REPLACE STREAM "MOCK_DB"."MY_SCHEMA"."MY_STREAM" ON TABLE "MOCK_DB"."MY_SCHEMA"."BASE_CUSTOMERS";',)
                    elif "my_task" in obj_name:
                        self.fetchone.return_value = ('CREATE OR REPLACE TASK "MOCK_DB"."MY_SCHEMA"."MY_TASK" WAREHOUSE = \'MY_WH\' SCHEDULE = \'USING CRON 0 0 * * * UTC\' AS SELECT 1;',)
                    else:
                        self.fetchone.return_value = (f'-- MOCK DDL for {query}',)
                else:
                    self.fetchone.return_value = (f'-- MOCK DDL for {query}',)
            else:
                self.__iter__.return_value = []
                self.fetchone.return_value = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class MockConnection(MagicMock):
        def cursor(self, *args, **kwargs):
            return MockCursor()

    return MockConnection()
