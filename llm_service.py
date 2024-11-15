import sqlite3
import logging
import requests

class LLMService:
    def __init__(self, api_key, db_path):
        self.api_key = api_key
        self.db_path = db_path
        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _get_table_schema(self, table_name):
        """Fetch the schema of the given table from SQLite."""
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()

            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            connection.close()

            if not columns:
                self.logger.warning(f"No columns found for table: {table_name}")
                return []

            schema = [f"{column[1]} {column[2]}" for column in columns]
            return schema
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching schema for table {table_name}: {e}")
            return []

    def _get_possible_tables(self):
        """Fetch a list of tables from the SQLite database."""
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            connection.close()

            if not tables:
                self.logger.warning("No tables found in the database.")
            return [table[0] for table in tables]
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching tables from database: {e}")
            return []

    def _infer_tables_from_query(self, natural_query):
        """Analyze the query and identify the relevant tables and relationships."""
        # Check for tables mentioned in the query
        possible_tables = self._get_possible_tables()
        relevant_tables = []

        if "company" in natural_query.lower():
            relevant_tables.append("companies")
        if "location" in natural_query.lower():
            relevant_tables.append("locations")
        
        # Return the identified tables
        return relevant_tables

    def convert_to_sql_query(self, natural_query):
        """Convert natural language to SQL query."""
        table_name = self._infer_table_from_query(natural_query)
        
        if not table_name:
            return {"error": "Could not infer table name."}

        # Dynamically fetch schema for the inferred table
        schema = self._get_table_schema(table_name)
        if not schema:
            return {"error": f"Could not fetch schema for table {table_name}."}

        schema_str = "\n".join([f"- {field}" for field in schema])

        system_prompt = f"""
        You are a helpful assistant that converts natural language queries about a SQLite database schema to valid SQL queries.
        Please check the following:
        1. Which tables should be used based on the parameters in the query?
        2. If a parameter is missing from the first table, find it in the related tables.
        3. Join the relevant tables using the foreign keys, such as company_id, location_id, etc.
        4. Generate a valid SQL query to fetch the required data.

        Here is the schema of the database for the '{table_name}' table:
        {schema_str}
        
        Convert the following natural language query into a valid SQL query:
        '{natural_query}'
        """

        payload = {
            "model": "llama3-8b-8192",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": natural_query}
            ],
            "max_tokens": 500,
            "temperature": 0.1
        }

        try:
            self.logger.info(f"Sending request to Groq API for query: {natural_query}")
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()

            self.logger.info(f"Received response from Groq API: {result}")

            if 'choices' in result and len(result['choices']) > 0:
                content = result['choices'][0]['message']['content']
                sql_query = self._extract_sql_query(content)
                if sql_query:
                    return sql_query
                else:
                    return {"error": "No valid SQL query extracted from response"}
            else:
                return {"error": "No response content from LLM"}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request error: {str(e)}")
            return {"error": f"API request failed: {str(e)}"}


    def convert_to_sql_query(self, natural_query):
        """Convert natural language to SQL query."""
        relevant_tables = self._infer_tables_from_query(natural_query)
        if not relevant_tables:
            return {"error": "Could not infer relevant tables from the query."}

        # Fetch schemas for the relevant tables
        schemas = {}
        for table in relevant_tables:
            schemas[table] = self._get_table_schema(table)
        
        # Prepare the schema details to send to Llama
        schema_str = "\n".join(
            [f"Table '{table}' has columns: {', '.join([col.split()[0] for col in schema])}" for table, schema in schemas.items()]
        )

        # Construct a detailed system prompt for Llama
        system_prompt = f"""
        You are a helpful assistant that converts natural language queries about a SQLite database schema to valid SQL queries.
        Here is the schema of the database:

        {schema_str}
        
        Relationships between tables:
        - The 'companies' table has columns: id, company_name.
        - The 'locations' table has columns: id, company_id, location_name.
        The common column between these two tables is 'company_id'.

        Based on the above schema and relationships, convert the following natural language query into a valid SQL query:

        {natural_query}
        """

        # Send the query to the Llama model via the Groq API
        payload = {
            "model": "llama3-8b-8192",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": natural_query}
            ],
            "max_tokens": 500,
            "temperature": 0.1
        }

        try:
            self.logger.info(f"Sending request to Groq API for query: {natural_query}")
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()

            self.logger.info(f"Received response from Groq API: {result}")

            if 'choices' in result and len(result['choices']) > 0:
                content = result['choices'][0]['message']['content']
                sql_query = self._extract_sql_query(content)
                if sql_query:
                    return sql_query
                else:
                    return {"error": "No valid SQL query extracted from response"}
            else:
                return {"error": "No response content from LLM"}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request error: {str(e)}")
            return {"error": f"API request failed: {str(e)}"}

    def _extract_sql_query(self, content):
        """Extract the actual SQL query from the response text."""
        if "```sql" in content:
            start = content.find("```sql") + len("```sql")
            end = content.find("```", start)
            sql_query = content[start:end].strip()
            return sql_query
        elif "SELECT" in content:
            return content.strip().splitlines()[0]
        else:
            return None
