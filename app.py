import streamlit as st
import sqlite3
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
import os
from typing import Optional, Dict, Any, List, Union, Tuple
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TableRelation:
    """Represents a relationship between two tables"""
    from_table: str
    to_table: str
    from_column: str
    to_column: str

class ConfigurationError(Exception):
    """Custom exception for configuration related errors."""
    pass

class DatabaseSchema:
    """Handles database schema analysis and relationship mapping"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.relationships: List[TableRelation] = []
        self._analyze_relationships()
        
    def _analyze_relationships(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [table[0] for table in cursor.fetchall()]
                
                for table in tables:
                    cursor.execute(f"PRAGMA foreign_key_list({table});")
                    foreign_keys = cursor.fetchall()
                    
                    for fk in foreign_keys:
                        self.relationships.append(TableRelation(
                            from_table=table,
                            to_table=fk[2],
                            from_column=fk[3],
                            to_column=fk[4]
                        ))
                        
        except sqlite3.Error as e:
            logger.error(f"Error analyzing database relationships: {e}")
            raise

    def find_path_between_tables(self, start_table: str, target_table: str) -> List[TableRelation]:
        def dfs(current: str, target: str, path: List[TableRelation], visited: set) -> Optional[List[TableRelation]]:
            if current == target:
                return path
            visited.add(current)
            for rel in self.relationships:
                if rel.from_table == current and rel.to_table not in visited:
                    new_path = dfs(rel.to_table, target, path + [rel], visited)
                    if new_path:
                        return new_path
                if rel.to_table == current and rel.from_table not in visited:
                    new_path = dfs(rel.from_table, target, path + [rel], visited)
                    if new_path:
                        return new_path
            return None
        
        return dfs(start_table, target_table, [], set()) or []

class LLMService:
    def __init__(self, api_key: Optional[str] = None, db_path: Optional[str] = None):
        load_dotenv()
        
        self.api_key = api_key or os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ConfigurationError("GROQ_API_KEY not found")
            
        self.db_path = db_path or os.getenv("DB_PATH")
        if not self.db_path:
            raise ConfigurationError("DB_PATH not found")
            
        if not os.path.exists(self.db_path):
            raise ConfigurationError(f"Database file not found at: {self.db_path}")

        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        self.db_schema = DatabaseSchema(self.db_path)

    def _get_all_tables_info(self) -> Dict[str, List[Dict[str, Any]]]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                return {
                    table[0]: self.db_schema.get_table_columns(table[0])
                    for table in tables
                }
        except sqlite3.Error as e:
            logger.error(f"Database error while fetching tables info: {e}")
            raise

    def convert_to_sql_query(self, natural_query: str) -> Union[str, Dict[str, str]]:
        try:
            tables_info = self._get_all_tables_info()
            
            system_prompt = """
            You are a helpful assistant that converts natural language queries to SQL queries.
            Database schema:
            """
            
            for table_name, columns in tables_info.items():
                system_prompt += f"\n{table_name} table:\n"
                for col in columns:
                    system_prompt += f"- {col['name']} ({col['type']})"
                    if col['primary_key']:
                        system_prompt += " PRIMARY KEY"
                    system_prompt += "\n"
            
            system_prompt += "\nTable relationships:\n"
            for rel in self.db_schema.relationships:
                system_prompt += f"- {rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}\n"

            payload = {
                "model": "llama3-8b-8192",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": natural_query}
                ],
                "max_tokens": 500,
                "temperature": 0.1
            }

            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()

            if not ('choices' in result and result['choices']):
                return {"error": "No response from LLM"}

            query_info = eval(result['choices'][0]['message']['content'])
            conditions = [(condition['column'], condition['operator'], condition['value']) for condition in query_info['conditions']]

            required_joins = []
            for ref_table in query_info['reference_tables']:
                path = self.db_schema.find_path_between_tables(
                    query_info['main_table'],
                    ref_table
                )
                required_joins.extend(path)

            final_query = self._build_join_query(
                query_info['main_table'],
                conditions,
                required_joins
            )

            return final_query

        except Exception as e:
            logger.error(f"Error converting query: {str(e)}")
            return {"error": f"Error converting query: {str(e)}"}

    def execute_sql_query(self, sql_query: str) -> Dict[str, Any]:
        try:
            with sqlite3.connect(self.db_path) as connection:
                cursor = connection.cursor()
                cursor.execute(sql_query)
                results = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]
                return {"columns": column_names, "results": results}
        except sqlite3.Error as e:
            logger.error(f"Database error while executing query: {e}")
            return {"error": f"SQLite error: {str(e)}"}

def main():
    st.title("Natural Language to SQL Query Generator")
    st.write("Enter a natural language query, and the app will generate and execute the corresponding SQL query.")

    try:
        llm_service = LLMService(
            api_key="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",
            db_path="/Users/nileshhanotia/Projects/Firebase-ai-bot_1/valet_system copy.db"
        )
    except ConfigurationError as e:
        st.error(f"Configuration Error: {str(e)}")
        return
    except Exception as e:
        st.error(f"Initialization Error: {str(e)}")
        return

    natural_query = st.text_input(
        "Enter your query",
        "from the locations table show me the location name of company name devtest"
    )

    if st.button("Generate and Execute SQL Query"):
        if not natural_query:
            st.warning("Please enter a valid query.")
            return
            
        sql_query = llm_service.convert_to_sql_query(natural_query)

        if isinstance(sql_query, dict) and 'error' in sql_query:
            st.error(sql_query['error'])
            return

        st.subheader("Generated SQL Query:")
        st.code(sql_query, language='sql')
        
        results = llm_service.execute_sql_query(sql_query)
        
        if 'error' in results:
            st.error(results['error'])
            return

        st.subheader("Query Results:")
        if results["results"]:
            df = pd.DataFrame(results["results"], columns=results["columns"])
            st.dataframe(df)
        else:
            st.write("No results found.")

if __name__ == "__main__":
    main()
