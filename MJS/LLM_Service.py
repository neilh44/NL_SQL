import logging
import json
import os
import requests
import re
from typing import Dict, Any


class LLMService:
    def __init__(self, api_key: str, json_folder_path: str):
        self.api_key = api_key
        self.json_folder_path = json_folder_path
        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        logging.basicConfig(level=logging.DEBUG)  # Set logging level to DEBUG for more detailed logs
        self.logger = logging.getLogger(__name__)
        self.file_info = self._load_json_structure()

    def _load_json_structure(self) -> Dict[str, Any]:
        """Load the JSON structure and relationships"""
        file_info = {}
        try:
            json_files = [f for f in os.listdir(self.json_folder_path) if f.endswith('.json')]
            for file_name in json_files:
                file_info[file_name] = file_name  # Keep it simple for now
            self.logger.debug(f"Loaded JSON structure: {file_info}")
            return file_info
        except Exception as e:
            self.logger.error(f"Error loading JSON structure: {e}")
            return {}

    def convert_to_json_query(self, natural_query: str) -> Dict[str, Any]:
        """Convert natural language to JSON query using Groq API"""
        schema_prompt = self._prepare_schema_prompt()

        system_prompt = f"""
        You are an expert in JSON query generation. Your task is to convert natural language queries into JSON query format based on the provided file structure.

        {schema_prompt}

        Generate a JSON query for the following natural language request:
        {natural_query}

        Return the JSON query with all explanation.
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
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()

            raw_response = response.json()  # Directly parse the response as JSON
            self.logger.debug(f"Raw response from Groq API: {raw_response}")

            # Extract the message content from the response
            message_content = raw_response['choices'][0]['message']['content']
            self.logger.debug(f"Message content: {message_content}")

            # Use a regular expression to extract the JSON query from the message content
            json_match = re.search(r'```json(.*?)```', message_content, re.DOTALL)
            if json_match:
                json_query_str = json_match.group(1).strip()  # Extract the JSON string between the code block
                self.logger.debug(f"Extracted JSON query: {json_query_str}")

                try:
                    # Parse the extracted JSON query string into a Python dict
                    json_query = json.loads(json_query_str)
                    self.logger.debug(f"Successfully parsed JSON query: {json_query}")
                    return {"success": True, "query": json_query}
                except json.JSONDecodeError:
                    self.logger.error(f"Failed to parse the extracted JSON query: {json_query_str}")
                    return {"success": False, "error": "Failed to parse the extracted JSON query."}
            else:
                self.logger.error(f"No valid JSON query found in the response: {message_content}")
                return {"success": False, "error": "No valid JSON query found in the response."}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request error: {str(e)}")
            return {"success": False, "error": f"API request failed: {str(e)}"}

    def execute_json_query(self, json_query: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate executing the JSON query and return results"""
        try:
            # Ensure the query is converted to a JSON string if it's passed as a dictionary
            if isinstance(json_query, dict):
                json_query_str = json.dumps(json_query)  # Convert dict to JSON string
            else:
                json_query_str = json_query  # If it's already a string, use it directly

            self.logger.debug(f"Executing query: {json_query_str}")

            result = {}

            # Loop through all the files in the folder to find matches
            for file_name in self.file_info.keys():
                file_path = os.path.join(self.json_folder_path, file_name)
                self.logger.debug(f"Loading data from {file_path}")

                if not os.path.exists(file_path):
                    self.logger.error(f"File not found: {file_path}")
                    continue

                with open(file_path, 'r') as f:
                    data = json.load(f)
                    self.logger.debug(f"Loaded data from {file_path}: {data}")  # Log loaded data

                    # Get the filter condition for companies
                    companies_filter = json_query.get("query", {}).get("companies", {}).get("filter", {})
                    if companies_filter:
                        filter_key = companies_filter.get("id", {}).get("eq")

                        if filter_key:
                            self.logger.debug(f"Filtering data by id: {filter_key}")
                            # Filter the data based on the provided ID filter
                            filtered_data = [item for item in data if item.get("id") == filter_key]
                            if filtered_data:
                                result[file_name] = filtered_data
                            else:
                                self.logger.warning(f"No data found for ID: {filter_key} in {file_name}")

            # Extract the requested fields from the filtered data
            final_result = {}
            fields_requested = json_query.get("query", {}).get("companies", {}).get("fields", [])

            if "companies.json" in result:
                for item in result["companies.json"]:
                    extracted_data = {field: item.get(field) for field in fields_requested}
                    if extracted_data:
                        final_result["companies.json"] = extracted_data
                    else:
                        self.logger.warning(f"No matching fields found for: {fields_requested}")

            if final_result:
                self.logger.debug(f"Final result: {final_result}")
                return {"success": True, "results": final_result}

            self.logger.error(f"No data found matching the query fields or filter")
            return {"success": False, "error": "No data found matching the query fields or filter"}

        except Exception as e:
            self.logger.error(f"Error executing JSON query: {e}")
            return {"success": False, "error": str(e)}

    def _prepare_schema_prompt(self) -> str:
        """Prepare schema description for LLM"""
        prompt = "JSON Schema with Relationships:\n\n"
        for file_name in self.file_info.keys():
            prompt += f"File: {file_name}\n"
            prompt += "\n"
        return prompt
