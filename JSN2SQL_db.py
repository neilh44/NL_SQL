import json
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy.ext.declarative import declared_attr
from typing import Dict, List, Any, Optional
import os
from datetime import datetime
import logging
from tqdm import tqdm
import pandas as pd
import jsonschema
from concurrent.futures import ThreadPoolExecutor
import hashlib

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

Base = declarative_base()

class SchemaValidator:
    """Validates JSON schema consistency across files"""
    
    def __init__(self):
        self.schema_cache = {}
        
    def generate_schema(self, data: Dict) -> Dict:
        """Generate schema from sample data"""
        if isinstance(data, dict):
            return {k: type(v).__name__ for k, v in data.items()}
        return {}
        
    def validate_schema(self, data: Dict, file_name: str) -> bool:
        """Validate data against existing schema"""
        current_schema = self.generate_schema(data)
        
        if not self.schema_cache:
            self.schema_cache = current_schema
            return True
            
        if current_schema != self.schema_cache:
            logger.warning(f"Schema mismatch in file {file_name}")
            self.show_schema_differences(current_schema)
            return False
        return True
    
    def show_schema_differences(self, current_schema: Dict):
        """Show differences between current and cached schema"""
        for key in set(self.schema_cache.keys()) | set(current_schema.keys()):
            if key not in self.schema_cache:
                logger.warning(f"New field found: {key}")
            elif key not in current_schema:
                logger.warning(f"Missing field: {key}")
            elif self.schema_cache[key] != current_schema[key]:
                logger.warning(f"Type mismatch for {key}: expected {self.schema_cache[key]}, got {current_schema[key]}")

class TableGenerator:
    def __init__(self):
        self.tables = {}
        self.relationships = {}
        self.schema_validator = SchemaValidator()
        
    def infer_column_type(self, value: Any) -> sa.types.TypeEngine:
        """Infer SQLAlchemy column type from Python value with extended support"""
        if isinstance(value, bool):
            return sa.Boolean
        elif isinstance(value, int):
            return sa.BigInteger if abs(value) > 2**31 else sa.Integer
        elif isinstance(value, float):
            return sa.Float
        elif isinstance(value, datetime):
            return sa.DateTime
        elif isinstance(value, dict):
            return sa.JSON
        elif isinstance(value, list):
            return sa.JSON
        elif len(str(value)) > 255:
            return sa.Text
        else:
            return sa.String(255)
    
    def detect_relationships(self, data: Dict) -> Dict:
        """Detect potential relationships between tables with enhanced detection"""
        relationships = {}
        
        for key, value in data.items():
            if isinstance(value, dict):
                relationships[key] = {
                    'type': 'one_to_one',
                    'table': key.title(),
                    'nullable': True
                }
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Check for many-to-many relationship
                if self._has_multiple_foreign_keys(value[0]):
                    relationships[key] = {
                        'type': 'many_to_many',
                        'table': key.title(),
                        'junction_table': f"{key.lower()}_junction"
                    }
                else:
                    relationships[key] = {
                        'type': 'one_to_many',
                        'table': key.title()
                    }
        return relationships
    
    def _has_multiple_foreign_keys(self, data: Dict) -> bool:
        """Check if data structure suggests many-to-many relationship"""
        foreign_key_count = sum(1 for key in data.keys() if key.endswith('_id'))
        return foreign_key_count > 1
    
    def create_table_class(self, table_name: str, sample_data: Dict, parent_table: str = None):
        """Create SQLAlchemy model class with enhanced features"""
        attributes = {
            '__tablename__': table_name.lower(),
            'id': sa.Column(sa.Integer, primary_key=True),
            'created_at': sa.Column(sa.DateTime, default=datetime.utcnow),
            'updated_at': sa.Column(sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
            'hash': sa.Column(sa.String(64), unique=True)  # For deduplication
        }
        
        if parent_table:
            foreign_key_name = f"{parent_table.lower()}_id"
            attributes[foreign_key_name] = sa.Column(sa.Integer, 
                                                   sa.ForeignKey(f'{parent_table.lower()}.id'),
                                                   nullable=True)
            
        for key, value in sample_data.items():
            if isinstance(value, (dict, list)):
                continue
            col_type = self.infer_column_type(value)
            attributes[key] = sa.Column(col_type, nullable=True)
            
        return type(table_name, (Base,), attributes)

class JSONToSQL:
    def __init__(self, db_url: str):
        self.engine = sa.create_engine(db_url)
        self.table_generator = TableGenerator()
        
    def _calculate_hash(self, data: Dict) -> str:
        """Calculate hash of data for deduplication"""
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    
    def load_json_files(self, json_directory: str):
        """Load multiple JSON files with progress bar and validation"""
        json_files = [f for f in os.listdir(json_directory) if f.endswith('.json')]
        
        for json_file in tqdm(json_files, desc="Processing JSON files"):
            try:
                with open(os.path.join(json_directory, json_file)) as f:
                    data = json.load(f)
                    if self.table_generator.schema_validator.validate_schema(data, json_file):
                        self.table_generator.process_json_data(data)
                    else:
                        logger.warning(f"Skipping {json_file} due to schema mismatch")
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in file {json_file}")
            except Exception as e:
                logger.error(f"Error processing {json_file}: {str(e)}")
    
    def create_database(self):
        """Create database tables with proper indexing"""
        inspector = sa.inspect(self.engine)
        
        # Create or update tables
        for table in Base.metadata.sorted_tables:
            if table.name not in inspector.get_table_names():
                table.create(self.engine)
                logger.info(f"Created table: {table.name}")
            else:
                logger.info(f"Table {table.name} already exists")
    
    def insert_data(self, json_data: Dict, table_class, session: Session):
        """Insert data with deduplication and batch processing"""
        if isinstance(json_data, list):
            # Batch process lists
            batch_size = 1000
            for i in range(0, len(json_data), batch_size):
                batch = json_data[i:i + batch_size]
                for item in batch:
                    self._insert_single_record(item, table_class, session)
                session.flush()
            return
        
        self._insert_single_record(json_data, table_class, session)
    
    def _insert_single_record(self, data: Dict, table_class, session: Session):
        """Insert a single record with deduplication"""
        record_data = {}
        data_hash = self._calculate_hash(data)
        
        # Check for existing record
        existing_record = session.query(table_class).filter_by(hash=data_hash).first()
        if existing_record:
            logger.debug(f"Duplicate record found in {table_class.__tablename__}")
            return existing_record.id
        
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                continue
            record_data[key] = value
        
        record_data['hash'] = data_hash
        record = table_class(**record_data)
        session.add(record)
        session.flush()
        
        # Handle relationships
        self._process_relationships(data, record, table_class, session)
        
        return record.id
    
    def _process_relationships(self, data: Dict, record, table_class, session: Session):
        """Process relationships for a record"""
        for key, rel_info in self.table_generator.relationships.get(table_class.__name__, {}).items():
            related_data = data.get(key)
            if not related_data:
                continue
                
            related_table = self.table_generator.tables[rel_info['table']]
            
            if rel_info['type'] == 'many_to_many':
                self._handle_many_to_many(related_data, record, related_table, rel_info, session)
            elif isinstance(related_data, list):
                for item in related_data:
                    item[f"{table_class.__name__.lower()}_id"] = record.id
                    self.insert_data(item, related_table, session)
            else:
                related_data[f"{table_class.__name__.lower()}_id"] = record.id
                self.insert_data(related_data, related_table, session)
    
    def convert(self, json_directory: str = "/Users/nileshhanotia/Projects/Firebase-ai-bot_1/Json"):
        """Convert JSON files to SQL database with enhanced error handling and logging"""
        logger.info(f"Starting conversion from {json_directory}")
        
        try:
            # Validate directory
            if not os.path.exists(json_directory):
                raise FileNotFoundError(f"Directory not found: {json_directory}")
            
            # Load JSON files and create table classes
            self.load_json_files(json_directory)
            
            # Create database tables
            self.create_database()
            
            # Insert data with progress tracking
            session = Session(self.engine)
            try:
                json_files = [f for f in os.listdir(json_directory) if f.endswith('.json')]
                
                for json_file in tqdm(json_files, desc="Inserting data"):
                    try:
                        with open(os.path.join(json_directory, json_file)) as f:
                            data = json.load(f)
                            root_table = self.table_generator.tables['Root']
                            self.insert_data(data, root_table, session)
                        session.commit()
                        logger.info(f"Successfully processed {json_file}")
                    except Exception as e:
                        session.rollback()
                        logger.error(f"Error processing {json_file}: {str(e)}")
                
            finally:
                session.close()
                
            logger.info("Conversion completed successfully")
            
        except Exception as e:
            logger.error(f"Conversion failed: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    try:
        # Initialize converter with SQLite database
        converter = JSONToSQL('sqlite:///output_database.db')
        
        # Convert JSON files from the specified directory
        converter.convert()
        
        logger.info("Database creation completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")