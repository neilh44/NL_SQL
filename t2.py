import json
import sqlite3
from datetime import datetime
import os

# Set the path to JSON files
JSON_PATH = "/Users/nileshhanotia/Projects/Firebase-ai-bot_1/Json"

def timestamp_to_datetime(timestamp_dict):
    if not timestamp_dict:
        return None
    seconds = timestamp_dict.get('_seconds', 0)
    return datetime.fromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S')

def create_database():
    db_path = os.path.join(JSON_PATH, 'valet_system_1.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create companies table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS companies (
        id TEXT PRIMARY KEY,
        company_name TEXT,
        company_email TEXT,
        company_phone TEXT,
        status TEXT,
        company_slug TEXT,
        created_at DATETIME,
        modified_at DATETIME,
        stripe_customer_id TEXT
    )
    ''')

    # Create users table with a foreign key to companies
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        company_id TEXT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        role TEXT,
        status TEXT,
        is_phone_verified BOOLEAN,
        is_registered BOOLEAN,
        created_at DATETIME,
        modified_at DATETIME,
        FOREIGN KEY (company_id) REFERENCES companies(id)
    )
    ''')

    # Create locations table with a foreign key to companies
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS locations (
        id TEXT PRIMARY KEY,
        company_id TEXT,
        location_name TEXT,
        location_status TEXT,
        location_slug TEXT,
        address_line1 TEXT,
        address_line2 TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        time_zone TEXT,
        status TEXT,
        created_at DATETIME,
        modified_at DATETIME,
        FOREIGN KEY (company_id) REFERENCES companies(id)
    )
    ''')

    # Create tickets table with a foreign key to locations
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tickets (
        id TEXT PRIMARY KEY,
        location_id TEXT,
        ticket_no TEXT,
        shift_id TEXT,
        ticket_type TEXT,
        customer_name TEXT,
        customer_phone TEXT,
        customer_email TEXT,
        parking_zone TEXT,
        pickup_zone TEXT,
        spot TEXT,
        key_spot TEXT,
        current_status TEXT,
        created_at DATETIME,
        modified_at DATETIME,
        FOREIGN KEY (location_id) REFERENCES locations(id)
    )
    ''')

    # Create activities table with foreign keys to tickets and locations
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS activities (
        id TEXT PRIMARY KEY,
        ticket_id TEXT,
        location_id TEXT,
        shift_id TEXT,
        type TEXT,
        user_name TEXT,
        user_id TEXT,
        created_at DATETIME,
        FOREIGN KEY (ticket_id) REFERENCES tickets(id),
        FOREIGN KEY (location_id) REFERENCES locations(id)
    )
    ''')

    return conn

def insert_company_data(cursor, companies_data):
    if not isinstance(companies_data, list):
        companies_data = [companies_data]

    for company_data in companies_data:
        try:
            cursor.execute('''
            INSERT INTO companies (
                id, company_name, company_email, company_phone,
                status, company_slug, created_at, modified_at,
                stripe_customer_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                company_data['id'],
                company_data['companyName'],
                company_data['companyEmail'],
                company_data['companyPhone'],
                company_data['status'],
                company_data['companySlug'],
                timestamp_to_datetime(company_data.get('createdAt')),
                timestamp_to_datetime(company_data.get('modifiedAt')),
                company_data.get('stripeCustomerId')
            ))
        except Exception as e:
            print(f"Error inserting company {company_data.get('id')}: {str(e)}")

def insert_user_data(cursor, users_data):
    if not isinstance(users_data, list):
        users_data = [users_data]

    for user_data in users_data:
        try:
            cursor.execute('''
            INSERT INTO users (
                id, company_id, first_name, last_name, email, phone,
                role, status, is_phone_verified, is_registered,
                created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user_data['id'],
                user_data['companyId'],
                user_data['firstName'],
                user_data['lastName'],
                user_data['email'],
                user_data['phone'],
                user_data['role'],
                user_data['status'],
                user_data['isPhoneVerified'],
                user_data['isRegistered'],
                timestamp_to_datetime(user_data.get('createdAt')),
                timestamp_to_datetime(user_data.get('modifiedAt'))
            ))
        except Exception as e:
            print(f"Error inserting user {user_data.get('id')}: {str(e)}")

def insert_location_data(cursor, locations_data):
    if not isinstance(locations_data, list):
        locations_data = [locations_data]

    for location_data in locations_data:
        try:
            cursor.execute('''
            INSERT INTO locations (
                id, company_id, location_name, location_status, location_slug,
                address_line1, address_line2, city, state, zipcode,
                time_zone, status, created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                location_data['id'],
                location_data['companyId'],
                location_data['locationName'],
                location_data['locationStatus'],
                location_data['locationSlug'],
                location_data['addressLine1'],
                location_data['addressLine2'],
                location_data['city'],
                location_data['state'],
                location_data['zipcode'],
                location_data['timeZone'],
                location_data['status'],
                timestamp_to_datetime(location_data.get('createdAt')),
                timestamp_to_datetime(location_data.get('modifiedAt'))
            ))
        except Exception as e:
            print(f"Error inserting location {location_data.get('id')}: {str(e)}")

def insert_ticket_data(cursor, tickets_data):
    if not isinstance(tickets_data, list):
        tickets_data = [tickets_data]

    for ticket_data in tickets_data:
        try:
            cursor.execute('''
            INSERT INTO tickets (
                id, location_id, ticket_no, shift_id, ticket_type,
                customer_name, customer_phone, customer_email,
                parking_zone, pickup_zone, spot, key_spot,
                current_status, created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticket_data['id'],
                ticket_data['locationId'],
                ticket_data['ticketNo'],
                ticket_data['shiftId'],
                ticket_data['ticketType'],
                ticket_data['customerName'],
                ticket_data['customerPhone'],
                ticket_data.get('customerEmail'),
                ticket_data.get('parkingZone'),
                ticket_data.get('pickupZone'),
                ticket_data.get('spot'),
                ticket_data.get('keySpot'),
                ticket_data['currentStatus'],
                timestamp_to_datetime(ticket_data.get('createdAt')),
                timestamp_to_datetime(ticket_data.get('modifiedAt'))
            ))
        except Exception as e:
            print(f"Error inserting ticket {ticket_data.get('id')}: {str(e)}")

def insert_activity_data(cursor, activities_data):
    if not isinstance(activities_data, list):
        activities_data = [activities_data]

    for activity_data in activities_data:
        try:
            cursor.execute('''
            INSERT INTO activities (
                id, ticket_id, location_id, shift_id, type,
                user_name, user_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                activity_data['id'],
                activity_data['ticketId'],
                activity_data['locationId'],
                activity_data['shiftId'],
                activity_data['type'],
                activity_data['userName'],
                activity_data['userId'],
                timestamp_to_datetime(activity_data.get('createdAt'))
            ))
        except Exception as e:
            print(f"Error inserting activity {activity_data.get('id')}: {str(e)}")

def main():
    conn = create_database()
    cursor = conn.cursor()

    json_files = {
        'companies.json': insert_company_data,
        'users.json': insert_user_data,
        'Location.json': insert_location_data,
        'tickets.json': insert_ticket_data,
        'activities.json': insert_activity_data
    }

    for filename, insert_function in json_files.items():
        file_path = os.path.join(JSON_PATH, filename)
        try:
            with open(file_path, 'r') as f:
                content = f.read().replace(',}', '}').replace(',]', ']')
                data = json.loads(content)
                insert_function(cursor, data)
        except FileNotFoundError:
            print(f"Warning: {filename} not found, skipping...")
        except json.JSONDecodeError as e:
            print(f"Error parsing {filename}: {str(e)}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
