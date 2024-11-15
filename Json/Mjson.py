import json

def load_json(file_path):
    """Load a JSON file."""
    with open(file_path, 'r') as file:
        return json.load(file)

def merge_json(companies_file, locations_file, activities_file, tickets_file, users_file):
    """Merge multiple JSON files, maintaining hierarchy and relationships."""
    
    # Load the JSON files
    companies = load_json(companies_file)
    locations = load_json(locations_file)
    activities = load_json(activities_file)
    tickets = load_json(tickets_file)
    users = load_json(users_file)
    
    # Step 1: Build dictionaries for easy lookup by id
    company_dict = {company['id']: company for company in companies}
    location_dict = {location['id']: location for location in locations}
    activity_dict = {activity['id']: activity for activity in activities}
    
    # Step 2: Add locations to companies based on matching company names (if applicable)
    for location in locations:
        location_name = location.get('location_name', '')  # Check if 'location_name' exists
        for company in companies:
            company_name = company.get('company_name', '')  # Check if 'company_name' exists
            if company_name and company_name in location_name:  # Match company name with location name
                company_id = company['id']
                location['company_id'] = company_id  # Add company_id to the location
                company.setdefault('locations', []).append(location)
                break
    
    # Step 3: Add activities to locations based on location_id
    for activity in activities:
        location_id = activity['location_id']
        if location_id in location_dict:
            location_dict[location_id].setdefault('activities', []).append(activity)
        else:
            print(f"Warning: Location with ID {location_id} not found for activity {activity['activity_name']}")

    # Step 4: Add tickets to locations and activities based on location_id and activity_id
    for ticket in tickets:
        location_id = ticket['location_id']
        activity_id = ticket['activity_id']
        if location_id in location_dict:
            location_dict[location_id].setdefault('tickets', []).append(ticket)
        else:
            print(f"Warning: Location with ID {location_id} not found for ticket {ticket['ticket_id']}")
        
        if activity_id in activity_dict:
            activity_dict[activity_id].setdefault('tickets', []).append(ticket)
        else:
            print(f"Warning: Activity with ID {activity_id} not found for ticket {ticket['ticket_id']}")

    # Step 5: Add users to locations based on location_id
    for user in users:
        location_id = user['location_id']
        if location_id in location_dict:
            location_dict[location_id].setdefault('users', []).append(user)
        else:
            print(f"Warning: Location with ID {location_id} not found for user {user['user_name']}")

    # Return the merged data with all relationships preserved
    return list(company_dict.values())  # Return the merged companies list

def save_merged_json(merged_data, output_file):
    """Save merged data to a new JSON file."""
    with open(output_file, 'w') as file:
        json.dump(merged_data, file, indent=4)

# Paths to input JSON files
companies_file = 'companies.json'
locations_file = 'locations.json'
activities_file = 'activities.json'
tickets_file = 'tickets.json'
users_file = 'users.json'

# Merge the JSON files while preserving relationships and hierarchy
merged_data = merge_json(companies_file, locations_file, activities_file, tickets_file, users_file)

# Save the merged data to a new JSON file
save_merged_json(merged_data, 'merged_data.json')

print("Merging complete. Data saved in 'merged_data.json'.")
