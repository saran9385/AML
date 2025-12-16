import json
import requests
from fuzzywuzzy import fuzz
import re
from datetime import datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# URLs and file paths
positions_url = "https://data.opensanctions.org/datasets/latest/peps/pep-positions.json"
names_url = "https://data.opensanctions.org/datasets/latest/peps/names.txt"
local_file_path = "PEP/pep-positions-in.txt"
india_json_file = "PEP/PEP_India.json"  # Path to your JSON file


# Load JSON data from the URL
def load_positions(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error fetching data from {url}: {e}')
        return {}

# Load JSON data from a local file
def load_local_positions(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read().splitlines()
    except FileNotFoundError:
        print(f'File not found: {file_path}')
    except Exception as e:
        print(f'Error reading the local file: {e}')
    return []

# Load the India JSON file
def load_india_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f'File not found: {file_path}')
    except json.JSONDecodeError as e:
        print(f'Error decoding JSON from {file_path}: {e}')
    except Exception as e:
        print(f'Error reading the JSON file: {e}')
    return []

# Load names data from the URL
def load_names(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text.splitlines()
    except requests.RequestException as e:
        print(f'Error fetching names from {url}: {e}')
        return []

def search_positions(position, threshold, dob=None, country=None):
    start_time = time.time()
    try:
        threshold = int(threshold)
    except ValueError:
        print(f"Invalid threshold value: {threshold}. Using default value of 60.")
        threshold = 60  # Default value if conversion fails
    
    matched_records = []
    if country in ['India', 'IN'] or country is None:

        # Load data using multi-threading
        with ThreadPoolExecutor() as executor:
            future_india = executor.submit(load_india_json, india_json_file)
            future_local = executor.submit(load_local_positions, local_file_path)

            india_data = future_india.result()
            local_positions = future_local.result()

        # Normalize the provided DOB to YYYY-MM-DD format
        normalized_dob = None
        if dob:
            try:
                normalized_dob = datetime.strptime(dob.split("T")[0], "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid DOB format: {dob}. Skipping DOB matching.")

        # Process India JSON data using multi-threading
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_position_record, record, position, threshold, normalized_dob) for record in india_data]
            for future in as_completed(futures):
                matched_records.extend(future.result())

        # Process local positions using multi-threading
        with ThreadPoolExecutor() as executor:
            if dob is None:
                local_futures = [executor.submit(process_local_position, local_pos, position, threshold) for local_pos in local_positions]
                for future in as_completed(local_futures):
                    matched_records.extend(future.result())

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Time taken for fetching and processing: {elapsed_time:.2f} seconds pep postion")

    return matched_records

# Function to process individual position records (India Data) with DOB matching
def process_position_record(record, search_position, threshold, normalized_dob):
    position_label = record.get("positionLabel", "").lower()
    person_label = record.get("personLabel", "")
    record_dob = record.get("dob", "").split("T")[0]  # Extract only the date part

    try:
        record_dob_date = datetime.strptime(record_dob, "%Y-%m-%d").date()
    except ValueError:
        record_dob_date = None  # Handle invalid DOB formats

    position_score = fuzz.token_set_ratio(search_position.lower(), position_label)
    if search_position.lower() != position_label and 80 <= position_score <= 100:
        position_score -= 10  # Apply penalty for close matches

    if position_score >= threshold:
        if normalized_dob:
            if record_dob_date == normalized_dob:
                return [{
                    "name": person_label,
                    "dob": record_dob,
                    "position": record.get("positionLabel", ""),
                    "score": position_score,
                    "startDate": record.get("startDate", ""),
                    "endDate": record.get("endDate", "")
                }]
        else:
            return [{
                "name": person_label,
                "dob": record_dob,
                "position": record.get("positionLabel", ""),
                "score": position_score,
                "startDate": record.get("startDate", ""),
                "endDate": record.get("endDate", "")
            }]
    return []

# Function to process individual local position records
def process_local_position(local_pos, search_position, threshold):
    local_pos_lower = local_pos.lower()
    position_score = fuzz.token_set_ratio(search_position.lower(), local_pos_lower)

    if search_position.lower() != local_pos_lower and 80 <= position_score <= 100:
        position_score -= 10  # Apply penalty for partial matches

    if position_score >= threshold:
        return [{
            "position": local_pos,
            "score": position_score
        }]
    return []


def search_names(name, threshold, dob=None, country=None):
    start_time = time.time()  # Record start time
    matched_names = []
    if country in ['India', 'IN'] or country is None:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []

            # Load the necessary data concurrently
            futures.append(executor.submit(load_names, names_url))
            futures.append(executor.submit(load_india_json, india_json_file))

            names_list, india_data = [future.result() for future in futures]

            search_parts = re.split(r'\s+|\.', name.lower().strip())
            search_name = name.lower().strip()
            normalized_dob = None
            if dob:
                try:
                    normalized_dob = datetime.strptime(dob.split("T")[0], "%Y-%m-%d").date()
                except ValueError:
                    print(f"Invalid DOB format: {dob}. Skipping DOB matching.")
            # Process India JSON data
            for record in india_data:
                person_label = record.get("personLabel", "")
                position_label = record.get("positionLabel", "")
                record_dob = record.get("dob", "").split("T")[0]
                try:
                    record_dob_date = datetime.strptime(record_dob, "%Y-%m-%d").date()
                except ValueError:
                    continue

                if name.lower() in person_label.lower() or person_label.lower() in name.lower():
                    score = fuzz.partial_ratio(name.lower(), person_label.lower())
                    if name.lower() != person_label.lower():
                        if score >= 80 and score <= 100:
                            score -= 10

                    # Match by DOB if provided
                    if score >= threshold:
                        if normalized_dob:
                            if record_dob_date == normalized_dob:
                                matched_names.append({
                                    "name": person_label,
                                    "dob": dob,
                                    "score": score,
                                    "Country": record.get("countryLabel", ""),
                                    "position": record.get("positionLabel", ""),
                                    "startDate": record.get("startDate", ""),
                                    "endDate": record.get("endDate", "")
                                })
                        else:
                            matched_names.append({
                                "name": person_label,
                                "dob": record_dob,
                                "position": position_label,
                                "Country": record.get("countryLabel", ""),
                                "score": score,
                                "startDate": record.get("startDate", ""),
                                "endDate": record.get("endDate", "")
                            })
        end_time = time.time()  # Record end time
        elapsed_time = end_time - start_time

        # print(f"Time taken for fetching and processing: {elapsed_time:.2f} seconds pep name")
    # print(matched_names)

    return matched_names

def search_name_and_position(name, position, threshold, dob=None, country=None):
    start_time=time.time()
    try:
        threshold = int(threshold)
    except ValueError:
        print(f'Invalid threshold value: {threshold}. Using default value of 80.')
        threshold = 80

    if country in ['India', 'IN'] or country is None:
        search_name = name.lower().strip()
        search_position = str(position).lower().strip()
        matched_records = []
        normalized_dob = None

        if dob:
            try:
                normalized_dob = datetime.strptime(dob.split("T")[0], "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid DOB format: {dob}. Skipping DOB matching.")

        # Use ThreadPoolExecutor to process India JSON data concurrently
        with ThreadPoolExecutor() as executor:
            futures = []
            # Submit tasks for processing each record in india_data
            india_data = load_india_json(india_json_file)  # Load data first
            for record in india_data:
                futures.append(executor.submit(process_record, record, search_name, search_position, normalized_dob, threshold))

            # Collect results from each future
            for future in as_completed(futures):
                matched_records.extend(future.result())

    else:
        return []
    end_time = time.time()  # Record end time
    elapsed_time = end_time - start_time

    # print(f"Time taken for fetching and processing: {elapsed_time:.2f} seconds pep name and position")

    return matched_records

# This function processes each individual record
def process_record(record, search_name, search_position, normalized_dob, threshold):
    person_label = record.get("personLabel", "").lower()
    position_label = record.get("positionLabel", "").lower()
    record_dob = record.get("dob", "").split("T")[0]
    try:
        record_dob_date = datetime.strptime(record_dob, "%Y-%m-%d").date()
    except ValueError:
        return []  # Skip if DOB format is invalid

    # Check name similarity
    name_score = fuzz.partial_ratio(search_name, person_label)
    if search_name != person_label:
        if 80 <= name_score <= 100:
            name_score -= 10

    # Check position similarity
    position_score = fuzz.partial_ratio(search_position, position_label)
    if search_position != position_label:
        if 80 <= position_score <= 100:
            position_score -= 10

    # Match conditions
    matched_records = []
    if name_score >= threshold and position_score >= threshold:
        if normalized_dob:
            if record_dob_date == normalized_dob:
                matched_records.append({
                    "name": record.get("personLabel", ""),
                    "dob": record_dob,
                    "position": record.get("positionLabel", ""),
                    "score": name_score,
                    "startDate": record.get("startDate", ""),
                    "endDate": record.get("endDate", "")
                })
        else:
            matched_records.append({
                "name": record.get("personLabel", ""),
                "dob": record_dob,
                "position": record.get("positionLabel", ""),
                "score": name_score,
                "startDate": record.get("startDate", ""),
                "endDate": record.get("endDate", "")
            })

    return matched_records