# -*- coding: utf-8 -*-
"""Tenant Hunter - Refined Google Sheets Script with Silent API Error Handling

This script automates the process of finding and evaluating business leads from a Google Sheet.
It now includes:
- The maximum allowed search radius for the Yelp API (40,000 meters).
- Separate sheets for raw and ranked leads.
- A new initial filter to exclude low-quality leads immediately.
- A smart review-fetching mechanism that only pulls reviews when most needed.
- An enhanced AI evaluation prompt that includes review data.
- All website-grabbing functionality has been removed.
- Updates the status of a completed property in the control sheet.
- IMPORTANT: API keys are now securely read from local files.
- NEW: Leads with less than 3 reviews are now filtered out to improve efficiency.
- NEW: AI-evaluated leads are now sorted by score within each search term.
- NEW: Re-added the filter to remove leads with less than 3.0 stars.
"""

# --- LIBRARY IMPORTS ---
import os
import sys
import datetime
import json
import time
import requests
import gspread
import math
from google.oauth2.service_account import Credentials

# --- PATH FINDER FUNCTION ---
def find_file_path(filename):
    """
    Finds the path to a file, whether the script is running directly
    or as a PyInstaller one-file executable.
    """
    if getattr(sys, 'frozen', False):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, filename)

# --- CONFIGURATION (Edit These Only) ---
# The spreadsheet ID is now part of the script itself, not a credential.
SPREADSHEET_ID = "1VcliY4xbM7yNHMRpOtj5b1YQJ6gueudpetdcZfDx7sM"
CONTROL_SHEET_NAME = "Control_Sheet"

# AI Evaluation Settings (These can be overridden by the Control Sheet)
BATCH_SIZE = 20
FILTER_HIGH_ONLY = True

# New Review and Rating Filters
REVIEW_FILTER_MIN_COUNT = 3
RATING_FILTER_MIN = 3.0

# --- API KEYS ---
# IMPORTANT: These are now read from local files
# for a simple, no-code-entry user experience.
try:
    config_path = find_file_path('config.json')
    with open(config_path) as config_file:
        config = json.load(config_file)
    API_KEY = config['GEMINI_API_KEY']
    YELP_API_KEY = config['YELP_API_KEY']
    YELP_HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
except FileNotFoundError:
    print("❌ Missing config.json file. Please create it and add your API keys.")
    exit()
except KeyError as e:
    print(f"❌ config.json is missing a required key: {e}")
    exit()

# --- HELPER FUNCTIONS ---

def get_sheet_connection():
    """Establishes and returns a connection to the Google Sheet using credentials from a local file."""
    try:
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        creds_path = find_file_path('service_account.json')
        creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        return sh
    except FileNotFoundError:
        print("❌ Missing service_account.json file. Please place it in the same directory.")
        return None
    except Exception as e:
        print(f"❌ Error connecting to Google Sheets: {e}")
        return None

def read_from_sheet(sheet_name):
    """
    Reads data from the specified Google Sheet tab.
    Returns the worksheet as a list of lists.
    """
    sh = get_sheet_connection()
    if not sh:
        return None
    try:
        worksheet = sh.worksheet(sheet_name)
        return worksheet.get_all_values()
    except Exception as e:
        print(f"❌ Error reading from Google Sheet '{sheet_name}': {e}")
        return None

def write_to_sheet(sheet_name, header, data):
    """
    Writes data to a new or existing sheet in the specified Google Sheet.
    If the sheet exists, it appends the data. If it does not, it creates it.
    """
    sh = get_sheet_connection()
    if not sh:
        return
    try:
        try:
            worksheet = sh.worksheet(sheet_name)
            worksheet.clear()
            if header:
                worksheet.append_row(header)
            if data:
                worksheet.append_rows(data)
            print(f"✅ Successfully wrote results to the '{sheet_name}' sheet.")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=sheet_name, rows="1", cols="1")
            if header:
                worksheet.append_row(header)
            if data:
                worksheet.append_rows(data)
            print(f"✅ Successfully created and wrote results to the new '{sheet_name}' sheet.")
    except Exception as e:
        print(f"❌ Error writing to Google Sheet: {e}")

def update_control_sheet_status(sheet_prefix):
    """Updates the status and timestamp for a property in the control sheet."""
    sh = get_sheet_connection()
    if not sh:
        return

    try:
        control_sheet = sh.worksheet(CONTROL_SHEET_NAME)
        cell = control_sheet.find(sheet_prefix)
        if cell:
            control_sheet.update_cell(cell.row, 7, "Completed")
            control_sheet.update_cell(cell.row, 8, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print(f"✅ Successfully updated status for property with prefix '{sheet_prefix}'.")
        else:
            print(f"⚠️ Could not find property with prefix '{sheet_prefix}' in the control sheet.")
    except Exception as e:
        print(f"❌ Error updating control sheet: {e}")

def yelp_search_leads(term, city, limit=50, radius=40000):
    """
    Searches Yelp for leads based on a search term and city.
    Returns a list of leads including the Yelp business ID.
    """
    yelp_url = "https://api.yelp.com/v3/businesses/search"
    all_leads = []
    
    for offset in range(0, limit, 50):
        params = {
            'term': term,
            'location': city,
            'limit': min(limit - offset, 50),
            'radius': radius,
            'offset': offset,
        }

        try:
            response = requests.get(yelp_url, headers=YELP_HEADERS, params=params)
            response.raise_for_status()
            businesses = response.json().get('businesses', [])
            
            for business in businesses:
                rating = business.get('rating', 0)
                review_count = business.get('review_count', 0)

                # NEW: Initial filter to skip low-quality leads
                if review_count < REVIEW_FILTER_MIN_COUNT or rating < RATING_FILTER_MIN:
                    continue

                lead = {
                    'name': business.get('name'),
                    'address': business.get('location', {}).get('display_address', [''])[0],
                    'phone': business.get('phone'),
                    'business_id': business.get('id'),
                    'rating': rating,
                    'review_count': review_count,
                    'business_type': term,
                    'source': 'Yelp API',
                    'reviews': []
                }
                all_leads.append(lead)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Yelp API request failed for '{term}': {e}")
            break
        
        time.sleep(1)
            
    return all_leads

def get_yelp_reviews(business_id):
    """
    Fetches the top 3 reviews for a given business ID from the Yelp API.
    Returns a list of review text strings.
    """
    reviews_url = f"https://api.yelp.com/v3/businesses/{business_id}/reviews"
    reviews = []
    try:
        response = requests.get(reviews_url, headers=YELP_HEADERS)
        response.raise_for_status()
        reviews_json = response.json().get('reviews', [])
        for review in reviews_json:
            reviews.append(review.get('text', ''))
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            pass
        elif e.response.status_code == 429:
            print(f"❌ Failed to fetch reviews for business ID {business_id}: 429 Client Error: Too Many Requests.")
        else:
            print(f"❌ Failed to fetch reviews for business ID {business_id}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch reviews for business ID {business_id}: {e}")
    
    time.sleep(0.5)
    return reviews


def ai_evaluate_batch(batch, suite_sizes, property_city):
    """
    Evaluates a batch of leads using the Gemini API.
    Returns a list of leads with evaluation results.
    """
    results = []
    
    prompt_parts = [
        "You are an expert commercial real estate analyst. "
        "Your task is to evaluate a list of businesses to determine their likelihood of being a viable tenant "
        "for a commercial property. Your final response must be a JSON array of objects. "
        "The objects should have the keys 'Likelihood' ('High', 'Medium', or 'Low'), 'Score' (1-100), and 'Reasoning'.\n\n",
        f"The property for lease has suites available in the size range of {suite_sizes} in {property_city}.\n",
        "Here are the businesses to evaluate:\n"
    ]

    for lead in batch:
        prompt_parts.append(f"- Business: {lead['name']}, Type: {lead['business_type']}, Rating: {lead['rating']}, Review Count: {lead['review_count']}\n")
        if lead['reviews']:
            prompt_parts.append("  Recent Reviews:\n")
            for review in lead['reviews']:
                prompt_parts.append(f"  - \"{review}\"\n")
    
    prompt = "".join(prompt_parts)

    try:
        chat_history = []
        chat_history.append({ "role": "user", "parts": [{ "text": prompt }] })
        payload = {
            "contents": chat_history,
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "Likelihood": { "type": "STRING" },
                            "Score": { "type": "NUMBER" },
                            "Reasoning": { "type": "STRING" }
                        },
                        "propertyOrdering": ["Likelihood", "Score", "Reasoning"]
                    }
                }
            }
        }
        
        retry_count = 0
        max_retries = 5
        while retry_count < max_retries:
            try:
                apiUrl = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={API_KEY}"
                response = requests.post(apiUrl, headers={'Content-Type': 'application/json'}, data=json.dumps(payload))
                response.raise_for_status()
                result = response.json()
                
                if result.get("candidates") and result["candidates"][0].get("content") and result["candidates"][0]["content"].get("parts"):
                    raw_json = result["candidates"][0]["content"]["parts"][0]["text"]
                    evals = json.loads(raw_json)
                    break
                else:
                    print(f"❌ Gemini response was missing candidates or parts.")
                    retry_count += 1
                    time.sleep(2 ** retry_count)
                    continue
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    print("❌ Gemini API request failed: You have been rate-limited. Retrying with exponential backoff...")
                elif e.response.status_code >= 400 and e.response.status_code < 500:
                    print(f"❌ Gemini API request failed with a client error ({e.response.status_code}). Check your API key or usage limits.")
                    return []
                else:
                    print(f"❌ Gemini API request failed with a server error ({e.response.status_code}). Retrying...")
                retry_count += 1
                time.sleep(2 ** retry_count)
            except requests.exceptions.RequestException as e:
                print(f"❌ Gemini request failed: {e}")
                retry_count += 1
                time.sleep(2 ** retry_count)
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON from Gemini response: {e}")
                retry_count += 1
                time.sleep(2 ** retry_count)
        else:
            print("❌ Max retries exceeded. Skipping batch.")
            return []

    except Exception as e:
        print(f"❌ Gemini request failed unexpectedly: {e}")
        return []

    # Map the AI evaluation results back to the original leads
    evaluated_leads = []
    for idx, eval_row in enumerate(evals):
        if idx >= len(batch):
            break
        
        lead = batch[idx].copy()
        lead["Likelihood"] = eval_row.get("Likelihood", "Low")
        lead["Score"] = eval_row.get("Score", 0)
        lead["Reasoning"] = eval_row.get("Reasoning", "")
        evaluated_leads.append(lead)

    # Sort the results by score in descending order
    sorted_leads = sorted(evaluated_leads, key=lambda x: x.get('Score', 0), reverse=True)

    final_output = []
    for lead in sorted_leads:
        if FILTER_HIGH_ONLY and lead.get("Likelihood", "Low") != "High":
            continue
            
        final_output.append([
            lead.get("name"),
            lead.get("address"),
            lead.get("phone"),
            lead.get("rating"),
            lead.get("review_count"),
            lead.get("business_type"),
            lead.get("source"),
            lead.get("Likelihood", "Low"),
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            f"Score: {lead.get('Score', 0)} | {lead.get('Reasoning', '')}"
        ])

    return final_output

def run_scan():
    """
    This function orchestrates the entire workflow: reading from the control sheet,
    searching for leads, fetching reviews, and writing the results to new sheets.
    """
    print("🚀 Starting Tenant Hunter program...")
    
    control_data = read_from_sheet(CONTROL_SHEET_NAME)
    if not control_data:
        print("🛑 Failed to retrieve data from Control Sheet. Exiting.")
        return
    
    control_header = control_data[0]
    control_rows = control_data[1:]

    for control_row in control_rows:
        control_dict = dict(zip(control_header, control_row))
        
        if (control_dict.get("Status (paused/active)", "").strip().lower() != "active"):
            print(f"Skipping '{control_dict.get('Property Name', 'N/A')}' - not active.")
            continue
            
        print(f"--- Processing Property: {control_dict['Property Name']} ---")
        
        property_city = control_dict["City"]
        search_terms = [term.strip() for term in control_dict["Search Terms"].split(',')]
        sheet_prefix = control_dict["Sheet Prefix"]
        suite_sizes = control_dict["Suite Sizes"]
        
        all_evaluated_leads = []
        
        # New loop to process and sort leads per search term
        for term in search_terms:
            print(f"Searching Yelp for new leads with term: '{term}'...")
            leads = yelp_search_leads(term, property_city, limit=50)
            
            # The filtering based on review count and rating is now handled inside yelp_search_leads
            
            if not leads:
                print(f"No leads found for '{term}' that meet the filtering criteria.")
                continue
                
            # Fetch reviews only for leads that pass the initial filter
            for lead in leads:
                business_id = lead.get('business_id')
                if business_id:
                    reviews = get_yelp_reviews(business_id)
                    lead['reviews'] = reviews
            
            # Evaluate and sort this term's batch of leads
            evaluated_term_leads = []
            num_batches = math.ceil(len(leads) / BATCH_SIZE)
            if num_batches > 0:
              for i in range(0, len(leads), BATCH_SIZE):
                  batch = leads[i:i + BATCH_SIZE]
                  print(f"Processing batch {i//BATCH_SIZE + 1} of {num_batches} for '{term}'...")

                  evaluated_batch = ai_evaluate_batch(batch, suite_sizes, property_city)
                  evaluated_term_leads.extend(evaluated_batch)
            else:
              print(f"No leads to process for '{term}'.")
            
            # Add the sorted, evaluated leads for this term to the main list
            all_evaluated_leads.extend(evaluated_term_leads)

        if not all_evaluated_leads:
            print(f"No leads passed the filters and evaluation for '{control_dict['Property Name']}'.")
            continue
            
        output_sheet_name = f"{sheet_prefix}_RankedLeads"
        
        output_header = ["Business Name", "Address", "Phone", "Rating", "Review Count", "Business Type", "Source"]
        output_header += ["Likelihood", "Run Timestamp", "Reasoning"]
        
        write_to_sheet(output_sheet_name, output_header, all_evaluated_leads)
        
        update_control_sheet_status(sheet_prefix)

    print("🎉 Program finished successfully.")

# --- MAIN EXECUTION LOGIC ---
if __name__ == "__main__":
    run_scan()
