import requests
import os
from datetime import datetime
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
import io
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import pandas as pd
import time
import random

"""
Authenticates using the service account and sets up the Drive API client.
Scrapes the required data from the URL.
Before uploading each file, it checks if a file with the same name already exists in the target Google Drive folder.
If the file doesnt exist, it uploads the new file and prints the upload details. If it does exist, it skips the upload and notifies that the file already exists.
"""
#--------------------------------------
#DEF List

# Define a logging function
def log(message):
    print(f"{datetime.now()}: {message}")

#Authenticate and create a Google API service client using OAuth 2.0 credentials
#with a focus on token persistence to avoid repeated user logins
def Create_Service(client_secret_file, api_name, api_version, *scopes):
    log(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    log(SCOPES)

    cred = None

    pickle_file = f'token_{API_SERVICE_NAME}_{API_VERSION}.pickle'
    # print(pickle_file)

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        log(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        log('Unable to connect.')
        log(e)
        return None

#Execute request with number of retry and delay time to combat http error.
def execute_with_retries(request, max_retries=5, initial_delay=2):
    retries = 0
    delay = initial_delay

    while retries < max_retries:
        try:
            return request.execute()
        except HttpError as error:
            if error.resp.status == 500:
                log(f'Internal Error: {error}. Retrying in {delay} seconds...')
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                retries += 1
            else:
                raise  # Re-raise the error if it's not an internal server error
    raise Exception(f'Failed to execute request after {max_retries} retries')

# Function to create a folder in Google Drive
def create_folder(folder_name, parent_folder_id):
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id]
    }
    
    request = service.files().create(body=file_metadata, fields='id, name')
    file = execute_with_retries(request)
    return file.get('id')             
                         
# Function to check if a folder exists
def get_folder_id(folder_name, parent_folder_id):
    query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
    results = execute_with_retries(request)
    folders = results.get('files', [])
    if folders:
        return folders[0]['id']
    return None                

# Function to find an existing Google Sheet by name
def get_sheet_id(service, parent_folder_id, sheet_name):
    query = f"name='{sheet_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
    results = execute_with_retries(request)
    sheets = results.get('files', [])
    if sheets:
        return sheets[0]['id']
    return None                      
                   
# Get the list of existing files in the target folder
def list_existing_files(folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    request = service.files().list(q=query, spaces='drive', fields='files(id, name)')
    results = execute_with_retries(request)
    files = results.get('files', [])
    existing_files = {file['name'] + '.csv' for file in files}
    return existing_files

# Function to upload a file to Google Drive
def upload_to_drive(file_name, file_content, parent_folder_id, existing_files):
    if file_name in existing_files:
        log(f'{file_name} already exists in Google Drive.')
        return False

    file_metadata = {
        'name': file_name,
        'parents': [parent_folder_id],
        'mimeType': 'application/vnd.google-apps.spreadsheet'
    }
    
    file_content = headers.encode('utf-8') + file_content
    
    media = MediaIoBaseUpload(io.BytesIO(file_content), mimetype='text/csv')
    request = service.files().create(body=file_metadata, media_body=media, fields='id, name, mimeType, webViewLink, parents')
    file = execute_with_retries(request)
    log(f'Uploaded {file.get("name")}')
    log(f'File ID: {file.get("id")}')
    log(f'File MIME Type: {file.get("mimeType")}')
    log(f'File Web View Link: {file.get("webViewLink")}\n')
    existing_files.add(file_name)  # Add the uploaded file to the existing files set

#Function to create Google sheet
def create_Google_Sheet(service,parent_folder_id,File_Name):
    google_sheet_body = {
        'name': File_Name,
        'parents': [parent_folder_id],
        'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
    
    spreadsheet = service.files().create(body=google_sheet_body, fields='id, name').execute()
    return spreadsheet.get('id')

#Function to create sheet(tab)
def Create_Sheet (service,SpreadSheetID,Sheet_Name,headers):
    add_sheet_body = {
    'requests': [{
        'addSheet': {
            'properties': {
                'title': Sheet_Name
            }
        }
    }]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=SpreadSheetID, body=add_sheet_body).execute()
    
    #Add header to tab
    header_list = headers.split(",")
    data = [header_list]
    range_name = f"{Sheet_Name}!A1"
    header = {
        'values': data}
    service.spreadsheets().values().append(spreadsheetId=SpreadSheetID,body=header,valueInputOption="RAW",range=range_name).execute()
    
    
   # Function to write data to a specific sheet
def write_to_sheet(service, spreadsheet_id, sheet_name, df):
    data = df.values.tolist()
    #data.insert(0, df.columns.tolist())
    body = {
        'values': data
    }
    range_name = f"{sheet_name}!A1"
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()
    
    
  #Function to filter out data based on Name List 
def Include_Row(df,list_name):
    return df[df[2].str.startswith(tuple(list_name))]

# process batch of data by year and apply waiting time
# Waiting time of 2 minutes is appied when running into error, then rerun the upload where it's left off.
def process_batches(service, sheet_service, folder_id, csv_list, headers, state, Master_Sheet_Id, types):
    batch_size = 1  # Process 2 years in each batch
    years_processed = 0  # Track number of years processed
    processed_files = set()  # Keep track of processed files

    while years_processed < len(csv_list) // (len(months) * len(types)):
        try:
            for i in range(years_processed * batch_size * len(months) * len(types), len(csv_list), batch_size * len(months) * len(types)):
                batch = csv_list[i:i + batch_size * len(months) * len(types)]
                year_folders = {}  # Keep track of folders created within the batch

                for csv in batch:
                    if csv in processed_files:
                        continue  # Skip already processed files

                    year_csv = csv.split("/")[0]
                    full_url = base_url + csv
                    while True:  # Retry loop
                        try:
                            response = requests.get(full_url, timeout=30)  # Set timeout to 30 seconds
                            break  # Exit the loop if the request was successful
                        except requests.exceptions.Timeout:
                            log(f'Timeout occurred while downloading {csv}. Retrying...')
                            time.sleep(60)  # Wait for 1 minute before retrying
                        except requests.exceptions.ConnectionError:
                            log(f'Connection error occurred while downloading {csv}. Retrying...')
                            time.sleep(60)  # Wait for 1 minute before retrying

                    if response.status_code == 200:
                        if year_csv not in year_folders:
                            # Check if the folder for the year already exists
                            year_folder_id = get_folder_id(year_csv, folder_id)
                            
                            if not year_folder_id:
                                # Create a folder for the year if it doesn't exist
                                year_folder_id = create_folder(year_csv, folder_id)
                                
                            year_folders[year_csv] = year_folder_id
                            existing_files = list_existing_files(year_folder_id)  # Update the list of existing files for the new folder
                            
                        else:
                            year_folder_id = year_folders[year_csv]
                        
                        file_name = csv.split("/")[-1]  # Use only the file name part
                        Upload = upload_to_drive(file_name, response.content, year_folder_id, existing_files)
                        
                        # Append data to the master Google Sheet if new file 
                        if Upload is not False:
                            file_content = response.content.decode('utf-8')
                            df = pd.read_csv(io.StringIO(file_content), header=None)
                            df = Include_Row(df, state)  # Filter only data that start with State Abbreviation
                            sheet_name = csv.split("-")[0].split("/")[1]
                            write_to_sheet(sheet_service, Master_Sheet_Id, sheet_name, df)

                        processed_files.add(csv)  # Mark the file as processed
                    else:
                        log(f'Failed to download {csv}')
                
                years_processed += batch_size
                log(f'Processed {years_processed} years of data.')
                if years_processed < len(csv_list) // (len(months) * len(types)):
                    log('Waiting for 1 minutes before processing the next batch...')
                    time.sleep(60)  # Wait for 2 minutes (120 seconds)
        except Exception as e:
            log(f'An error occurred: {e}')
            log('Waiting for 1 minutes before retrying...')
            time.sleep(60)  # Wait for 2 minutes before retrying
    
            
#--------------------------------------------------------------
#CODE EXECUTION PART

try:
     #Get the directory of the current script
     script_dir = os.path.dirname(os.path.abspath("daen690-vcc-fbb986abb488-service-key.json"))

except NameError:
    # __file__ is not defined, assume current working directory is the script directory
    script_dir = os.getcwd()


# Use the service account JSON key file for authentication
SERVICE_ACCOUNT_FILE = os.path.join(script_dir, "daen690-vcc-fbb986abb488-service-key.json")
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

#Create Service to create/update google Drive folder
service = build('drive', 'v3', credentials=credentials)
#Create Service to create/update google sheet
sheet_service = build('sheets', 'v4', credentials=credentials)



# Base URL for data
base_url = "https://www.ncei.noaa.gov/data/nclimgrid-daily/access/averages/"

# Define the headers to be added
headers = "Region Type,Region Code,Region Name,Year,Month,Variable Type,"+ ",".join(map(str, list(range(1, 32)))) + "\n"

#List of States for filter
state = ['VA']

# Defining the current year
current_year = int(datetime.now().year)

# Setting the range back 30 years until the current year 
years = [*range(1951, current_year + 1, 1)] 

# Defining the 4 measurement types and the months
types = ['prcp','tavg','tmax','tmin']
months = [*range(1,13)] #!!!!!!!!!!switch it back to 13


# Creating the list of CSV filenames
csv_list = []
for year in years:
    for month in months:
        for type in types:
            csv_filename = str(year) + "/" + type + "-" + str(year) + str('%02d' % month) + "-cty-scaled.csv"
            csv_list.append(csv_filename)


# Define the Google Drive parent folder ID where the files will be uploaded
folder_id = '1BtzVsVUoc40eVh9fTd91FB5SDoP2qWui'  # Replace with your folder ID

#Check for Master Sheet.
Master_Sheet_Name = "TEST_VCC Climate Master Sheet"
Master_Sheet_Id = get_sheet_id(service, folder_id, Master_Sheet_Name)

#Create a master spreadsheet if there is none yet.
if not Master_Sheet_Id:
    Master_Sheet_Id = create_Google_Sheet(service,folder_id,Master_Sheet_Name)
    
    for tab_name in types:
        Create_Sheet(sheet_service, Master_Sheet_Id, tab_name,headers)
else:
    log("Master Sheet exists.")

# Get the list of existing files in the target folder
existing_files = list_existing_files(folder_id)

#run batches with a wait time between each batch
process_batches(service, sheet_service, folder_id, csv_list, headers, state, Master_Sheet_Id, types)

log('Data scraping and upload completed.')
