import os
import shutil
import requests
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Access environment variables
un_url = os.getenv("UN_URL")
ofac_consolidated_url = os.getenv("OFAC_CONSOLIDATED_URL")
ofac_sdn_url = os.getenv("OFAC_SDN_URL")
eu_url = os.getenv("EU_URL")
uk_url = os.getenv("UK_URL")
au_url = os.getenv("AU_URL")

print("UN URL:", un_url)
print("OFAC Consolidated URL:", ofac_consolidated_url)
print("OFAC SDN URL:", ofac_sdn_url)
print("EU URL:", eu_url)
print("UK URL:", uk_url)
print("AU URL:", au_url)

# URLs for XML documents
urls = {
    "un": un_url,
    "ofac_consolidated": ofac_consolidated_url,
    "ofac_sdn": ofac_sdn_url,
    "eu": eu_url,
    "uk": uk_url,
    "au": au_url
}

# Directories
latest_folder = "Sanction/Latest"
archive_folder = "Sanction/Archive"

# Ensure directories exist
os.makedirs(latest_folder, exist_ok=True)
os.makedirs(archive_folder, exist_ok=True)

# Download and save XML files
def download_files():
    downloaded_files = []
    for key, url in urls.items():
        try:
            print(f"Downloading from {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise error for HTTP issues
            if 'au' in key:
                file_extention='.xlsx'
            else:
                file_extention='.xml'
            # Save to the Latest folder
            file_name = f"{key}_{datetime.now().strftime('%Y%m%d')}{file_extention}"
            latest_file_path = os.path.join(latest_folder, file_name)
            with open(latest_file_path, "wb") as file:
                file.write(response.content)

            downloaded_files.append(file_name)
            print(f"Downloaded and saved: {latest_file_path}")

        except requests.exceptions.RequestException as e:
            print(f"Failed to download {url}: {e}")
    return downloaded_files

# Move outdated files to Archive
def archive_old_files(downloaded_files):
    today_date = datetime.now().strftime('%Y%m%d')
    for file_name in os.listdir(latest_folder):
        file_date = file_name.split("_")[-1].split(".")[0]
        source_path = os.path.join(latest_folder, file_name)

        if file_date != today_date:  # Check if the file was not downloaded today
            destination_path = os.path.join(archive_folder, file_name)
            shutil.move(source_path, destination_path)
            print(f"Archived outdated file: {file_name}")

# Clean up the Archive folder
def clean_archive():
    two_days_ago = datetime.now().timestamp() - (2 * 24 * 60 * 60)  # Calculate timestamp for 2 days ago

    for file_name in os.listdir(archive_folder):
        file_path = os.path.join(archive_folder, file_name)

        # Check if it's a file (not a directory)
        if os.path.isfile(file_path):
            # Get the last modification time of the file
            file_mod_time = os.path.getmtime(file_path)

            # Delete the file if it is older than 2 days
            if file_mod_time < two_days_ago:
                os.remove(file_path)
                print(f"Deleted old archived file: {file_name}")

# Execute functions
if __name__ == "__main__":
    downloaded_files = download_files()  # Step 1: Download files
    archive_old_files(downloaded_files)  # Step 2: Archive outdated files
    clean_archive()  # Step 3: Clean the archive folder
