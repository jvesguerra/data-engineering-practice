import requests
import zipfile
import os
import aiohttp
import asyncio

downloads_dir = "downloads"

def download_file(url, file):
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)
        
    # Construct the full file path
    full_file_path = os.path.join(downloads_dir, file)
                
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        with open(full_file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"File downloaded successfully to {full_file_path}")
        return None  # Indicate success
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return e
    except OSError as e:
        print(f"OS Error during download: {e}")
        return e
    except Exception as e:
        print(f"An unexpected error occurred during download: {e}")
        return e
    
async def download_file_async(session, url, file):
    """Downloads a file asynchronously using aiohttp."""
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)
    full_file_path = os.path.join(downloads_dir, file)
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            with open(full_file_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    f.write(chunk)
        print(f"File downloaded successfully to {full_file_path}")
        return None  # Indicate success
    except aiohttp.ClientError as e:
        print(f"Error downloading file: {e}")
        return e
    except OSError as e:
        print(f"OS Error during download: {e}")
        return e
    except Exception as e:
        print(f"An unexpected error occurred during download: {e}")
        return e
        
def extract(file):
    # Construct the full file path
    full_file_path = os.path.join(downloads_dir, file)
    
    try:
        with zipfile.ZipFile(full_file_path, 'r') as zObject:
            # Extracting all the members of the zip into a specific location.
            zObject.extractall(path=downloads_dir)

        print(f"Successfully extracted '{file}' to '{downloads_dir}'")

    except FileNotFoundError:
        print(f"Error: Zip file '{file}' not found.")
    except zipfile.BadZipFile:
        print(f"Error: '{file}' is not a valid zip file.")
    except PermissionError:
        print(f"Error: Permission denied while extracting to '{downloads_dir}'.")
    except OSError as e: # Catch other OS errors, like disk full.
        print(f"OS Error during extraction: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during extraction: {e}")
        
def delete_zip_file(file):
    """Deletes a zip file at the given filepath."""
    # Construct the full file path
    full_file_path = os.path.join(downloads_dir, file)
    
    try:
        os.remove(full_file_path)
        print(f"Successfully deleted: {full_file_path}")
    except FileNotFoundError:
        print(f"Error: File not found: {full_file_path}")
    except PermissionError:
        print(f"Error: Permission denied to delete: {full_file_path}")
    except OSError as e:
        print(f"OS Error deleting {full_file_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
async def main():
    # your code here
    download_uris = [
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
        "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
    ]
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in download_uris:
            file = url.split("/")[-1]
            tasks.append(download_file_async(session, url, file))
        results = await asyncio.gather(*tasks)

        for i, result in enumerate(results):
            file = download_uris[i].split("/")[-1]
            if result:
                print(f"Skipping {file} due to download error: {result}")
            else:
                extract(file)
                delete_zip_file(file)
    
if __name__ == "__main__":
    asyncio.run(main())
