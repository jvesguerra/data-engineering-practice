import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

def scrape_website(url, element_selector):
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}) #Important to set a user agent
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        soup = BeautifulSoup(response.content, "html.parser")
        elements = soup.select(element_selector)
        data = [element.text.strip() for element in elements]

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def download_file(url, file, downloads_dir):
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

def main():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"  # Replace with the target URL
    element_selector = "tr"  # Replace with the correct CSS selector
    scraped_data = scrape_website(url, element_selector)
    target_date_time = "2024-01-19 10:27"
    downloads_dir = "downloads"
    max_records = {}

    if scraped_data: 
        for row in scraped_data:
            parts = row.split()
            try:
                file,date = parts[0].split('.csv', maxsplit=1)
                date_time = date + " " + parts[1]
                filename = file + ".csv"

                if target_date_time == date_time:
                    download_url = url + filename
                    download_file(download_url,filename,downloads_dir)
                    
                    file_path = os.path.join(downloads_dir, filename)
                    df = pd.read_csv(file_path, low_memory=False)
                    max_value = df['HourlyDryBulbTemperature'].max()
                    max_row = df[df['HourlyDryBulbTemperature'] == max_value]

                    #max_records[filename] = max_row
                    max_records[filename] = max_value
                    
            except ValueError:
                print(f"Error: Could not split '{parts[0]}'. '.csv' not found.")
            except IndexError:
                print("Error: parts list is too short.")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                
    print(max_records)
                

            
            


if __name__ == "__main__":
    main()
