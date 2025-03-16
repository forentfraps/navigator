from yapi import yAPI 
import os
import json

def verify_integrity(filename: str) -> bool:
    exists = os.path.exists(filename)
    if (not exists):
        return exists
    
    with open(filename, "rb") as f:
        data = f.read()
    for elem in data:
        if elem != 0:
            return True
    return False

def query_all_settlement_schedules(api, force_download=False):
    """
    Iterates over all settlements from the stations data,
    queries the schedule for each using its Yandex code as the station code,
    and saves the JSON result in a file named '<station_code>.json'
    inside the 'routes/' folder.
    
    Parameters:
      api (yAPI): An instance of your yAPI class.
      force_download (bool): Whether to force a fresh download of the stations data.
    """
    # Ensure that the routes folder exists
    os.makedirs("routes", exist_ok=True)
    
    # Get the stations data (this includes all countries, regions, and settlements)
    data = api.get_stations_data(force_download=force_download)
    
    # Iterate over all settlements
    for country in data.get("countries", []):
        for region in country.get("regions", []):
            for settlement in region.get("settlements", []):
                for station in settlement.get("stations", []):
                    # Retrieve the settlement code (Yandex code)
                    station_code = station.get("codes", {}).get("yandex_code")
                    title = station.get("title")
                    if not station_code:
                        logging.info("No yandex_code found for station, skipping...")
                        continue
    
                    logging.info(f"Querying schedule for {title}: {station_code}")
                    try:
                        if (not verify_integrity("routes/"+ station_code+".json")):
                            logging.info(f"Failed to find a valid code for {title} : {station_code}")
                        else:
                            logging.info(f"skipping {station_code}")
                            continue
                        # Query the schedule with limit set to 250
                        schedule_data = api.station_schedule(station=station_code, limit=250)
                        # Define the output filename based on settlement code
                        output_file = os.path.join("routes", f"{station_code}.json")
                        # Save the schedule data to the file
                        with open(output_file, "w", encoding="utf-8") as f:
                            json.dump(schedule_data, f, ensure_ascii=False, indent=4)
                    except Exception as e:
                        logging.info(f"Failed to query schedule for station{station_code}: {e}")


if __name__ == "__main__":

    api = yAPI()
    query_all_settlement_schedules(api)


