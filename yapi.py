#yapi.py
import requests
import json
import os
import glob
import datetime

class yAPI:
    def __init__(self, cache_file="resp.json", miss_cache_file="station_schedule_misses.json"):
        self.url = "https://api.rasp.yandex.net/v3.0"
        # API key as parameter (or via Authorization header)
        self.apikey = "apikey=70aafa03-0fdb-4aed-a88e-94972422528d"
        self.format = "format=json"
        self.lang = "lang=ru_RU"
        self.cache_file = cache_file
        self.miss_cache_file = miss_cache_file
        # We'll load any previously saved "miss" stations from disk
        self._station_schedule_miss = set()
        self._load_miss_cache()
    
    def get(self, endpoint, extraparams=""):
        url = f"{self.url}/{endpoint}/?{self.apikey}&{self.format}&{self.lang}{extraparams}"
        resp = requests.get(url)
        resp.raise_for_status()  # Raise an error if the request failed
        return resp.content
    def _load_miss_cache(self):
        """Load the 'miss' set from a JSON file if it exists."""
        if os.path.exists(self.miss_cache_file):
            try:
                with open(self.miss_cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # data is presumably a list, we convert to a set for faster membership checks
                self._station_schedule_miss = set(data)
            except Exception as e:
                print(f"Warning: failed to read {self.miss_cache_file}: {e}")

    def _save_miss_cache(self):
        """Write the current _station_schedule_miss set to the JSON file."""
        try:
            with open(self.miss_cache_file, "w", encoding="utf-8") as f:
                json.dump(list(self._station_schedule_miss), f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Warning: failed to write {self.miss_cache_file}: {e}")

    def stations_list(self):
        """
        Downloads the full stations list from the API and saves it to the cache file.
        """
        data = self.get("stations_list")
        with open(self.cache_file, "wb") as f:
            f.write(data)
    
    def get_stations_data(self, force_download=False):
        """
        Returns the parsed JSON data for stations.
        If force_download is False and the cache file exists on the system,
        the data is loaded from the file. Otherwise, it downloads from the API.
        """
        if force_download or not os.path.exists(self.cache_file):
            print("Downloading stations data...")
            self.stations_list()
        with open(self.cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    
    def search_stations(self, query, force_download=False):
        """
        Searches for stations where the station title includes the given query (case-insensitive).
        Returns a list of matching stations with key details.
        """
        data = self.get_stations_data(force_download=force_download)
        results = []
        count = 0
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    for station in settlement.get("stations", []):
                        count += 1
                        if query.lower() in station.get("title", "").lower():
                            station_info = {
                                "title": station.get("title"),
                                "yandex_code": station.get("codes", {}).get("yandex_code"),
                                "esr_code": station.get("codes", {}).get("esr_code"),
                                "latitude": station.get("latitude"),
                                "longitude": station.get("longitude"),
                                "transport_type": station.get("transport_type"),
                                "station_type": station.get("station_type")
                            }
                            results.append(station_info)
        print(count)
        return results

    def search_settlements(self, query, force_download=False):
        """
        Searches for settlements where the settlement title includes the given query (case-insensitive).
        Returns a list of matching settlements with key details.
        
        According to the API, a settlement contains:
          - title: The name of the settlement.
          - codes: An object with the Yandex code (key "yandex_code").
          - stations: A list of station objects within the settlement.
          
        This implementation also includes the parent region and country names.
        """
        data = self.get_stations_data(force_download=force_download)
        results = []
        total_settlements = 0
        
        for country in data.get("countries", []):
            country_title = country.get("title", "")
            for region in country.get("regions", []):
                region_title = region.get("title", "")
                for settlement in region.get("settlements", []):
                    total_settlements += 1
                    if query.lower() in settlement.get("title", "").lower():
                        settlement_info = {
                            "title": settlement.get("title"),
                            "yandex_code": settlement.get("codes", {}).get("yandex_code"),
                            "country": country_title,
                            "region": region_title,
                            # "stations": settlement.get("stations", [])
                        }
                        results.append(settlement_info)
        print(f"Total settlements processed: {total_settlements}")
        return results

    def station_schedule(
        self,
        station,
        date=None,
        direction=None,
        transport_types=None,
        offset=0,
        limit=100,
        max_day_lookahead=3
    ):
        """
        Retrieves schedule (list of trips) for the specified station and
        tries multiple dates if:
           - no explicit date given, OR
           - the schedule is empty for the initial date.

        We loop up to `max_day_lookahead` (default=3 days).
        Merges all found schedules into a single data dict.

        Returns the final JSON data dict or None if not available.
        """
        if station in self._station_schedule_miss:
            print(f"[station_schedule] Station {station} is in miss cache, returning None.")
            return None

        # If user did not provide date, start from "today"
        # Or you might set default to tomorrow, etc.:
        if not date:
            date = datetime.date.today().strftime("%Y-%m-%d")

        # We’ll accumulate all flights in one big ‘schedule’ list:
        combined_data = None
        day_count = 0

        while day_count < max_day_lookahead:
            current_date_str = (
                datetime.datetime.strptime(date, "%Y-%m-%d") +
                datetime.timedelta(days=day_count)
            ).strftime("%Y-%m-%d")
            print(f"[station_schedule] Attempting date={current_date_str} ...")

            # -- do an actual request for that date --
            single_day_data = self._fetch_station_schedule_once(
                station=station,
                date=current_date_str,
                direction=direction,
                transport_types=transport_types,
                offset=offset,
                limit=limit
            )

            # If request failed or returned None => break/skip
            if not single_day_data:
                print(f"[station_schedule] No data returned for {current_date_str}")
                day_count += 1
                continue

            # If first successful day, store a copy to combined_data
            if combined_data is None:
                combined_data = single_day_data
            else:
                # Merge: combine "schedule" from single_day_data into combined_data
                sched1 = combined_data.get("schedule", [])
                sched2 = single_day_data.get("schedule", [])
                sched_merged = sched1 + sched2
                combined_data["schedule"] = sched_merged

            day_count += 1

            # If the user gave an explicit date, do NOT keep looping
            # — unless you actually want to loop anyway. 
            if date:
                break

        if combined_data:
            # Save to schedule.json so lazygraph sees it, etc.
            with open("schedule.json", "wb") as f:
                f.write(json.dumps(combined_data, ensure_ascii=False, indent=2).encode("utf-8"))
            return combined_data

        # If we never got any schedules
        print(f"[station_schedule] No schedules found for station={station} in {max_day_lookahead} days.")
        return None

    def _fetch_station_schedule_once(self, station, date, direction, transport_types, offset, limit):
        """
        Helper that does a single request for station-schedule on the given date
        and handles pagination. If everything is good, returns the schedule data dict;
        else returns None.
        """
        # Build params
        extraparams = f"&station={station}&offset={offset}&limit={limit}&date={date}"
        if direction:
            extraparams += f"&direction={direction}"
        if transport_types:
            extraparams += f"&transport_types={transport_types}"

        try:
            data_bytes = self.get("schedule", extraparams)
        except requests.HTTPError as e:
            print(f"[station_schedule] HTTPError for station={station}, date={date}. Error: {e}")
            self._station_schedule_miss.add(station)
            self._save_miss_cache()
            return None
        except Exception as e:
            print(f"[station_schedule] General error for station={station}, date={date}. Error: {e}")
            self._station_schedule_miss.add(station)
            self._save_miss_cache()
            return None

        data = json.loads(data_bytes)
        if not data.get("schedule"):
            # Possibly empty schedule, no trips
            return None

        # pagination check
        pagination = data.get("pagination", {})
        total = pagination.get("total", 0)
        current_limit = pagination.get("limit", limit)
        if total > current_limit:
            # re-request with limit=total
            new_extraparams = f"&station={station}&offset=0&limit={total}&date={date}"
            if direction:
                new_extraparams += f"&direction={direction}"
            if transport_types:
                new_extraparams += f"&transport_types={transport_types}"

            try:
                second_data_bytes = self.get("schedule", new_extraparams)
                data2 = json.loads(second_data_bytes)
                return data2
            except Exception as e:
                print(f"[station_schedule] Second fetch error: {e}")
                return data
        else:
            return data
    def between2stations(self, code1, code2, date):
        """
        Retrieves route information between two stations based on their codes and the given date.
        The resulting JSON data is saved to "route.json".
        """
        data = self.get("search", f"&from={code1}&to={code2}&date={date}")
        with open("route.json", "wb") as f:
            f.write(data)
    def thread_stops(self, uid, date=None, from_code=None, to_code=None, show_systems="all"):
        """
        Lazy-load from threads/<uid>.json if present, else call the API 
        and save to that file. Then return the JSON.
        """
        # ensure folder
        os.makedirs("threads", exist_ok=True)
        filtered_uid = uid.replace("*", "")
        #we cannot save a file with a * in its name sadly
        uid_mask = filtered_uid.split("_")[0]

        # optimisation since it otherwise downloads a lot of same threads but for slightly different dates
        if uid_mask == "empty":
            local_file = os.path.join("threads", f"{filtered_uid}.json")
            if os.path.exists(local_file):
                # already cached
                with open(local_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        else:
            pattern = os.path.join("threads", f"{uid_mask}_*.json")
            matching_files = glob.glob(pattern)
            if matching_files:
                local_file = matching_files[0]
                if os.path.exists(local_file):
                    with open(local_file, "r", encoding="utf-8") as f:
                        return json.load(f)
            else:
                local_file = os.path.join("threads", f"{filtered_uid}.json")

        # not cached, do the actual call
        extraparams = f"&uid={uid}&show_systems={show_systems}"
        if date:
            extraparams += f"&date={date}"
        if from_code:
            extraparams += f"&from={from_code}"
        if to_code:
            extraparams += f"&to={to_code}"
        print(f"[Q] thread {uid}")

        data_bytes = self.get("thread", extraparams)
        data_json = json.loads(data_bytes)

        # Save to disk for future calls
        print(f"saving f{local_file}")
        with open(local_file, "w", encoding="utf-8") as f:
            json.dump(data_json, f, ensure_ascii=False, indent=2)

        return data_json
    def get_settlement_station_codes(self, settlement_yandex_code, force_download=False):
        """
        Return a list of station 'yandex_code' values belonging to the settlement
        whose settlement 'yandex_code' = settlement_yandex_code.
        
        Example:
          get_settlement_station_codes("c39") -> [ 's9601445', 's9601446', ... ]
        """
        data = self.get_stations_data(force_download=force_download)
        stations_list = []
    
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    # settlement code
                    s_code = settlement.get("codes", {}).get("yandex_code", None)
                    if s_code == settlement_yandex_code:
                        # Found the settlement we want
                        for station in settlement.get("stations", []):
                            st_code = station.get("codes", {}).get("yandex_code")
                            if st_code:
                                stations_list.append(st_code)
                        return stations_list

        # If not found, we return an empty list
        return stations_list
# Example usage:
if __name__ == '__main__':
    api = yAPI()
    
    # Example: Search for stations with a query (e.g., "Москва")
    # query = input("Enter station search query: ")
    #matching_stations = api.search_stations("Москва")
    #
    # if matching_stations:
    #     print(f"Found {len(matching_stations)} matching station(s):")
    #     for station in matching_stations:
    #         print(f"Station: {station['title']}")
    #         print(f"  Yandex Code: {station['yandex_code']}")
    #         if station['esr_code']:
    #             print(f"  ESR Code: {station['esr_code']}")
    #         print(f"  Location: ({station['latitude']}, {station['longitude']})")
    #         print(f"  Transport Type: {station['transport_type']}")
    #         print(f"  Station Type: {station['station_type']}")
    #         print("-" * 40)
    # else:
    #     print("No stations found matching your query.")
    
    matching_query = api.search_settlements(input("settlement name: "))
    #c39 rostov
    #c14 tver
    #c969 viborb

    # print(matching_query)
    # Example usage of between2stations (existing functionality)
    api.between2stations("c39", "c2", "2025-03-12")
