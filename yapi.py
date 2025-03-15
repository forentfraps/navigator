import concurrent.futures
import requests
import json
import os
import datetime
import math


def are_stations_within_distance(lat1, lon1, lat2, lon2, threshold_km):
    """
    Determine if two stations are within the threshold distance (in km) based on their latitude and longitude.
    
    Parameters:
        lat1 (float): Latitude of the first station.
        lon1 (float): Longitude of the first station.
        lat2 (float): Latitude of the second station.
        lon2 (float): Longitude of the second station.
        threshold_km (float): Distance threshold in kilometers.
    
    Returns:
        tuple: (is_within, distance) where is_within is True if the distance is less than or equal to threshold_km,
               and distance is the computed distance in kilometers.
    """
    # Convert degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Compute differences
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    # Haversine formula
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    
    # Earth's radius in kilometers (approx.)
    earth_radius_km = 6371.0
    distance = earth_radius_km * c
    
    return distance <= threshold_km, distance

class yAPI:
    def __init__(self, cache_file="resp.json", miss_cache_file="station_schedule_misses.json"):
        self.url = "https://api.rasp.yandex.net/v3.0"
        # Your API key
        self.apikey = "apikey=70aafa03-0fdb-4aed-a88e-94972422528d"
        self.format = "format=json"
        self.lang = "lang=ru_RU"
        self.cache_file = cache_file
        self.miss_cache_file = miss_cache_file
        self._station_schedule_miss = set()
        self._load_miss_cache()
    
    def get(self, endpoint, extraparams=""):
        url = f"{self.url}/{endpoint}/?{self.apikey}&{self.format}&{self.lang}{extraparams}"
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content

    def _load_miss_cache(self):
        if os.path.exists(self.miss_cache_file):
            try:
                with open(self.miss_cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._station_schedule_miss = set(data)
            except:
                pass

    def _save_miss_cache(self):
        try:
            with open(self.miss_cache_file, "w", encoding="utf-8") as f:
                json.dump(list(self._station_schedule_miss), f, ensure_ascii=False, indent=2)
        except:
            pass

    def stations_list(self):
        """ Downloads the full stations list from the API and saves to cache_file """
        data = self.get("stations_list")
        with open(self.cache_file, "wb") as f:
            f.write(data)

    def get_stations_data(self, force_download=False):
        """ Return parsed JSON data for stations. """
        if force_download or not os.path.exists(self.cache_file):
            print("Downloading stations data...")
            self.stations_list()
        with open(self.cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data

    def search_stations(self, query, force_download=False):
        data = self.get_stations_data(force_download=force_download)
        results = []
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    for station in settlement.get("stations", []):
                        if query.lower() in station.get("title", "").lower():
                            results.append({
                                "title": station.get("title"),
                                "yandex_code": station.get("codes", {}).get("yandex_code"),
                                "esr_code": station.get("codes", {}).get("esr_code"),
                                "latitude": station.get("latitude"),
                                "longitude": station.get("longitude"),
                                "transport_type": station.get("transport_type"),
                                "station_type": station.get("station_type")
                            })
        return results
    def populate_neo4j(self, transport_graph, force_download = False):
        data = self.get_stations_data(force_download=force_download)
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    for station in settlement.get("stations", []):
                        station_info = {
                            "title": station.get("title"),
                            "yandex_code": station.get("codes", {}).get("yandex_code"),
                            "esr_code": station.get("codes", {}).get("esr_code"),
                            "latitude": station.get("latitude"),
                            "longitude": station.get("longitude"),
                            "transport_type": station.get("transport_type"),
                            "station_type": station.get("station_type")
                        }
                        transport_graph.add_station_if_not_exists(station_info)
        return
    def fetch_station_info(self, station_ids, force_download = False):
        data = self.get_stations_data(force_download=force_download)
        station_infos = []
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    for station in settlement.get("stations", []):

                        code = station.get("codes", {}).get("yandex_code")
                        if code in station_ids:
    
                            station_info = {
                                "title": station.get("title"),
                                "yandex_code": station.get("codes", {}).get("yandex_code"),
                                "esr_code": station.get("codes", {}).get("esr_code"),
                                "latitude": station.get("latitude"),
                                "longitude": station.get("longitude"),
                                "transport_type": station.get("transport_type"),
                                "station_type": station.get("station_type")
                            }
                            station_infos.append(station_info)

        
        return station_infos



    def search_settlements(self, query, force_download=False):
        data = self.get_stations_data(force_download=force_download)
        results = []
        for country in data.get("countries", []):
            country_title = country.get("title", "")
            for region in country.get("regions", []):
                region_title = region.get("title", "")
                for settlement in region.get("settlements", []):
                    if query.lower() in settlement.get("title", "").lower():
                        results.append({
                            "title": settlement.get("title"),
                            "yandex_code": settlement.get("codes", {}).get("yandex_code"),
                            "country": country_title,
                            "region": region_title
                        })
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
        Retrieves schedule (list of trips) for the specified station.
        Loops up to `max_day_lookahead` if no explicit date is given or if empty results.
        Returns the final JSON data or None.
        """
        if station in self._station_schedule_miss:
            print(f"[station_schedule] Station {station} is in miss cache, returning None.")
            return None

        if type(date)!= datetime.datetime:
            date = datetime.datetime.fromtimestamp(date).strftime("%Y-%m-%d")
        if not date:
            date = datetime.date.today().strftime("%Y-%m-%d")


        print(f"[station_schedule] Attempting date={date} ...")

        single_day_data = self._fetch_station_schedule_once(
            station=station,
            date=date,
            direction=direction,
            transport_types=transport_types,
            offset=offset,
            limit=limit
        )

        return single_day_data

    def _fetch_station_schedule_once(self, station, date, direction, transport_types, offset, limit):
        extraparams = f"&station={station}&offset={offset}&limit={limit}&date={date}"
        if direction:
            extraparams += f"&direction={direction}"
        if transport_types:
            extraparams += f"&transport_types={transport_types}"

        try:
            data_bytes = self.get("schedule", extraparams)
        except requests.HTTPError:
            self._station_schedule_miss.add(station)
            self._save_miss_cache()
            return None
        data = json.loads(data_bytes)
        if not data.get("schedule"):
            return None

        pagination = data.get("pagination", {})
        total = pagination.get("total", 0)
        current_limit = pagination.get("limit", limit)

        if total > current_limit:
            # re‐fetch with bigger limit
            new_extraparams = f"&station={station}&offset=0&limit={total}&date={date}"
            if direction:
                new_extraparams += f"&direction={direction}"
            if transport_types:
                new_extraparams += f"&transport_types={transport_types}"
            try:
                second_data_bytes = self.get("schedule", new_extraparams)
                return json.loads(second_data_bytes)
            except:
                return data
        return data

    def between2stations(self, code1, code2, date):
        """
        Get route info between two stations. Return the JSON directly.
        """
        data_bytes = self.get("search", f"&from={code1}&to={code2}&date={date}")
        return json.loads(data_bytes)

    def thread_stops(self, uid, date=None, from_code=None, to_code=None, show_systems="all"):
        """
        Fetch the stops for a given thread (train/bus, etc.).
        """
        print(f"thread stops : {uid} date {date}")
        extraparams = f"&uid={uid}&show_systems={show_systems}"
        if date:
            extraparams += f"&date={date}"
        if from_code:
            extraparams += f"&from={from_code}"
        if to_code:
            extraparams += f"&to={to_code}"

        data_bytes = self.get("thread", extraparams)
        return json.loads(data_bytes)
     
    def bulk_thread_stops(self, queries, max_workers=100):
        """
        Query multiple thread stops concurrently using multithreading.
        
        Parameters:
            queries (list): A list of dictionaries, each containing parameters for thread_stops.
                            For example:
                            [
                                {'uid': 'uid1', 'date': '2025-03-15', 'from_code': 'AAA', 'to_code': 'BBB', 'show_systems': 'all'},
                                {'uid': 'uid2', 'date': '2025-03-16', 'from_code': 'CCC', 'to_code': 'DDD', 'show_systems': 'all'},
                                ...
                            ]
            max_workers (int): The maximum number of threads to run concurrently. Defaults to 10.
        
        Returns:
            list: A list containing the result of each thread_stops call.
        """
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Map each query to a future
            future_to_query = {executor.submit(self.thread_stops, **query): query for query in queries}
            for future in concurrent.futures.as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    # Handle exceptions if any individual thread_stops call fails.
                    print(f"Query {query} generated an exception: {exc}")
                    results.append(None)
        return results
    
    def walkable_stations(self,initial_station_info,threshold_km = 1.0, force_download = False):
        data = self.get_stations_data(force_download=force_download)
        walkable_stations = []
        distances = []
        compare_lon = initial_station_info["longitude"]
        compare_lat= initial_station_info["latitude"]
        if not compare_lon or not compare_lat:
            return ([], [])
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    for station in settlement.get("stations", []):

                        lat= station.get("latitude")
                        lon = station.get("longitude")
                        if (not lat) or (not lon) or (initial_station_info["yandex_code"] == station.get("codes", {}).get("yandex_code")):
                            continue
                        viable, distance = are_stations_within_distance(compare_lat, compare_lon, lat, lon, threshold_km)
                        if viable:
    
                            station_info = {
                                "title": station.get("title"),
                                "yandex_code": station.get("codes", {}).get("yandex_code"),
                                "esr_code": station.get("codes", {}).get("esr_code"),
                                "latitude": station.get("latitude"),
                                "longitude": station.get("longitude"),
                                "transport_type": station.get("transport_type"),
                                "station_type": station.get("station_type")
                            }
                            
                            distances.append(distance)
                            walkable_stations.append(station_info)
        return walkable_stations, distances
     
    def get_settlement_station_codes(self, settlement_yandex_code, force_download=False):
        """
        Return a list of station yandex_code for all stations in a given settlement.
        """
        data = self.get_stations_data(force_download=force_download)
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    if settlement.get("codes", {}).get("yandex_code") == settlement_yandex_code:
                        return [
                            st.get("codes", {}).get("yandex_code")
                            for st in settlement.get("stations", [])
                            if st.get("codes", {}).get("yandex_code")
                        ]
        return []

