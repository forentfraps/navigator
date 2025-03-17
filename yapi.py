#yapi.py

import concurrent.futures
import requests
import json
import os
import datetime
import math
import logging
import csv
from time import sleep

def are_stations_within_distance(lat1, lon1, lat2, lon2, threshold_km):
    """
    Determine if two stations are within the threshold distance (in km) 
    using the Haversine formula.
    Returns (bool, distance_km).
    """
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    
    earth_radius_km = 6371.0
    distance = earth_radius_km * c
    
    return distance <= threshold_km, distance

def generate_relationship_csv(station_infos, threshold_km, output_csv):
    """
    Generate a CSV file with pairs of stations that are within threshold_km distance.
    """
    with open(output_csv, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["start_yandex_code", "end_yandex_code", "distance_km"])
        
        n = len(station_infos)
        for i in range(n):
            station_a = station_infos[i]
            lat1 = station_a["latitude"]
            lon1 = station_a["longitude"]
            if (not lat1) or (not lon1):
                continue
            code_a = station_a["yandex_code"]
            
            for j in range(i + 1, n):
                station_b = station_infos[j]
                lat2 = station_b["latitude"]
                lon2 = station_b["longitude"]
                if (not lat2) or (not lon2):
                    continue
                code_b = station_b["yandex_code"]
                
                it_is, distance = are_stations_within_distance(lat1, lon1, lat2, lon2, threshold_km)
                if it_is:
                    writer.writerow([code_a, code_b, distance])

class yAPI:
    def __init__(self, cache_file="resp.json", miss_cache_file="station_schedule_misses.json"):
        self.url = "https://api.rasp.yandex.net/v3.0"
        self.apikey = "apikey=a8661682-f755-41cb-973f-e48ff8aa09d4"
        self.format = "format=json"
        self.lang = "lang=ru_RU"
        self.cache_file = cache_file
        self.miss_cache_file = miss_cache_file
        self._station_schedule_miss = set()
        self._load_miss_cache()
    
    def get(self, endpoint, extraparams=""):
        url = f"{self.url}/{endpoint}/?{self.apikey}&{self.format}&{self.lang}{extraparams}"
        resp = requests.get(url, timeout = 5)
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
        """Downloads the full stations list from the API and saves to self.cache_file."""
        data = self.get("stations_list")
        with open(self.cache_file, "wb") as f:
            f.write(data)

    def get_stations_data(self, force_download=False):
        """Return parsed JSON data for stations."""
        if force_download or not os.path.exists(self.cache_file):
            logging.info("Downloading stations data from Yandex Rasp...")
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

    def populate_neo4j(self, transport_graph, force_download=False):
        """
        Read all station data and insert them into Neo4j (nodes). 
        This was used once and everyone included regretted it, it should be done
        via LOAD CSV and with said csvs, but mistakes were made...
        """
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

    def fetch_station_info(self, station_ids, force_download=False):
        """
        Return station info dicts for the given station_ids. If station_ids is None, return all stations.
        """
        data = self.get_stations_data(force_download=force_download)
        station_infos = []
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    for station in settlement.get("stations", []):
                        code = station.get("codes", {}).get("yandex_code")
                        if station_ids is None or code in station_ids:
                            station_info = {
                                "title": station.get("title"),
                                "yandex_code": code,
                                "esr_code": station.get("codes", {}).get("esr_code"),
                                "latitude": station.get("latitude"),
                                "longitude": station.get("longitude"),
                                "transport_type": station.get("transport_type"),
                                "station_type": station.get("station_type")
                            }
                            station_infos.append(station_info)
        return station_infos

    def search_settlements(self, query, force_download=False):
        """
        Return settlement info that matches the query substring in the settlement title.
        """
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
     
    def bulk_station_schedule(self, queries, max_workers=20):
        """
        Query multiple station_schedule calls concurrently.
        queries = [ {'station': '...', 'date': '2025-03-15', ...}, ... ]
        """
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_query = {
                executor.submit(self.station_schedule, **query): query 
                for query in queries
            }
            for future in concurrent.futures.as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    logging.info(f"Query {query} generated an exception: {exc}")
                    results.append(None)
        return results

    def station_schedule_2days(
        self,
        station,
        dt=None,
        event="departure",
        direction=None,
        transport_types=None,
        offset=0,
        limit=100
    ):
        """
        Fetch station_schedule from the Yandex Rasp API for 2 consecutive days,
        then merge the results. This helps if your travel time might extend
        past midnight, so you can see the next day’s schedule.
        """
        if not dt:
            dt = datetime.datetime.now()
     
        date_str_1 = dt.strftime("%Y-%m-%d")
        dt_next = dt + datetime.timedelta(days=1)
        date_str_2 = dt_next.strftime("%Y-%m-%d")
     
        result_day1 = self.station_schedule(
            station=station,
            date=date_str_1,
            event=event,
            direction=direction,
            transport_types=transport_types,
            offset=offset,
            limit=limit
        )
        result_day2 = self.station_schedule(
            station=station,
            date=date_str_2,
            event=event,
            direction=direction,
            transport_types=transport_types,
            offset=offset,
            limit=limit
        )
     
        merged_result = {}
        if not result_day1:
            result_day1 = {}
        if not result_day2:
            result_day2 = {}
     
        merged_result.update(result_day1)
     
        merged_result["pagination"] = {
            "total": (
                (result_day1.get("pagination") or {}).get("total", 0) +
                (result_day2.get("pagination") or {}).get("total", 0)
            ),
            "limit": limit,
            "offset": 0,
        }
        schedule_1 = result_day1.get("schedule", [])
        schedule_2 = result_day2.get("schedule", [])
        merged_result["schedule"] = schedule_1 + schedule_2
     
        return merged_result

    def station_schedule(
        self,
        station,
        date=None,
        direction=None,
        transport_types=None,
        offset=0,
        limit=100,
        event="departure",
    ):
        """
        Retrieves schedule (list of trips) for the specified station.
        If event='arrival', then we get all trips arriving at station.
        """
        if station in self._station_schedule_miss:
            logging.info(f"[station_schedule] Station {station} in miss cache, returning None.")
            return None
     
        if isinstance(date, datetime.datetime):
            date = date.strftime("%Y-%m-%d")
        if not date:
            extraparams = f"&station={station}&offset={offset}&limit={limit}&event={event}"
        else:
            extraparams = f"&station={station}&offset={offset}&limit={limit}&date={date}&event={event}"
        logging.info(f"[QS] {station}")
     
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
        if data.get("error"):
            self._station_schedule_miss.add(station)
            self._save_miss_cache()
            return None

        if not data.get("schedule"):
            return None

        # Possibly handle pagination logic:
        pagination = data.get("pagination", {})
        total = pagination.get("total", 0)
        current_limit = pagination.get("limit", limit)
        if total > current_limit:
            try:
                second_data_bytes = self.get(
                    "schedule",
                    f"&station={station}&offset=0&limit={total}&date={date}&event={event}"
                    + (f"&direction={direction}" if direction else "")
                    + (f"&transport_types={transport_types}" if transport_types else "")
                )
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
        Fetch the stops for a given thread (e.g., train, bus).
        """
        logging.info(f"[QT] {uid}")
        extraparams = f"&uid={uid}&show_systems={show_systems}"
        if date:
            extraparams += f"&date={date}"
        if from_code:
            extraparams += f"&from={from_code}"
        if to_code:
            extraparams += f"&to={to_code}"

        data_bytes = self.get("thread", extraparams)
        try:
            data = json.loads(data_bytes)
        except:
            return None
        return data
     
    def bulk_thread_stops(self, queries, max_workers=5):
        """
        Query multiple thread_stops concurrently.
        queries = [ {'uid':'...', 'date':'...', 'from_code':'...', 'to_code':'...', ...}, ...]
        """
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_query = {
                executor.submit(self.thread_stops, **query): query for query in queries
            }
            for future in concurrent.futures.as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    result = future.result(timeout=5)
                    results.append(result)
                except Exception as exc:
                    logging.info(f"Query {query} generated an exception: {exc}")
                    sleep(10)

        return results
    
    def walkable_stations(self, initial_station_info, threshold_km=1.0, force_download=False):
        """
        Return stations within threshold_km of `initial_station_info`.
        Returns ([list_of_stations], [distances_corresponding]).
        """
        data = self.get_stations_data(force_download=force_download)
        walkable_stations = []
        distances = []
        compare_lon = initial_station_info["longitude"]
        compare_lat = initial_station_info["latitude"]
        if not compare_lon or not compare_lat:
            return ([], [])

        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    for station in settlement.get("stations", []):
                        lat = station.get("latitude")
                        lon = station.get("longitude")
                        if not lat or not lon:
                            continue
                        if initial_station_info["yandex_code"] == station.get("codes", {}).get("yandex_code"):
                            continue
                        viable, distance = are_stations_within_distance(
                            compare_lat, compare_lon,
                            lat, lon,
                            threshold_km
                        )
                        if viable:
                            station_info = {
                                "title": station.get("title"),
                                "yandex_code": station.get("codes", {}).get("yandex_code"),
                                "esr_code": station.get("codes", {}).get("esr_code"),
                                "latitude": lat,
                                "longitude": lon,
                                "transport_type": station.get("transport_type"),
                                "station_type": station.get("station_type")
                            }
                            walkable_stations.append(station_info)
                            distances.append(distance)
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

