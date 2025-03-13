# lazygraph.py

import os
import math
import json
from datetime import datetime

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Return great-circle distance in km.
    """
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = rlon2 - rlon1
    dlat = rlat2 - rlat1
    a = math.sin(dlat/2)**2 + math.cos(rlat1)*math.cos(rlat2)*math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return 6371.0 * c


class LazyRouteGraph:
    def __init__(
        self,
        api,
        stations_file="resp.json",
        routes_folder="routes",
        walk_distance_km=1.0,
        debug=False,
        cost_mode="distance"
    ):
        """
        :param cost_mode: 'distance' or 'time' to choose how edges are weighted.
        """
        self.api = api
        self.stations_file = stations_file
        self.routes_folder = routes_folder
        self.walk_distance_km = walk_distance_km
        self.debug = debug
        self.cost_mode = cost_mode  # 'distance' or 'time'

        self._stations = {}
        # We'll store adjacency as a dict:
        #   station_code -> [ (nbr_code, cost, mode, route_info, dist_km, travel_time_sec), ... ]
        self._adj_cache = {}
        self._thread_used = set()

        self._load_stations_data()

    def _dprint(self, msg):
        if self.debug:
            print("[DEBUG]", msg)

    def _load_stations_data(self):
        """Load station definitions from resp.json (or similar)."""
        if not os.path.exists(self.stations_file):
            raise FileNotFoundError(f"Stations file not found: {self.stations_file}")

        with open(self.stations_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        count_loaded = 0
        for country in data.get("countries", []):
            for region in country.get("regions", []):
                for settlement in region.get("settlements", []):
                    for station in settlement.get("stations", []):
                        code = station.get("codes", {}).get("yandex_code")
                        if not code:
                            continue
                        raw_lat = station.get("latitude")
                        raw_lon = station.get("longitude")
                        has_coords = False
                        lat = lon = None

                        if raw_lat != "" and raw_lon != "" and raw_lat is not None and raw_lon is not None:
                            try:
                                lat = float(raw_lat)
                                lon = float(raw_lon)
                                has_coords = True
                            except ValueError:
                                pass

                        title = station.get("title", "Unnamed Station")
                        self._stations[code] = {
                            "title": title,
                            "lat": lat,
                            "lon": lon,
                            "has_coords": has_coords
                        }
                        count_loaded += 1
        self._dprint(f"Loaded {count_loaded} stations from {self.stations_file}.")

    def station_distance(self, code_a, code_b):
        """
        Returns Haversine distance in km or inf if missing coords.
        """
        s1 = self._stations.get(code_a)
        s2 = self._stations.get(code_b)
        if not s1 or not s2:
            return float('inf')
        if not s1["has_coords"] or not s2["has_coords"]:
            return float('inf')
        return haversine_distance(s1["lat"], s1["lon"], s2["lat"], s2["lon"])

    def get_neighbors(self, station_code):
        """
        Returns adjacency list: [ (nbr_code, cost, mode, route_info, dist_km, travel_time_sec), ... ]
        where cost is either distance or time, depending on self.cost_mode.
        """
        if station_code in self._adj_cache:
            return self._adj_cache[station_code]

        adjacency_list = []
        adjacency_list.extend(self._build_transport_adjacency(station_code))
        adjacency_list.extend(self._build_walk_adjacency(station_code))

        self._adj_cache[station_code] = adjacency_list
        return adjacency_list

    def _build_transport_adjacency(self, station_code):
        """Build edges from routes/<station_code>.json (schedule data)."""
        results = []
        schedule_data = self._load_station_schedule(station_code)
        if not schedule_data:
            return results

        sched_list = schedule_data.get("schedule", [])
        self._dprint(f"{station_code} schedule has {len(sched_list)} departures.")

        for item in sched_list:
            thread = item.get("thread", {})
            uid = thread.get("uid")
            transport_type = thread.get("transport_type", "unknown")
            route_info = {
                "title": thread.get("title", ""),
                "uid": uid,
                "departure_time": item.get("departure"),
                "arrival_time": item.get("arrival"),
            }

            if not uid:
                continue

            if uid not in self._thread_used:
                self._thread_used.add(uid)
                thread_data = self.api.thread_stops(uid)
                stops = thread_data.get("stops", [])
                self._dprint(f"Thread {uid} has {len(stops)} stops. Building edges...")

                for i in range(len(stops) - 1):
                    stA = stops[i].get("station", {})
                    stB = stops[i+1].get("station", {})
                    codeA = stA.get("codes", {}).get("yandex")
                    codeB = stB.get("codes", {}).get("yandex")
                    if not codeA or not codeB:
                        continue

                    dist_km = self.station_distance(codeA, codeB)
                    travel_time_sec = self._compute_travel_time_sec(stops[i], stops[i+1])

                    # final "cost" depends on cost_mode
                    cost_value = dist_km if (self.cost_mode=="distance") else travel_time_sec

                    edge = (
                        codeB,
                        cost_value,      # the edge cost (time or dist)
                        transport_type,
                        route_info,
                        dist_km,
                        travel_time_sec
                    )
                    self._add_edge_to_adjacency(codeA, edge)

        # Return adjacency if it exists
        return self._adj_cache.get(station_code, [])

    def _compute_travel_time_sec(self, stopA, stopB):
        """
        Use departure from A & arrival at B, or durations, to compute travel time in seconds.
        If parsing fails, return some fallback (0 or large).
        """
        # Direct from 'duration' fields if available
        durA = stopA.get("duration")
        durB = stopB.get("duration")
        if durA is not None and durB is not None:
            try:
                return float(durB) - float(durA) if float(durB) > float(durA) else 0
            except:
                pass

        # Fallback: parse departure from A vs arrival at B
        depA = stopA.get("departure")  # "2025-03-13 11:50:00"
        arrB = stopB.get("arrival")
        if depA and arrB:
            from datetime import datetime
            fmt = "%Y-%m-%d %H:%M:%S"
            try:
                tA = datetime.strptime(depA, fmt)
                tB = datetime.strptime(arrB, fmt)
                diff_sec = (tB - tA).total_seconds()
                return diff_sec if diff_sec >= 0 else 0
            except:
                return 0
        return 0

    def _add_edge_to_adjacency(self, from_code, edge):
        """Add an edge to adjacency cache."""
        if from_code not in self._adj_cache:
            self._adj_cache[from_code] = []
        self._adj_cache[from_code].append(edge)

    def _build_walk_adjacency(self, station_code):
        """Create 'walk' edges for stations within self.walk_distance_km."""
        results = []
        st_info = self._stations.get(station_code)
        if not st_info or not st_info["has_coords"]:
            return results

        lat1, lon1 = st_info["lat"], st_info["lon"]
        lat_offset = self.walk_distance_km / 111.0
        cos_factor = math.cos(math.radians(lat1))
        if abs(cos_factor) < 1e-8:
            cos_factor = 1e-8
        lon_offset = self.walk_distance_km / (111.0 * cos_factor)

        min_lat, max_lat = lat1 - lat_offset, lat1 + lat_offset
        min_lon, max_lon = lon1 - lon_offset, lon1 + lon_offset

        for other_code, oinfo in self._stations.items():
            if other_code == station_code:
                continue
            if not oinfo["has_coords"]:
                continue
            lat2, lon2 = oinfo["lat"], oinfo["lon"]

            # bounding box
            if min_lat <= lat2 <= max_lat and min_lon <= lon2 <= max_lon:
                dist = haversine_distance(lat1, lon1, lat2, lon2)
                if dist <= self.walk_distance_km:
                    # if cost_mode=time, pick a walking speed, e.g. 5km/h => 12 min per km
                    if self.cost_mode == "distance":
                        cost_value = dist
                    else:
                        # time mode => dist (km) * 12 min => dist * 12 * 60 sec
                        cost_value = dist * 12 * 60

                    edge = (
                        other_code,
                        cost_value,
                        "walk",
                        {},
                        dist,
                        cost_value  # store same in travel_time_sec if you want
                    )
                    results.append(edge)
        self._dprint(f"[walk] {station_code} has {len(results)} walk neighbors.")
        return results

    def _load_station_schedule(self, station_code):
        """
        Checks routes/<station_code>.json; if missing, calls API to get station_schedule(...) 
        and saves it. Then returns the loaded JSON.
        """
        os.makedirs(self.routes_folder, exist_ok=True)
        path = os.path.join(self.routes_folder, f"{station_code}.json")

        if not os.path.exists(path):
            self._dprint(f"No schedule file for {station_code}, downloading from API...")

            try:
                data = self.api.station_schedule(station_code)
                if data:
                    with open(path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    return data
                else:
                    self._dprint(f"Warning: no data from station_schedule({station_code})")
                    return None
            except:
                self._dprint(f"Warning: no data from station_schedule({station_code})")
                return None

        else:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self._dprint(f"Error reading {path}: {e}")
                return None

