from datetime import datetime
from neo4j import GraphDatabase
from yapi import yAPI  
from neo4j import Session

DATETIME_FMT = "%Y-%m-%d %H:%M:%S"

class TransportGraph:
    def __init__(self, uri, user, password):
        """
        Initialize the Neo4j driver and session.
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.session = self.driver.session()
        self.api = yAPI()  # your existing Yandex Rasp API wrapper

    def close(self):
        """
        Close the Neo4j driver/session.
        """
        self.session.close()
        self.driver.close()

    # --------------------------------------------------------------------------
    # Helpers for creating or finding stations in Neo4j
    # --------------------------------------------------------------------------
    def create_station_node(self, station_code, title, lat, lon, transport_type, station_type):
        """
        MERGE a station node based on its unique Yandex code.
        If it already exists, just update properties (if needed).
        """
        query = """
        MERGE (s:Station {yandex_code: $station_code})
        ON CREATE SET
            s.title = $title,
            s.latitude = $lat,
            s.longitude = $lon,
            s.transport_type = $transport_type,
            s.station_type = $station_type
        ON MATCH SET
            s.title = $title,
            s.latitude = $lat,
            s.longitude = $lon,
            s.transport_type = $transport_type,
            s.station_type = $station_type
        RETURN s
        """
        params = {
            "station_code": station_code,
            "title": title,
            "lat": lat,
            "lon": lon,
            "transport_type": transport_type,
            "station_type": station_type
        }
        # print("creating node: ", params)
        self.session.run(query, **params)

    def create_walkable_edge(self, code1, code2, distance_km):
        """
        Create a 'WALKABLE' edge between two stations if it doesn't exist.
        Here, 'distance_km' is just an example property. 
        """
        query = """
        MATCH (s1:Station {yandex_code: $code1})
        MATCH (s2:Station {yandex_code: $code2})
        MERGE (s1)-[r:WALKABLE]->(s2)
        ON CREATE SET r.distance_km = $distance
        RETURN r
        """

        # print("creaing walk edge")
        self.session.run(query, code1=code1, code2=code2, distance=distance_km)

    def create_transport_edge(self, from_code, to_code, departure, arrival, thread_uid):
        """
        Create a 'TRANSPORT' edge for a specific trip from station A to station B.
        Example properties stored:
          - departure: date/time
          - arrival:   date/time
          - thread_uid: ID of the route (train, bus, etc.)
        """
        query = """
        MATCH (s1:Station {yandex_code: $code1})
        MATCH (s2:Station {yandex_code: $code2})
        MERGE (s1)-[r:TRANSPORT {
            thread_uid: $thread_uid, 
            departure_time: $departure_time,
            arrival_time: $arrival_time
        }]->(s2)
        RETURN r
        """
        params = {
            "code1": from_code,
            "code2": to_code,
            "thread_uid": thread_uid,
            "departure_time": departure,
            "arrival_time": arrival
        }

        print("creating tr edge: ", params)
        
        self.session.run(query, **params)
    
    def create_walkable_edges_bulk(self, edges):
        """
        Create multiple 'TRANSPORT' edges in bulk using APOC's periodic iterate.
        
        Each element in the `edges` list should be a dictionary containing:
          - from_code: station code for the origin station
          - to_code: station code for the destination station
          - departure: departure datetime (ISO 8601 string or datetime)
          - arrival: arrival datetime (ISO 8601 string or datetime)
          - thread_uid: the unique identifier for the transport thread
          
        Example:
          edges = [
              {
                  "from_code": "STN001",
                  "to_code": "STN002",
                  "departure": "2025-03-15T08:00:00",
                  "arrival": "2025-03-15T09:30:00",
                  "thread_uid": "TUID001"
              },
              {
                  "from_code": "STN003",
                  "to_code": "STN004",
                  "departure": "2025-03-16T10:00:00",
                  "arrival": "2025-03-16T11:45:00",
                  "thread_uid": "TUID002"
              }
          ]
        
        This function leverages APOC to process edges in batches.
        """
        query = """
        CALL apoc.periodic.iterate(
          "UNWIND $edges as edge RETURN edge",
          "MATCH (s1:Station {yandex_code: edge.from_code})
           MATCH (s2:Station {yandex_code: edge.to_code})
           MERGE (s1)-[r:WALKABLE {
             distance_km: edge.distance_km
           }]->(s2)",
          {batchSize: 100, params:{edges:$edges}, parallel: false}
        ) YIELD batches, total
        RETURN batches, total
        """
        print("Creating transport edges in bulk using APOC:", edges)
        
        print(self.session.run(query, edges=edges).consume().notifications)
    def create_transport_edges_bulk(self, edges):
        """
        Create multiple 'TRANSPORT' edges in bulk using APOC's periodic iterate.
        
        Each element in the `edges` list should be a dictionary containing:
          - from_code: station code for the origin station
          - to_code: station code for the destination station
          - departure: departure datetime (ISO 8601 string or datetime)
          - arrival: arrival datetime (ISO 8601 string or datetime)
          - thread_uid: the unique identifier for the transport thread
          
        Example:
          edges = [
              {
                  "from_code": "STN001",
                  "to_code": "STN002",
                  "departure": "2025-03-15T08:00:00",
                  "arrival": "2025-03-15T09:30:00",
                  "thread_uid": "TUID001"
              },
              {
                  "from_code": "STN003",
                  "to_code": "STN004",
                  "departure": "2025-03-16T10:00:00",
                  "arrival": "2025-03-16T11:45:00",
                  "thread_uid": "TUID002"
              }
          ]
        
        This function leverages APOC to process edges in batches.
        """
        query = """
        CALL apoc.periodic.iterate(
          "UNWIND $edges as edge RETURN edge",
          "MATCH (s1:Station {yandex_code: edge.from_code})
           MATCH (s2:Station {yandex_code: edge.to_code})
           MERGE (s1)-[r:TRANSPORT {
             thread_uid: edge.thread_uid,
             departure_time: edge.departure,
             arrival_time: edge.arrival
           }]->(s2)",
          {batchSize: 100, params:{edges:$edges}, parallel: false}
        ) YIELD batches, total
        RETURN batches, total
        """
        print(query)
        print("Creating transport edges in bulk using APOC:", edges)
        
        self.session.run(query, edges=edges)
    def parse_thread(self, thread_json):
        stops = thread_json.get("stops", [])
        uid = thread_json.get("uid","")
        parsed = []
        stations = []
        for i in range(len(stops[:-1])):
            _from = stops[i]
            _to = stops[i + 1]

            from_code = _from.get("station").get("codes",[]).get("yandex","UNK")
            to_code = _to.get("station").get("codes",[]).get("yandex","UNK")
            departure = datetime.strptime(_from.get("departure"), DATETIME_FMT)
            arrival = datetime.strptime(_to.get("arrival"), DATETIME_FMT)
            # self.create_transport_edge(from_code, to_code, departure.timestamp(), arrival.timestamp(), uid)
            parsed.append({"from_code": from_code, "to_code": to_code, "departure": departure.timestamp(), "arrival":arrival.timestamp(), "thread_uid": uid})
            stations.append(from_code)
            stations.append(to_code)



        return parsed, stations

    # --------------------------------------------------------------------------
    # Higher-level methods: searching or updating from Yandex Rasp
    # --------------------------------------------------------------------------
    def add_station_if_not_exists(self, station_dict):
        """
        Given one station's data from yAPI, create/update its node in Neo4j.
        station_dict might look like:
          {
            "title": "...",
            "yandex_code": "s9600370",
            "esr_code": "2004001",
            "latitude": 55.7558,
            "longitude": 37.6176,
            "transport_type": "train",
            "station_type": "train_station"
          }
        """
        yandex_code = station_dict["yandex_code"]
        title = station_dict.get("title", "")
        lat = station_dict.get("latitude", 0)
        lon = station_dict.get("longitude", 0)
        tr_type = station_dict.get("transport_type", "unknown")
        st_type = station_dict.get("station_type", "unknown")

        self.create_station_node(
            station_code=yandex_code,
            title=title,
            lat=lat,
            lon=lon,
            transport_type=tr_type,
            station_type=st_type
        )


    def add_stations_bulk(self, stations):
        """
        Given a list of station dictionaries from yAPI, create or update station nodes in Neo4j in bulk.
        
        Each dictionary should look like:
          {
              "title": "...",
              "yandex_code": "s9600370",
              "esr_code": "2004001",
              "latitude": 55.7558,
              "longitude": 37.6176,
              "transport_type": "train",
              "station_type": "train_station"
          }
          
        This function leverages APOC's periodic iterate to process the stations in batches.
        """
        query = """
        CALL apoc.periodic.iterate(
          "UNWIND $stations AS station RETURN station",
          "MERGE (s:Station {yandex_code: station.yandex_code})
           ON CREATE SET 
               s.title = station.title,
               s.esr_code = station.esr_code,
               s.latitude = station.latitude,
               s.longitude = station.longitude,
               s.transport_type = station.transport_type,
               s.station_type = station.station_type
           ON MATCH SET 
               s.title = station.title,
               s.esr_code = station.esr_code,
               s.latitude = station.latitude,
               s.longitude = station.longitude,
               s.transport_type = station.transport_type,
               s.station_type = station.station_type",
          {batchSize: 100, params: {stations: $stations}, parallel: false}
        ) YIELD batches, total
        RETURN batches, total
        """
        print(stations)
        print("Creating/updating station nodes in bulk.")
        self.session.run(query, stations=stations)
    def add_walkable_edges_in_same_settlement(self, settlement_query, distance_km=1.0):
        """
        Example: find a settlement by name (e.g. "Москва"), 
        get all stations in that settlement, and create
        bidirectional `WALKABLE` edges (or one-way, up to you).
        """
        settlement_matches = self.api.search_settlements(settlement_query)
        if not settlement_matches:
            print(f"No settlement found for query '{settlement_query}'")
            return

        for settle in settlement_matches:
            settle_code = settle["yandex_code"]
            # get all station codes in that settlement
            station_codes = self.api.get_settlement_station_codes(settle_code)

            # if we have more than one station in that settlement, 
            # create walkable edges for them (fully connected or pairwise).
            n = len(station_codes)
            for i in range(n):
                for j in range(i + 1, n):
                    code_a = station_codes[i]
                    code_b = station_codes[j]
                    self.create_walkable_edge(code_a, code_b, distance_km)
                    # Also create the reverse edge if you want symmetrical:
                    self.create_walkable_edge(code_b, code_a, distance_km)

    def ensure_transport_edge(self, station_a, station_b, date_str):
        """
        Check if there's any existing TRANSPORT edge in Neo4j from station_a to station_b
        on a given date. If none found, call the API (between2stations) and create them.
        """
        query_check = """
        MATCH (s1:Station {yandex_code:$code1})-[t:TRANSPORT]->(s2:Station {yandex_code:$code2})
        WHERE date(t.departure_time) = date($wanted_date)
        RETURN t
        """
        record = self.session.run(query_check, code1=station_a, code2=station_b, wanted_date=date_str).single()

        if record:
            print(f"Transport edge already exists for {station_a} -> {station_b} on {date_str}")
            return

        # If none found, fetch from the API
        trip_data = self.api.between2stations(station_a, station_b, date_str)
        # That returns a JSON with schedules/trips. The structure can vary, but typically:
        # {
        #   "search": {...},
        #   "segments": [
        #       {
        #           "thread": {"uid": "...", ...},
        #           "departure": "2025-03-12T15:00:00",
        #           "arrival": "2025-03-12T16:30:00",
        #           "from": {"code": "sXXXX"},
        #           "to":   {"code": "sYYYY"},
        #           ...
        #       },
        #       ...
        #   ]
        # }
        # We'll parse that, and create TRANSPORT edges.

        segments = trip_data.get("segments", [])
        if not segments:
            print(f"No direct segments found in API for {station_a} -> {station_b} on {date_str}")
            return

        for seg in segments:
            thread_uid = seg.get("thread", {}).get("uid", "unknown")
            departure = seg.get("departure", None)  # e.g. '2025-03-12T15:00:00'
            arrival = seg.get("arrival", None)

            from_station_code = seg.get("from", {}).get("code", "")
            to_station_code = seg.get("to", {}).get("code", "")

            # Make sure the station nodes exist in Neo4j:
            # (The code below just uses minimal info to create or update stations;
            #  or you can call an existing function to fully populate them.)
            self.create_station_node(station_code=from_station_code, 
                                     title=from_station_code,
                                     lat=0, lon=0,
                                     transport_type="unknown", 
                                     station_type="unknown")
            self.create_station_node(station_code=to_station_code,
                                     title=to_station_code,
                                     lat=0, lon=0,
                                     transport_type="unknown",
                                     station_type="unknown")

            # Now create the transport edge
            self.create_transport_edge(from_code=from_station_code,
                                       to_code=to_station_code,
                                       departure=departure,
                                       arrival=arrival,
                                        thread_uid=thread_uid)
    def remove_transport_edge(self, station_code1, station_code2, thread_uid):
        query = """
        MATCH (a: Station{yandex_code:$station_code1})-[t:TRANSPORT {thread_uid:$thread_uid}]->(b: Station{yandex_code2})
        DELETE t
        """
        self.session.run(query, station_code1 = station_code1, station_code2 = station_code2, thread_uid = thread_uid)
        return 

    def populate_transport_edges(self, json_result_schedule):

        #
        if not json_result_schedule:
            return
        thread_list_unparsed = json_result_schedule.get("schedule", {})
        thread_list = []
        for entry in thread_list_unparsed:
            thread_list.append({"uid": entry.get("thread",{}).get("uid","UNK")})
        results = self.api.bulk_thread_stops(thread_list)
        parsed_threads = []
        parsed_stations = []

        for thread_json in results:
            if thread_json:
                _parsed_threads, _parsed_stations = self.parse_thread(thread_json)
                parsed_threads += _parsed_threads
                parsed_stations += _parsed_stations
        #after SOME schedule was returned it is valid to re-request the neighbors
        parsed_station_info = self.api.fetch_station_info(set(parsed_stations))
        self.add_stations_bulk(parsed_station_info)
        self.create_transport_edges_bulk(parsed_threads)

    def populate_walkable_edges(self, source_station_infos):
        edges = []
        for source_station_info in source_station_infos:
            station_infos, distances = self.api.walkable_stations(source_station_info)
            print(source_station_info, station_infos, distances)
            if station_infos:
                self.add_stations_bulk(station_infos)
                parsed_walked_infos = [
                    {"distance_km": _distance, "from_code":source_station_info.get("yandex_code"),
                     "to_code":_station["yandex_code"]}
                    for _station, _distance in zip(station_infos, distances)]
                edges += parsed_walked_infos
        self.create_walkable_edges_bulk(edges)

    def get_neighbors(self, station_code, date = None):
        """
        Query Neo4j for all edges from the given station. 
        Return a list of neighbor info in the form:
          [
            (nbr_code, cost_value, mode, route_info, dist_km, travel_time_sec),
            ...
          ]
        
        Where:
          - mode is either "walk" or "transport"
          - route_info is a dict with departure_time, arrival_time, etc. (for transport edges)
          - dist_km is how far the stations are if walking (0 for transport edges)
          - travel_time_sec is the approximate number of seconds from station->neighbor
        """
        query = """
        MATCH (s:Station {yandex_code:$station_code})-[r]->(nbr:Station)
        RETURN type(r) as rel_type, r, nbr.yandex_code as neighbor_code
        """
        results = self.session.run(query, station_code=station_code)
        
        neighbors = []
        transport_rel_count = 0
        walkable_rel_count = 0
        for record in results:
            rel_type = record["rel_type"]
            rel_props = record["r"]  # relationship properties
            nbr_code = record["neighbor_code"]

            if rel_type == "WALKABLE":
                walkable_rel_count += 1
                # Example assumption: we walk about 5 km/h => ~ 12 min/km = 720 sec/km
                dist_km = rel_props.get("distance_km", 1.0)
                walk_speed_seconds_per_km = 720
                travel_time_sec = dist_km * walk_speed_seconds_per_km
                
                # route_info can be empty or contain anything relevant
                route_info = {}
                
                neighbors.append(
                    (
                        nbr_code, 
                        dist_km,            # cost_value (arbitrary; can be distance)
                        "walk",             # mode
                        route_info, 
                        dist_km,            # dist_km
                        travel_time_sec     # travel_time_sec
                    )
                )

            elif rel_type == "TRANSPORT":
                transport_rel_count += 1
                # For transport, we store departure and arrival times in route_info
                departure_str = rel_props.get("departure_time")
                arrival_str   = rel_props.get("arrival_time")
                thread_uid    = rel_props.get("thread_uid", "")

                # Convert strings to Python datetime objects
                dep_dt = None
                arr_dt = None
                if departure_str and arrival_str:
                    # If your datetime strings are ISO-8601-like ("2025-03-12T16:30:00"), 
                    # you can parse them with fromisoformat:
                    try:
                        dep_dt = datetime.fromtimestamp(departure_str)
                        arr_dt = datetime.fromtimestamp(arrival_str)
                    except ValueError:
                        pass

                route_info = {
                    "thread_uid": thread_uid,
                    "departure_time": dep_dt,
                    "arrival_time": arr_dt
                }
                
                # We could compute travel_time_sec from departure->arrival
                travel_time_sec = 0
                if dep_dt and arr_dt:
                    travel_time_sec = int((arr_dt - dep_dt).total_seconds())

                # cost_value can be “1” or anything you want (the old code just had a placeholder).
                # dist_km can be “0” for a transport edge (or skip).
                neighbors.append(
                    (
                        nbr_code,
                        1,                  # cost_value (arbitrary for transport)
                        "transport",        # mode
                        route_info,
                        0,                  # dist_km
                        travel_time_sec
                    )
                )
        # added_new_flag = False
        #
        #
        #
        #
        #
        #
        # if added_new_flag:
        #
        #     return self.get_neighbors(station_code, date)

        return neighbors
