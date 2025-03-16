#lazygraph.py

from datetime import datetime
from neo4j import GraphDatabase
import logging
from yapi import yAPI

DATETIME_FMT = "%Y-%m-%d %H:%M:%S"

class TransportGraph:
    WALK_SPEED_SECONDS_PER_KM = 720  # e.g. ~5 km/h => 12 min per km => 720 sec/km

    def __init__(self, uri, user, password):
        """
        Initialize the Neo4j driver and session.
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.session = self.driver.session()
        self.api = yAPI()

    def close(self):
        """Close the Neo4j driver/session."""
        self.session.close()
        self.driver.close()

    # --------------------------------------------------------------------------
    # Station creation / updating
    # --------------------------------------------------------------------------
    def create_station_node(self, station_code, title, lat, lon, transport_type, station_type):
        """
        MERGE a station node by its unique Yandex code.
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
        """
        params = {
            "station_code": station_code,
            "title": title,
            "lat": lat,
            "lon": lon,
            "transport_type": transport_type,
            "station_type": station_type
        }
        self.session.run(query, **params)

    def add_station_if_not_exists(self, station_dict):
        """
        Create or update a single station node (from a station_dict).
        """
        yandex_code = station_dict["yandex_code"]
        if not yandex_code:
            return
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
        Bulk upsert station nodes using APOC.
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
          {batchSize: 100, params: {stations: $stations}, parallel: true}
        )
        YIELD batches, total
        RETURN batches, total
        """
        self.session.run(query, stations=stations).consume()

    # --------------------------------------------------------------------------
    # Edges: WALKABLE
    # --------------------------------------------------------------------------
    def create_walkable_edge(self, code1, code2, distance_km):
        """
        Create a 'WALKABLE' edge between two stations if it doesn't exist.
        """
        query = """
        MATCH (s1:Station {yandex_code: $code1})
        MATCH (s2:Station {yandex_code: $code2})
        MERGE (s1)-[r:WALKABLE]->(s2)
        ON CREATE SET r.distance_km = $distance
        """
        self.session.run(query, code1=code1, code2=code2, distance=distance_km)

    def create_walkable_edges_bulk(self, edges):
        """
        Create multiple 'WALKABLE' edges in bulk using APOC.
        edges = [
          {"distance_km":..., "from_code":..., "to_code":...},
          ...
        ]
        """
        query = """
        CALL apoc.periodic.iterate(
          "UNWIND $edges as edge RETURN edge",
          "MATCH (s1:Station {yandex_code: edge.from_code})
           MATCH (s2:Station {yandex_code: edge.to_code})
           MERGE (s1)-[r:WALKABLE { distance_km: edge.distance_km }]->(s2)",
          {batchSize: 100, params:{edges:$edges}, parallel: false}
        ) YIELD batches, total
        RETURN batches, total
        """
        self.session.run(query, edges=edges)

    # --------------------------------------------------------------------------
    # Edges: TRANSPORT
    # --------------------------------------------------------------------------
    def create_transport_edge(self, from_code, to_code, departure, arrival, thread_uid):
        """
        Create a 'TRANSPORT' edge for a specific trip from station A to station B.
        departure and arrival are floats (timestamps).
        """
        query = """
        MATCH (s1:Station {yandex_code: $code1})
        MATCH (s2:Station {yandex_code: $code2})
        MERGE (s1)-[r:TRANSPORT {
            thread_uid: $thread_uid, 
            departure_time: $departure_time,
            arrival_time: $arrival_time
        }]->(s2)
        """
        params = {
            "code1": from_code,
            "code2": to_code,
            "thread_uid": thread_uid,
            "departure_time": departure,
            "arrival_time": arrival
        }
        self.session.run(query, **params)

    def create_transport_edges_bulk(self, edges):
        """
        Bulk-create 'TRANSPORT' edges in batches.
        edges = [
          {
            "from_code": ...,
            "to_code": ...,
            "departure": float_timestamp,
            "arrival":   float_timestamp,
            "thread_uid": ...
          },
          ...
        ]
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
        self.session.run(query, edges=edges).consume()

    def parse_thread(self, thread_json):
        """
        Given a thread_stops() result, parse out the station pairs with departure/arrival.
        Returns (list_of_transport_edges, list_of_all_station_codes).
        """
        stops = thread_json.get("stops", [])
        uid = thread_json.get("uid", "")
        parsed_edges = []
        stations_encountered = []

        for i in range(len(stops) - 1):
            _from = stops[i]
            _to   = stops[i + 1]

            from_code = _from.get("station", {}).get("codes", {}).get("yandex", "UNK")
            to_code   = _to.get("station", {}).get("codes", {}).get("yandex", "UNK")

            dep_str = _from.get("departure")
            arr_str = _to.get("arrival")
            if not dep_str or not arr_str:
                continue

            dep_dt = datetime.strptime(dep_str, DATETIME_FMT)
            arr_dt = datetime.strptime(arr_str, DATETIME_FMT)
            parsed_edges.append({
                "from_code": from_code,
                "to_code": to_code,
                "departure": dep_dt.timestamp(),
                "arrival": arr_dt.timestamp(),
                "thread_uid": uid
            })
            stations_encountered.append(from_code)
            stations_encountered.append(to_code)

        return parsed_edges, stations_encountered

    def populate_transport_edges(self, json_result_schedule):
        """
        Parse a schedule response (station_schedule) to get threads,
        then fetch each thread's stops (bulk), then create edges in DB.
        """
        if not json_result_schedule:
            print("returning pte empty")
            return

        thread_entries = json_result_schedule.get("schedule", [])
        query_list = []
        for entry in thread_entries:
            uid = entry.get("thread", {}).get("uid", "")
            if uid:
                query_list.append({"uid": uid})

        results = self.api.bulk_thread_stops(query_list)
        parsed_transport_edges = []
        station_codes_to_upsert = []

        for thread_json in results:
            if not thread_json:
                continue
            edges_for_this_thread, stations_for_this_thread = self.parse_thread(thread_json)
            parsed_transport_edges.extend(edges_for_this_thread)
            station_codes_to_upsert.extend(stations_for_this_thread)

        self.add_stations_bulk(self.api.fetch_station_info(station_codes_to_upsert))
        self.create_transport_edges_bulk(parsed_transport_edges)

    def populate_walkable_edges(self, source_station_infos, inbound=False):
        """
        For each station in source_station_infos, find walkable neighbors 
        and create the edges in DB.
        If inbound=True, create edges (neighbor -> source). Otherwise (source -> neighbor).
        """
        edges = []
        for source_station_info in source_station_infos:
            station_infos, distances = self.api.walkable_stations(source_station_info)
            if station_infos:
                self.add_stations_bulk(station_infos)
                if not inbound:
                    # (source -> neighbor)
                    parsed_edges = [
                        {
                            "distance_km": dist,
                            "from_code": source_station_info["yandex_code"],
                            "to_code": st["yandex_code"]
                        }
                        for st, dist in zip(station_infos, distances)
                    ]
                else:
                    # (neighbor -> source)
                    parsed_edges = [
                        {
                            "distance_km": dist,
                            "from_code": st["yandex_code"],
                            "to_code": source_station_info["yandex_code"]
                        }
                        for st, dist in zip(station_infos, distances)
                    ]
                edges += parsed_edges
        self.create_walkable_edges_bulk(edges)

    # --------------------------------------------------------------------------
    # Outbound neighbors
    # --------------------------------------------------------------------------
    def get_out_neighbors(self, station_code, date, walk_distance_km=1.0):
        """
        Return all outbound neighbors from station_code (TRANSPORT edges 
        departing >= `date`, plus any WALKABLE edges).
        """
        transport_out = self._fetch_outbound_transport_edges_from_db(station_code, date)
        if not transport_out:
            logging.info(f"[GRAPH] no out nodes, querying for cutoffdate: {date.strftime(DATETIME_FMT)}")
            json_result_schedule = self.api.station_schedule_2days(
                station=station_code,
                dt=date,
                event="departure"
            )
            if json_result_schedule:
                self.populate_transport_edges(json_result_schedule)
            transport_out = self._fetch_outbound_transport_edges_from_db(station_code, date)

        walkable_out = self._fetch_outbound_walkable_edges_from_db(station_code, walk_distance_km)
        if not walkable_out:
            station_info_list = self.api.fetch_station_info([station_code])
            self.populate_walkable_edges(station_info_list, inbound=False)
            walkable_out = self._fetch_outbound_walkable_edges_from_db(station_code, walk_distance_km)

        return transport_out + walkable_out

    def _fetch_outbound_transport_edges_from_db(self, station_code, cutoff_datetime):
        """
        Fetch all TRANSPORT edges that depart from station_code
        with departure_time >= cutoff_datetime.timestamp().
        """
        query = """
        MATCH (s:Station {yandex_code: $station_code})-[r:TRANSPORT]->(nbr:Station)
        RETURN nbr.yandex_code as neighbor_code,
               r.thread_uid    as thread_uid,
               r.departure_time as dep_ts,
               r.arrival_time  as arr_ts
        """
        if cutoff_datetime:
            cutoff_ts = cutoff_datetime.timestamp()
        result = self.session.run(query, station_code=station_code)
        neighbors_out = []
     
        for rec in result:
            nbr_code   = rec["neighbor_code"]
            thread_uid = rec["thread_uid"]
            dep_ts     = rec["dep_ts"]
            arr_ts     = rec["arr_ts"]
     
            if (cutoff_datetime) and dep_ts <= cutoff_ts:
                continue
            dep_dt = datetime.fromtimestamp(dep_ts)
            arr_dt = datetime.fromtimestamp(arr_ts)
            travel_sec = int((arr_dt - dep_dt).total_seconds())
     
            route_info = {
                "thread_uid": thread_uid,
                "departure_time": dep_dt,
                "arrival_time": arr_dt,
            }
            neighbors_out.append(
                (nbr_code, 1, "transport", route_info, 0, travel_sec)
            )
        return neighbors_out

    def _fetch_outbound_walkable_edges_from_db(self, station_code, distance_threshold):
        """
        Fetch all (station_code)-[:WALKABLE]->(nbr) where distance_km <= distance_threshold.
        Return them as (nbr_code, cost_val, "walk", {}, dist_km, travel_time_sec).
        """
        query = """
        MATCH (s:Station {yandex_code:$station_code})-[r:WALKABLE]->(nbr:Station)
        WHERE r.distance_km <= $dist_threshold
        RETURN nbr.yandex_code AS neighbor_code, r.distance_km AS dist_km
        """
        records = self.session.run(query, station_code=station_code, dist_threshold=distance_threshold)
        walkables = []
        for rec in records:
            nbr_code = rec["neighbor_code"]
            dist_km = rec["dist_km"] or 0.0
            travel_time_sec = dist_km * self.WALK_SPEED_SECONDS_PER_KM
            walkables.append((nbr_code, dist_km, "walk", {}, dist_km, travel_time_sec))
        return walkables

    # --------------------------------------------------------------------------
    # Inbound neighbors
    # --------------------------------------------------------------------------
    def get_in_neighbors(self, station_code, date, walk_distance_km=1.0):
        """
        Return all inbound neighbors (TRANSPORT edges that arrive at station_code >= date,
        plus any inbound walk edges).
        """
        inbound_trans = self._fetch_inbound_transport_edges_from_db(station_code, date)
        if not inbound_trans:
            logging.info(f"[GRAPH] no in nodes")
            json_result_schedule = self.api.station_schedule_2days(
                station=station_code,
                dt=date,
                event="arrival"
            )
            if json_result_schedule:
                self.populate_transport_edges(json_result_schedule)
            inbound_trans = self._fetch_inbound_transport_edges_from_db(station_code, date)

        inbound_walk = self._fetch_inbound_walkable_edges_from_db(station_code, walk_distance_km)
        if not inbound_walk:
            station_info_list = self.api.fetch_station_info([station_code])
            self.populate_walkable_edges(station_info_list, inbound=True)
            inbound_walk = self._fetch_inbound_walkable_edges_from_db(station_code, walk_distance_km)

        return inbound_trans + inbound_walk

    def _fetch_inbound_transport_edges_from_db(self, station_code, cutoff_datetime):
        """
        Return inbound TRANSPORT edges that arrive at `station_code` on or before `cutoff_datetime`.
        """
        query = """
        MATCH (nbr:Station)-[r:TRANSPORT]->(s:Station {yandex_code:$station_code})
        RETURN nbr.yandex_code as neighbor_code,
               r.thread_uid as thread_uid,
               r.departure_time as dep_ts,
               r.arrival_time   as arr_ts
        """
        if cutoff_datetime:

            cutoff_ts = cutoff_datetime.timestamp()
        result = self.session.run(query, station_code=station_code)
        neighbors_in = []
     
        for rec in result:
            nbr_code   = rec["neighbor_code"]
            thread_uid = rec["thread_uid"]
            dep_ts     = rec["dep_ts"]
            arr_ts     = rec["arr_ts"]
     
            if (not cutoff_datetime) or arr_ts <= cutoff_ts:
                dep_dt = datetime.fromtimestamp(dep_ts)
                arr_dt = datetime.fromtimestamp(arr_ts)
                travel_sec = int((arr_dt - dep_dt).total_seconds())
     
                route_info = {
                    "thread_uid": thread_uid,
                    "departure_time": dep_dt,
                    "arrival_time": arr_dt,
                }
                neighbors_in.append(
                    (nbr_code, 1, "transport", route_info, 0, travel_sec)
                )
        return neighbors_in

    def _fetch_inbound_walkable_edges_from_db(self, station_code, distance_threshold):
        """
        Return inbound walk edges (nbr)-[:WALKABLE]->(station_code).
        """
        query = """
        MATCH (nbr:Station)-[r:WALKABLE]->(s:Station {yandex_code:$station_code})
        WHERE r.distance_km <= $dist_threshold
        RETURN nbr.yandex_code as neighbor_code, r.distance_km as dist_km
        """
        records = self.session.run(query, station_code=station_code, dist_threshold=distance_threshold)
        inbound_walks = []
        for rec in records:
            nbr_code = rec["neighbor_code"]
            dist_km  = rec["dist_km"] or 0.0
            travel_time_sec = dist_km * self.WALK_SPEED_SECONDS_PER_KM
            inbound_walks.append((nbr_code, dist_km, "walk", {}, dist_km, travel_time_sec))
        return inbound_walks

