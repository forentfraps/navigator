from yapi import yAPI
from lazygraph import Neo4jRouteGraph  # <== updated import
import datetime

def import_station_schedule_to_neo4j(api, neo_graph, station_code, date=None):
    """
    For a given station_code, fetch schedule from Yandex, then build edges in Neo4j for each segment.
    """
    schedule_data = api.station_schedule(station_code, date=date)
    if not schedule_data:
        logging.info(f"No schedule data returned for {station_code}")
        return

    for item in schedule_data.get("schedule", []):
        thread = item.get("thread", {})
        uid = thread.get("uid")
        transport_type = thread.get("transport_type", "unknown")
        route_title = thread.get("title", "")

        if not uid:
            continue

        # Full thread stops
        thread_data = api.thread_stops(uid)
        if not thread_data:
            continue

        stops = thread_data.get("stops", [])
        for i in range(len(stops) - 1):
            stA = stops[i].get("station", {})
            stB = stops[i + 1].get("station", {})

            codeA = stA.get("codes", {}).get("yandex")
            codeB = stB.get("codes", {}).get("yandex")
            if not codeA or not codeB:
                continue

            dep_str = stops[i].get("departure")
            arr_str = stops[i + 1].get("arrival")

            # Insert transport edge in Neo4j
            neo_graph.create_transport_edges(
                from_code      = codeA,
                to_code        = codeB,
                thread_uid     = uid,
                transport_type = transport_type,
                departure_str  = dep_str,
                arrival_str    = arr_str,
                route_title    = route_title
            )

def main():
    api = yAPI(cache_file="resp.json")

    # Connect to Neo4j
    neo_graph = Neo4jRouteGraph(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="yourpassword",
        walk_distance_km=1.0,
        cost_mode="time",
        debug=True
    )

    # 1) Load station data into Neo4j:
    data = api.get_stations_data(force_download=False)
    neo_graph.load_stations_data(data)

    # 2) Optionally build walk edges
    neo_graph.build_walk_edges()

    # 3) Import schedules for selected stations:
    station_codes = ["s9601445", "s9601446"]  # Example
    for code in station_codes:
        import_station_schedule_to_neo4j(api, neo_graph, code, date="2025-06-01")

    neo_graph.close()

if __name__ == "__main__":
    main()

