#main.py

import a_star
from lazygraph import TransportGraph
from yapi import yAPI
import yapi
from datetime import datetime, timezone, timedelta
from time import sleep
import logging
import threading
import queue

DOCKER = False

def docker_main():
    logging.basicConfig(level=logging.INFO)
    api = yAPI(cache_file="resp.json")  # Make sure 'resp.json' has your stations_list or it will download
    if DOCKER:
        sleep(37)
        transport_graph = TransportGraph(uri="bolt://neo4j:7687", user="neo4j", password="secretgraph")
    else:
        transport_graph = TransportGraph(uri="bolt://localhost:7687", user="neo4j", password="secretgraph")

    from_settlement_code = "c2"
    to_settlement_code   = "c213"

    # Get station codes in those settlements
    from_stations = api.get_settlement_station_codes(from_settlement_code)
    to_stations   = api.get_settlement_station_codes(to_settlement_code)

    start_time = datetime.now() + timedelta(hours = 3)

    # Example of calling each mode:
    # 1) Basic: ignoring time, then verifying
    path_basic = a_star.search_settlements_bidirectional(
        api,
        transport_graph,
        start_stations=from_stations,
        goal_stations=to_stations,
        mode="basic",  # <--- new mode
        start_time=start_time,
        debug=True
    )
    logging.warning(str(path_basic))
    if path_basic:
        logging.warning("FOUND BASIC PATH (ignoring time, then verifying schedule).")
        for (src, dst, mode_used, route_info, time_stamp) in path_basic:
            logging.warning(f"{src} -> {dst} via {mode_used}, time={time_stamp}")
    else:
        logging.warning("No feasible BASIC path found (or schedule verification failed).")
        for (src, dst, mode_used, route_info, time_stamp) in path_basic:
            logging.warning(f"{src} -> {dst} via {mode_used}, time={time_stamp}")

    input()
    # 2) Time-based (earliest arrival)
    path_time = a_star.search_settlements_bidirectional(
        api,
        transport_graph,
        start_stations=from_stations,
        goal_stations=to_stations,
        mode="time",   # <--- your original earliest-arrival approach
        start_time=start_time,
        debug=True
    )
    if path_time:
        logging.warning("FOUND TIME-BASED PATH (earliest arrival).")
        for (src, dst, mode_used, route_info, time_stamp) in path_time:
            logging.warning(f"{src} -> {dst} via {mode_used}, time={time_stamp}")
    else:
        logging.warning("No feasible TIME-based path found.")

    # 3) Cost-based (distance × ratio)
    path_cost = a_star.search_settlements_bidirectional(
        api,
        transport_graph,
        start_stations=from_stations,
        goal_stations=to_stations,
        mode="cost",   # <--- cost-based approach
        start_time=start_time,  # not strictly needed if ignoring time
        debug=True
    )
    if path_cost:
        logging.warning("FOUND COST-BASED PATH.")
        for (src, dst, mode_used, route_info, cost_val) in path_cost:
            logging.warning(f"{src} -> {dst} via {mode_used}, cost={cost_val} ratio-dist-based")
    else:
        logging.warning("No feasible COST-based path found.")

    transport_graph.close()

def get_station_title(neo_graph, station_code):
    with neo_graph.driver.session() as session:
        res = session.run("""
            MATCH (s:Station {yandex_code:$code}) RETURN s.title AS t
        """, code=station_code).single()
        return res["t"] if res else station_code

def pre_cache():
    """
    Example snippet for pre-caching and populating the DB with schedules.
    """
    def producer(station_infos, result_queue, batch_size=200):
        for i in range(0, len(station_infos), batch_size):
            batch = station_infos[i: i+batch_size]
            payload = [{"station": info.get("yandex_code")} for info in batch]
            try:
                json_response = api.bulk_station_schedule(payload)
                break
            except:
                continue
            result_queue.put(json_response)
        result_queue.put(None)

    def consumer(result_queue):
        while True:
            json_response = result_queue.get()  # Blocks until an item is available
            if json_response is None:
                break
            for single_json_response in json_response:
                transport_graph.populate_transport_edges(single_json_response)
            result_queue.task_done()

    logging.basicConfig(level=logging.INFO)

    from_settlement_code = "c66"   # Example
    to_settlement_code   = "c65"   # Example
    api = yAPI(cache_file="resp.json")
    transport_graph = TransportGraph(uri="bolt://localhost:7687", user="neo4j", password="secretgraph")
    station_infos = api.fetch_station_info(None)

    def dum():
        pass

    for station_info in station_infos:
        json_responses = api.bulk_station_schedule([{"station": station_info.get("yandex_code")}])
        [transport_graph.populate_transport_edges(json_response) if json_response else dum() for json_response in json_responses]

def populate_walkable():
    api = yAPI(cache_file="resp.json")
    station_infos = api.fetch_station_info(None)
    yapi.generate_relationship_csv(station_infos, 1.0, "walkable.csv")

if __name__ == "__main__":
    docker_main()

