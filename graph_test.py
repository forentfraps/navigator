# main.py

from yapi import yAPI
from lazygraph import LazyRouteGraph
import a_star
from datetime import datetime

def print_time_table_path(path, graph):
    """
    Pretty-print a path that includes actual departure/arrival times.
    """
    if not path:
        print("No path found.")
        return

    print("Reconstructed path with schedule times:")
    first_src = path[0][0]
    print(f"Start at station code={first_src} ({graph._stations[first_src]['title']})")

    prev_arrival = None
    for (src, dst, mode, route_info, arrival_dt) in path:
        src_name = graph._stations[src]["title"]
        dst_name = graph._stations[dst]["title"]

        if mode == "walk":
            # For walking edges, we didn't store departure_time in route_info.
            # The arrival_dt is the arrival at `dst`.
            print(f"  Walk from {src_name} to {dst_name}, arrive at {arrival_dt}")
        else:
            # We have route_info with departure_time, arrival_time
            dep_dt = route_info.get("departure_time", None)
            arr_dt = route_info.get("arrival_time", None)
            print(f"  {mode.upper()} {route_info.get('title','')} from {src_name} => {dst_name}")
            print(f"      Depart: {dep_dt}, Arrive: {arr_dt}, We actually reach at {arrival_dt}")

    print("\nFinished at station code={path[-1][1]} arrival={path[-1][4]}")


def main():
    api = yAPI(cache_file="resp.json")
    graph = LazyRouteGraph(
        api, stations_file="resp.json",
        routes_folder="routes",
        cost_mode="time", 
        debug=False
    )

    # Example: define from/to by settlement codes
    from_settlement_code = "c969"       # Rostov e.g.
    to_settlement_code   = "c39"    # St Petersburg e.g.
    
    from_stations = api.get_settlement_station_codes(from_settlement_code)
    to_stations   = api.get_settlement_station_codes(to_settlement_code)

    print("Len from_stations:", len(from_stations))
    print("Len to_stations:", len(to_stations))

    # We'll do earliest arrival search from now
    departure_time = datetime.now()

    path = a_star.time_table_search_settlements(
        graph=graph,
        start_stations=from_stations,
        goal_stations=to_stations,
        start_time=departure_time,
        debug=False
    )

    print_time_table_path(path, graph)

if __name__ == "__main__":
    main()
