# main.py

from yapi import yAPI
from lazygraph import LazyRouteGraph
import a_star

def print_path_with_cumulative(path, graph):
    """
    For each edge, show cumulative cost from the start (in distance or time).
    """
    if not path:
        print("No path found.")
        return

    cost_mode = graph.cost_mode  # 'distance' or 'time'
    cumulative = 0.0

    print("Reconstructed path:")
    first_src = path[0][0]
    print(f"Start at {first_src} (cumulative=0)")

    for (src, dst, mode, route_info, dist_km, travel_time_sec) in path:
        # The "cost" used in expansions:
        cost_for_edge = dist_km if (cost_mode == "distance") else travel_time_sec
        cumulative += cost_for_edge

        if cost_mode == "time":
            step_str = f"{cost_for_edge/60:.1f} min"
            cumul_str = f"{cumulative/60:.1f} min total"
        else:
            step_str = f"{cost_for_edge:.2f} km"
            cumul_str = f"{cumulative:.2f} km total"
        src_name = graph._stations[src]["title"]
        dst_name = graph._stations[dst]["title"]

        print(f"  {src_name} -> {dst_name} via {mode} | edge cost={step_str}, {cumul_str}, route={route_info.get('title','')}")

    if cost_mode == "time":
        print(f"\nTotal travel time: {cumulative/60:.1f} min.")
    else:
        print(f"\nTotal distance: {cumulative:.2f} km.")


def main():
    # 1) Create yAPI and graph
    api = yAPI(cache_file="resp.json")

    # cost_mode can be "distance" or "time"
    graph = LazyRouteGraph(
        api, stations_file="resp.json", 
        routes_folder="routes", cost_mode="time", debug=False)

    # 2) Suppose we found these settlement codes from search_settlements or known:
    from_settlement_code = "c37"  # e.g., Rostov
    to_settlement_code   = "c11435"   # e.g., Saint Petersburg

    # 3) Collect all station codes for these settlements
    from_stations = api.get_settlement_station_codes(from_settlement_code)
    to_stations   = api.get_settlement_station_codes(to_settlement_code)
    print("Len from stations: ", len(from_stations))
    print("Len to stations: ", len(to_stations))

    # 4) Call multi-settlement A*
    path = a_star.a_star_search_settlements(
        graph=graph,
        start_stations=from_stations,
        goal_stations=to_stations,
        debug=False
    )

    # 5) Print final results
    print_path_with_cumulative(path, graph)

if __name__ == "__main__":
    main()

