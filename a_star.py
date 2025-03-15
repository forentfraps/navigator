
# a_star.py
import heapq
from math import inf
from datetime import datetime, timedelta

def time_table_search_settlements(api, transport_graph, start_stations, goal_stations, start_time=None, debug=False):
    """
    Earliest-arrival search from multiple start stations to any of the goal stations.
    We'll track the best known arrival time at each station in 'best_arrival[station]'.

    :param transport_graph: an instance of your TransportGraph class 
                           that has a get_neighbors(station_code) method.
    :param start_stations: list of station codes for possible start points
    :param goal_stations:  list of station codes for possible goal points
    :param start_time: a Python datetime indicating when we are "ready" at the start station(s).
                       If None, defaults to now.
    :param debug: enable/disable debug prints
    :return: A list of edges representing the path, where each edge is
             (src_station, dst_station, mode, route_info, arrival_dt)
             or an empty list if no path was found.
    """
    def dprint(*args):
        if debug:
            print("[TimeTable]", *args)

    goal_set = set(goal_stations)
    if start_time is None:
        start_time = datetime.now()

    # best_arrival[station] = earliest known arrival time at 'station'
    best_arrival = {}
    came_from = {}

    # Priority queue (min-heap) to store (arrival_time, station)
    frontier = []

  
    # Initialize frontier with all start stations at the same start_time
    for st in start_stations:
        best_arrival[st] = start_time
        heapq.heappush(frontier, (start_time, st))
        dprint(f"Push start station={st} at time={start_time}")

    # Main loop

    while frontier:
        current_time, station = heapq.heappop(frontier)
        dprint(f"Pop station={station} at time={current_time}")

        # If we popped a station older than the best known => skip
        if current_time > best_arrival.get(station, datetime.max):
            dprint("  ... stale entry, skipping.")
            continue

        # Check if this is a goal station
        if station in goal_set:
            dprint("  Reached goal station!")
            # Reconstruct and return path
            return _reconstruct_path_with_times(came_from, station)

        json_result_schedule = api.station_schedule(station, date = current_time)
        transport_graph.populate_transport_edges(json_result_schedule)
        transport_graph.populate_walkable_edges(api.fetch_station_info([station]))
        # Explore neighbors from the database
        neighbors = transport_graph.get_neighbors(station, date=current_time)
        for (nbr, cost_value, mode, route_info, dist_km, travel_time_sec) in neighbors:
            arrival_candidate = None

            if mode == "walk":
                # You can start walking immediately at current_time
                dt_delta = timedelta(seconds=travel_time_sec)
                arrival_candidate = current_time + dt_delta

            elif mode == "transport":
                dep_dt = route_info.get("departure_time", None)
                arr_dt = route_info.get("arrival_time", None)

                if not dep_dt or not arr_dt:
                    # If missing times, we can't use this trip
                    continue

                # Must arrive at 'station' before dep_dt to catch it
                if dep_dt < current_time:
                    # We cannot catch it if it departs before our current arrival time
                    continue

                # Board at dep_dt, arrive at arr_dt
                arrival_candidate = arr_dt

            # Check if we can improve earliest arrival at 'nbr'
            if arrival_candidate is None:
                continue

            prev_best = best_arrival.get(nbr, datetime.max)
            if arrival_candidate < prev_best:
                best_arrival[nbr] = arrival_candidate
                came_from[nbr] = (station, mode, route_info, dist_km, arrival_candidate)
                heapq.heappush(frontier, (arrival_candidate, nbr))
                dprint(f"  Update {nbr} => arrival={arrival_candidate}, mode={mode}")

    # If we exhaust the queue without reaching a goal
    dprint("No path found.")
    return []

def _reconstruct_path_with_times(came_from, end_station):
    """
    Reconstruct the path in the form:
      [ (src, dst, mode, route_info, arrival_dt), ... ]
    where arrival_dt is when we arrive at 'dst'.
    """
    path_edges = []
    current = end_station
    while current in came_from:
        prev_station, mode, route_info, dist_km, arrival_dt = came_from[current]
        path_edges.append((prev_station, current, mode, route_info, arrival_dt))
        current = prev_station
    path_edges.reverse()
    return path_edges
