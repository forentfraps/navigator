# a_star.py

import heapq
from math import inf

def a_star_search(graph, start_code, goal_code, debug=False):
    """
    Single-station to single-station A*.
    'cost' = distance or time depending on graph.cost_mode.
    Returns a path: [ (src, dst, mode, route_info, dist_km, travel_time_sec), ... ]
    """

    def dprint(*args):
        if debug:
            print("[A*]", *args)

    # Quick checks
    if start_code not in graph._stations:
        dprint(f"Start station {start_code} not in graph.")
        return []
    if goal_code not in graph._stations:
        dprint(f"Goal station {goal_code} not in graph.")

    def heuristic(a, b):
        # Always uses station_distance => a "distance" in km
        dist = graph.station_distance(a, b)
        return dist if dist != inf else 0.0

    g_score = {start_code: 0.0}
    f_score = {start_code: heuristic(start_code, goal_code)}
    came_from = {}

    frontier = []
    heapq.heappush(frontier, (f_score[start_code], start_code))
    visited_best_f = {start_code: f_score[start_code]}

    while frontier:
        current_f, current_station = heapq.heappop(frontier)
        dprint(f"Popped {current_station}, f={current_f:.2f}")

        if current_station == goal_code:
            dprint("Reached goal!")
            return _reconstruct_path_edges(came_from, current_station)

        if current_f > visited_best_f.get(current_station, inf):
            dprint(f"Skipping {current_station}, better path found earlier.")
            continue

        neighbors = graph.get_neighbors(current_station)
        for (nbr_code, cost_value, mode, route_info, dist_km, travel_time_sec) in neighbors:
            tentative_g = g_score[current_station] + cost_value
            if tentative_g < g_score.get(nbr_code, inf):
                g_score[nbr_code] = tentative_g
                new_f = tentative_g + heuristic(nbr_code, goal_code)
                f_score[nbr_code] = new_f
                came_from[nbr_code] = (current_station, mode, route_info, dist_km, travel_time_sec)
                visited_best_f[nbr_code] = new_f
                heapq.heappush(frontier, (new_f, nbr_code))
                dprint(f"  Updated {nbr_code}, cost={cost_value:.2f}, f={new_f:.2f}, g={tentative_g:.2f}")

    dprint("No path found.")
    return []


def a_star_search_settlements(graph, start_stations, goal_stations, debug=False):
    """
    Multi-start to multi-goal A*.
    We place all 'start_stations' in the queue at g=0, and the search stops if we
    pop a station in 'goal_stations'.

    Returns path in same format: [ (src, dst, mode, route_info, dist_km, travel_time_sec), ... ].
    """

    def dprint(*args):
        if debug:
            print("[A*]", *args)

    goal_set = set(goal_stations)

    def heuristic(a):
        # We'll pick the station_distance to the NEAREST goal station
        # (Naive approach: just loop over all goals).
        # For huge goal sets, you might want a more efficient approach.
        best_dist = inf
        for g in goal_set:
            d = graph.station_distance(a, g)
            if d < best_dist:
                best_dist = d
        return best_dist if best_dist != inf else 0.0

    g_score = {}
    f_score = {}
    came_from = {}
    visited_best_f = {}
    frontier = []

    # Initialize frontier with all start stations at g=0
    for st in start_stations:
        g_score[st] = 0.0
        hval = heuristic(st)
        f_score[st] = hval
        visited_best_f[st] = hval
        heapq.heappush(frontier, (hval, st))

    while frontier:
        current_f, current_station = heapq.heappop(frontier)
        dprint(f"Popped {current_station}, f={current_f:.2f}")

        if current_station in goal_set:
            dprint(f"Reached goal station: {current_station}")
            return _reconstruct_path_edges(came_from, current_station)

        if current_f > visited_best_f.get(current_station, inf):
            dprint(f"Skipping {current_station}, better path was found.")
            continue

        neighbors = graph.get_neighbors(current_station)
        for (nbr_code, cost_value, mode, route_info, dist_km, travel_time_sec) in neighbors:
            tentative_g = g_score[current_station] + cost_value
            if tentative_g < g_score.get(nbr_code, inf):
                g_score[nbr_code] = tentative_g
                new_f = tentative_g + heuristic(nbr_code)
                f_score[nbr_code] = new_f
                came_from[nbr_code] = (current_station, mode, route_info, dist_km, travel_time_sec)
                visited_best_f[nbr_code] = new_f
                heapq.heappush(frontier, (new_f, nbr_code))
                dprint(f"  Updated {nbr_code}, cost={cost_value:.2f}, f={new_f:.2f}, g={tentative_g:.2f}")

    dprint("No path found from any start station to any goal station.")
    return []


def _reconstruct_path_edges(came_from, end_station):
    """
    Reconstruct edges in the form:
      [ (src, dst, mode, route_info, dist_km, travel_time_sec), ... ]
    from came_from dict:
      came_from[station] = (prev_station, mode, route_info, dist_km, travel_time_sec)
    """
    path_edges = []
    current = end_station
    while current in came_from:
        prev_station, mode, route_info, dist_km, travel_time_sec = came_from[current]
        path_edges.append((prev_station, current, mode, route_info, dist_km, travel_time_sec))
        current = prev_station
    path_edges.reverse()
    return path_edges

