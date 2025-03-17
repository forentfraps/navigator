#a_star.py

import heapq
import math
from datetime import datetime, timedelta
import logging

###########################################################
# Helpers: Haversine, lat-lon retrieval, cost/time logic
###########################################################

def haversine_km(lat1, lon1, lat2, lon2):
    """
    Returns the great-circle distance between two points (in km).
    """
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def load_latlon_cache(transport_graph):
    """
    Build a dict: {station_code: (latitude, longitude)} for quick lookup.
    """
    latlon_map = {}
    with transport_graph.driver.session() as session:
        query = """
        MATCH (s:Station)
        WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL
        RETURN s.yandex_code AS code, s.latitude AS lat, s.longitude AS lon
        """
        for rec in session.run(query):
            latlon_map[rec["code"]] = (rec["lat"], rec["lon"])
    return latlon_map

def heuristic_km(station_code, goal_stations, latlon_cache):
    """
    The A* heuristic: the minimum Haversine distance from station_code
    to any station in goal_stations (in km).
    If station_code not in latlon_cache, returns 0 for safety.
    """
    if station_code not in latlon_cache:
        return 0.0
    (lat1, lon1) = latlon_cache[station_code]
    if (not lat1) or (not lon1):
        return 0.0
    best_dist = float("inf")
    for gst in goal_stations:
        if gst in latlon_cache:
            (lat2, lon2) = latlon_cache[gst]
            if (not lat2) or (not lon2):
                continue
            dist = haversine_km(lat1, lon1, float(lat2), float(lon2))
            if dist < best_dist:
                best_dist = dist
    if best_dist == float("inf"):
        best_dist = 0.0
    return best_dist

###########################################################
# Mode-specific cost + expansions
###########################################################

TRANSPORT_RATIO = 1.0  # fallback ratio for transport edges
WALK_SPEED_SEC_PER_KM = 720

def forward_neighbors(api, transport_graph, station_code, current_g, current_time, mode, latlon_cache):
    """
    Returns a list of forward neighbors:
       [ (neighbor_code, cost_of_edge, arrival_time, neighbor_mode, neighbor_dist_km, thread_uid) , ... ]
    """
    edges = transport_graph._fetch_outbound_transport_edges_from_db(station_code, current_time)
    walkable = transport_graph._fetch_outbound_walkable_edges_from_db(station_code, 9999999)
    if (not edges) and (not walkable):
        schedule_json = api.station_schedule_2days(station=station_code, dt=current_time, event="departure")
        if schedule_json:
            transport_graph.populate_transport_edges(schedule_json)
        edges = transport_graph._fetch_outbound_transport_edges_from_db(station_code, current_time)
        walkable = transport_graph._fetch_outbound_walkable_edges_from_db(station_code, 9999999)

    neighbors_out = []
    # Transport edges
    for (nbr_code, _, edge_mode, route_info, dist_km, travel_sec) in edges:
        if edge_mode != "transport":
            continue
        dep_time = route_info["departure_time"]  # datetime
        arr_time = route_info["arrival_time"]    # datetime
        thread_uid = route_info["thread_uid"]
        if mode == "time":
            wait_sec = 0
            if dep_time > current_time:
                wait_sec = (dep_time - current_time).total_seconds()
            edge_cost = wait_sec + (arr_time - dep_time).total_seconds()
            next_time = arr_time
            neighbors_out.append((
                nbr_code, edge_cost, next_time, "transport",
                estimate_edge_distance_km(station_code, nbr_code, latlon_cache), thread_uid
            ))
        else:  # mode == "cost"
            dist_for_edge = estimate_edge_distance_km(station_code, nbr_code, latlon_cache)
            edge_cost = dist_for_edge * TRANSPORT_RATIO
            neighbors_out.append((
                nbr_code, edge_cost, None, "transport", dist_for_edge, thread_uid
            ))

    # Walkable edges
    for (nbr_code, dist_km, edge_mode, _, dist_km_again, travel_sec) in walkable:
        if edge_mode != "walk":
            continue
        if mode == "time":
            edge_cost = travel_sec
            next_time = current_time + timedelta(seconds=travel_sec)
            neighbors_out.append((
                nbr_code, edge_cost, next_time, "walk", dist_km, None
            ))
        else:
            edge_cost = 0
            neighbors_out.append((
                nbr_code, edge_cost, None, "walk", dist_km, None
            ))

    return neighbors_out

def backward_neighbors(api, transport_graph, station_code, current_g, current_time, mode, latlon_cache):
    """
    Symmetric to forward_neighbors, but for the backward search.
    Returns a list of inbound neighbors as:
      [ (neighbor_code, cost_of_edge, departure_time, neighbor_mode, neighbor_dist_km, thread_uid) , ... ]
    """
    edges = transport_graph._fetch_inbound_transport_edges_from_db(station_code, current_time)
    walkable = transport_graph._fetch_inbound_walkable_edges_from_db(station_code, 9999999)
    if (not edges) and (not walkable):
        schedule_json = api.station_schedule_2days(station=station_code, dt=current_time, event="arrival")
        if schedule_json:
            transport_graph.populate_transport_edges(schedule_json)
        edges = transport_graph._fetch_inbound_transport_edges_from_db(station_code, current_time)
        walkable = transport_graph._fetch_inbound_walkable_edges_from_db(station_code, 9999999)

    neighbors_in = []
    # Transport edges
    for (nbr_code, _, edge_mode, route_info, dist_km, travel_sec) in edges:
        if edge_mode != "transport":
            continue
        dep_time = route_info["departure_time"]
        arr_time = route_info["arrival_time"]
        thread_uid = route_info["thread_uid"]
        if mode == "time":
            edge_cost = (arr_time - dep_time).total_seconds()
            next_time = dep_time
            neighbors_in.append((
                nbr_code, edge_cost, next_time, "transport",
                estimate_edge_distance_km(nbr_code, station_code, latlon_cache), thread_uid
            ))
        else:
            dist_for_edge = estimate_edge_distance_km(nbr_code, station_code, latlon_cache)
            edge_cost = dist_for_edge * TRANSPORT_RATIO
            neighbors_in.append((
                nbr_code, edge_cost, None, "transport", dist_for_edge, thread_uid
            ))

    # Walkable edges
    for (nbr_code, dist_km, edge_mode, _, dist_km_again, travel_sec) in walkable:
        if edge_mode != "walk":
            continue
        if mode == "time":
            edge_cost = travel_sec
            next_time = current_time - timedelta(seconds=travel_sec)
            neighbors_in.append((
                nbr_code, edge_cost, next_time, "walk", dist_km, None
            ))
        else:
            edge_cost = 0
            neighbors_in.append((
                nbr_code, edge_cost, None, "walk", dist_km, None
            ))
    return neighbors_in

def estimate_edge_distance_km(station_a, station_b, latlon_cache):
    """
    Estimate the distance in km between two stations based on lat/lon.
    """
    if station_a not in latlon_cache or station_b not in latlon_cache:
        return 0.0
    (lat1, lon1) = latlon_cache[station_a]
    (lat2, lon2) = latlon_cache[station_b]
    if (not lat1) or (not lon1) or (not lat2) or (not lon2):
        return 0.0
    return haversine_km(lat1, lon1, lat2, lon2)

###########################################################
# Bidirectional A*
###########################################################

def bidirectional_a_star(api, transport_graph,
                         start_stations,
                         goal_stations,
                         start_time=None,
                         mode="time",
                         debug=False):
    """
    Bidirectional A* from multiple start stations to multiple goal stations.
    Returns a path as a list of edges:
      [ (src, dst, mode, cost, time_or_None, thread_uid), ... ]
    or an empty list if no path is found.
    """
    if start_time is None:
        start_time = datetime.now()

    latlon_cache = load_latlon_cache(transport_graph)
    goal_set = set(goal_stations)
    start_set = set(start_stations)

    forward_g = {}
    backward_g = {}
    forward_time = {}   # for time mode
    backward_time = {}

    forward_parent = {}  # station -> (prev_station, edge_mode, cost, arrival_time, thread_uid)
    backward_parent = {} # station -> (next_station, edge_mode, cost, departure_time, thread_uid)

    forward_pq = []
    backward_pq = []

    INF = float("inf")

    # Initialize forward queue
    for s in start_stations:
        forward_g[s] = 0.0
        if mode == "time":
            forward_time[s] = start_time
        hval = heuristic_km(s, goal_set, latlon_cache)
        fval = 0.0 + hval
        heapq.heappush(forward_pq, (fval, s))

    if mode == "time":
        infinite_future = start_time + timedelta(hours=48)
        for g in goal_stations:
            backward_g[g] = 0.0
            backward_time[g] = infinite_future
            hval = heuristic_km(g, start_set, latlon_cache)
            fval = 0.0 + hval
            heapq.heappush(backward_pq, (fval, g))
    else:
        for g in goal_stations:
            backward_g[g] = 0.0
            backward_time[g] = None
            hval = heuristic_km(g, start_set, latlon_cache)
            fval = 0.0 + hval
            heapq.heappush(backward_pq, (fval, g))

    best_path_cost = INF
    meeting_station = None

    def check_meeting(station):
        nonlocal best_path_cost, meeting_station
        if station in forward_g and station in backward_g:
            total_cost = forward_g[station] + backward_g[station]
            if total_cost < best_path_cost:
                best_path_cost = total_cost
                meeting_station = station

    def expand_forward():
        if not forward_pq:
            return
        fval, st = heapq.heappop(forward_pq)
        if st not in forward_g:
            return
        gval = forward_g[st]
        hval = heuristic_km(st, goal_set, latlon_cache)
        if (gval + hval) < fval - 1e-9:
            return  # stale
        check_meeting(st)
        cur_time = forward_time.get(st, start_time) if mode == "time" else None
        nbrs = forward_neighbors(api, transport_graph, st, gval, cur_time, mode, latlon_cache)
        for (nbr_code, edge_cost, arrival_time, edge_mode, dist_km, thread_uid) in nbrs:
            candidate_g = gval + edge_cost
            old_g = forward_g.get(nbr_code, INF)
            if candidate_g < old_g:
                forward_g[nbr_code] = candidate_g
                forward_parent[nbr_code] = (st, edge_mode, edge_cost,
                                             arrival_time if mode == "time" else None,
                                             thread_uid)
                if mode == "time":
                    forward_time[nbr_code] = arrival_time
                hval_nbr = heuristic_km(nbr_code, goal_set, latlon_cache)
                fval_nbr = candidate_g + hval_nbr
                heapq.heappush(forward_pq, (fval_nbr, nbr_code))
                check_meeting(nbr_code)

    def expand_backward():
        if not backward_pq:
            return
        fval, st = heapq.heappop(backward_pq)
        if st not in backward_g:
            return
        gval = backward_g[st]
        hval = heuristic_km(st, start_set, latlon_cache)
        if (gval + hval) < fval - 1e-9:
            return  # stale
        check_meeting(st)
        cur_time = backward_time.get(st, None)
        nbrs = backward_neighbors(api, transport_graph, st, gval, cur_time, mode, latlon_cache)
        for (nbr_code, edge_cost, departure_time, edge_mode, dist_km, thread_uid) in nbrs:
            candidate_g = gval + edge_cost
            old_g = backward_g.get(nbr_code, INF)
            if candidate_g < old_g:
                backward_g[nbr_code] = candidate_g
                backward_parent[nbr_code] = (st, edge_mode, edge_cost,
                                               departure_time if mode == "time" else None,
                                               thread_uid)
                if mode == "time":
                    backward_time[nbr_code] = departure_time
                hval_nbr = heuristic_km(nbr_code, start_set, latlon_cache)
                fval_nbr = candidate_g + hval_nbr
                heapq.heappush(backward_pq, (fval_nbr, nbr_code))
                check_meeting(nbr_code)

    while forward_pq and backward_pq:
        if meeting_station is not None:
            f_fwd = forward_pq[0][0] if forward_pq else INF
            f_bwd = backward_pq[0][0] if backward_pq else INF
            f_min = min(f_fwd, f_bwd)
            if best_path_cost <= f_min:
                break
        if forward_pq and backward_pq:
            if forward_pq[0][0] < backward_pq[0][0]:
                expand_forward()
            else:
                expand_backward()
        elif forward_pq:
            expand_forward()
        elif backward_pq:
            expand_backward()

    if meeting_station is None:
        return []

    return reconstruct_bidirectional_path(meeting_station,
                                          forward_parent,
                                          backward_parent,
                                          mode)

def reconstruct_bidirectional_path(meeting_station, forward_parent, backward_parent, mode):
    """
    Reconstruct the final path as a list of edges:
      [ (src, dst, mode, cost, time_or_None, thread_uid), ... ]
    """
    forward_edges = []
    cur = meeting_station
    while cur in forward_parent:
        (prev_st, edge_mode, edge_cost, arr_time, thread_uid) = forward_parent[cur]
        forward_edges.append((prev_st, cur, edge_mode, edge_cost, arr_time, thread_uid))
        cur = prev_st
    forward_edges.reverse()

    backward_edges = []
    cur = meeting_station
    while cur in backward_parent:
        (next_st, edge_mode, edge_cost, dep_time, thread_uid) = backward_parent[cur]
        backward_edges.append((cur, next_st, edge_mode, edge_cost, dep_time, thread_uid))
        cur = next_st

    return forward_edges + backward_edges

###########################################################
# Wrapper
###########################################################

def search_settlements_bidirectional(api, transport_graph,
                                     start_stations,
                                     goal_stations,
                                     start_time=None,
                                     mode="time",
                                     debug=False):
    """
    Wrapper that calls our bidirectional A* with the given mode.
    Returns a list of edges: 
      [ (src, dst, mode, cost, time_or_None, thread_uid), ... ]
    or [] if no path is found.
    """
    return bidirectional_a_star(api, transport_graph,
                                start_stations,
                                goal_stations,
                                start_time=start_time,
                                mode=mode,
                                debug=debug)

