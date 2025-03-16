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
    # In a multi-goal scenario, pick the minimum distance to *any* goal station
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

TRANSPORT_RATIO = 1.0  # If you can't detect sub-type, treat "transport" edges as ratio=1
WALK_SPEED_SEC_PER_KM = 720  # must match your TransportGraph?

def forward_neighbors(api, transport_graph, station_code, current_g, current_time, mode, latlon_cache):
    """
    Returns a list of forward neighbors:
       [ (neighbor_code, cost_of_edge, arrival_time, neighbor_mode, neighbor_dist_km) , ... ]

    - cost_of_edge: how many 'units' to add to G
      * For 'time' mode => waiting time + travel time (in seconds).
      * For 'cost' mode => distance * ratio.
    - arrival_time: relevant only in 'time' mode. Otherwise set to None.
    - neighbor_mode: "walk" or "transport" (just to reconstruct path).
    - neighbor_dist_km: distance in km (used for the heuristic).
    """
    # If no edges exist, fetch from API
    edges = transport_graph._fetch_outbound_transport_edges_from_db(station_code, current_time)
    walkable = transport_graph._fetch_outbound_walkable_edges_from_db(station_code, 1.0)
    # If both are empty, let's try to populate from Yandex 48h window:
    if (not edges) and (not walkable):
        # Query for 2 days (departures)
        schedule_json = api.station_schedule_2days(station=station_code, dt=current_time, event="departure")
        if schedule_json:
            transport_graph.populate_transport_edges(schedule_json)
        # re-fetch
        edges = transport_graph._fetch_outbound_transport_edges_from_db(station_code, current_time)
        walkable = transport_graph._fetch_outbound_walkable_edges_from_db(station_code, 1.0)

    neighbors_out = []

    # 1) Transport edges
    for (nbr_code, _, edge_mode, route_info, dist_km, travel_sec) in edges:
        if edge_mode != "transport":
            continue
        dep_time = route_info["departure_time"]  # datetime
        arr_time = route_info["arrival_time"]    # datetime

        if mode == "time":
            # waiting time if dep_time > current_time
            wait_sec = 0
            if dep_time > current_time:
                wait_sec = (dep_time - current_time).total_seconds()
            edge_cost = wait_sec + (arr_time - dep_time).total_seconds()  # total sec
            next_time = arr_time
            neighbors_out.append((
                nbr_code, edge_cost, next_time, "transport",
                estimate_edge_distance_km(station_code, nbr_code, latlon_cache)
            ))
        else:  # mode == "cost"
            # We do not have sub-type => use a fallback ratio
            # You *could* parse the route_info or station for sub-type if stored
            dist_for_edge = estimate_edge_distance_km(station_code, nbr_code, latlon_cache)
            edge_cost = dist_for_edge * TRANSPORT_RATIO
            neighbors_out.append((
                nbr_code, edge_cost, None, "transport", dist_for_edge
            ))

    # 2) Walkable edges
    for (nbr_code, dist_km, edge_mode, _, dist_km_again, travel_sec) in walkable:
        if edge_mode != "walk":
            continue
        if mode == "time":
            # walking time in seconds
            edge_cost = travel_sec
            next_time = current_time + timedelta(seconds=travel_sec)
            neighbors_out.append((
                nbr_code, edge_cost, next_time, "walk",
                dist_km  # used for heuristic
            ))
        else:  # mode == "cost"
            # walking cost is presumably free => edge_cost=0
            dist_for_edge = dist_km
            edge_cost = 0
            neighbors_out.append((
                nbr_code, edge_cost, None, "walk", dist_for_edge
            ))

    return neighbors_out

def backward_neighbors(api, transport_graph, station_code, current_g, current_time, mode, latlon_cache):
    """
    Symmetric to forward_neighbors, but for the backward search:
    we consider inbound edges that 'arrive' at station_code in time mode.
    If none exist, we fetch from Yandex (arrivals).
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

    # 1) Transport
    for (nbr_code, _, edge_mode, route_info, dist_km, travel_sec) in edges:
        if edge_mode != "transport":
            continue
        dep_time = route_info["departure_time"]  # datetime
        arr_time = route_info["arrival_time"]    # datetime
        # We know arr_time <= current_time in the backward direction if it's valid
        if mode == "time":
            # cost is (current_time - dep_time) basically, but we must factor wait if reversing?
            # Actually simpler: arrival_time <= current_time => no extra wait
            # total travel sec:
            edge_cost = (arr_time - dep_time).total_seconds()
            # new time becomes dep_time
            next_time = dep_time
            neighbors_in.append((
                nbr_code, edge_cost, next_time, "transport",
                estimate_edge_distance_km(nbr_code, station_code, latlon_cache)
            ))
        else:  # cost mode
            dist_for_edge = estimate_edge_distance_km(nbr_code, station_code, latlon_cache)
            edge_cost = dist_for_edge * TRANSPORT_RATIO
            neighbors_in.append((
                nbr_code, edge_cost, None, "transport", dist_for_edge
            ))

    # 2) Walkable
    for (nbr_code, dist_km, edge_mode, _, dist_km_again, travel_sec) in walkable:
        if edge_mode != "walk":
            continue
        if mode == "time":
            # walking cost is travel_sec
            edge_cost = travel_sec
            next_time = current_time - timedelta(seconds=travel_sec)
            neighbors_in.append((
                nbr_code, edge_cost, next_time, "walk",
                dist_km
            ))
        else:  # cost mode
            dist_for_edge = dist_km
            edge_cost = 0
            neighbors_in.append((
                nbr_code, edge_cost, None, "walk", dist_for_edge
            ))

    return neighbors_in

def estimate_edge_distance_km(station_a, station_b, latlon_cache):
    """
    For heuristic/cost estimation if not stored. We'll compute
    haversine between station_a and station_b from latlon_cache.
    """
    if station_a not in latlon_cache or station_b not in latlon_cache:
        return 0.0
    (lat1, lon1) = latlon_cache[station_a]
    (lat2, lon2) = latlon_cache[station_b]
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
    - mode="time": minimize total travel time (seconds).
    - mode="cost": minimize cost (distance * ratio, with walk=0).
    Heuristic is always Euclidean distance (km) to the nearest goal station.
    For time mode, G-values are in seconds. For cost mode, G-values are in cost units.

    Return a path as a list of edges:
      [ (src, dst, mode, cost, [arrival_time or None]) , ... ]
    or an empty list if no path found.
    """

    if start_time is None:
        start_time = datetime.now()

    # We build a lat/lon cache for all stations so we can compute heuristics
    latlon_cache = load_latlon_cache(transport_graph)
    goal_set = set(goal_stations)
    start_set = set(start_stations)

    # For the forward side, g-values represent cost from the start.
    # For the backward side, g-values represent cost from the goal.
    forward_g = {}
    backward_g = {}
    forward_time = {}   # store the actual current datetime in 'time' mode
    backward_time = {}

    forward_parent = {}  # station -> (prev_station, edge_mode, cost_of_edge, arrival_time/None)
    backward_parent = {} # station -> (next_station, edge_mode, cost_of_edge, departure_time/None)

    # Priority queue: items are (f, station), where f = g + h
    # We'll store them in dicts: forward_open, backward_open
    forward_pq = []
    backward_pq = []

    INF = float("inf")

    # Initialize
    for s in start_stations:
        forward_g[s] = 0.0
        if mode == "time":
            forward_time[s] = start_time
        hval = heuristic_km(s, goal_set, latlon_cache)
        fval = 0.0 + hval
        heapq.heappush(forward_pq, (fval, s))

    # For backward search, we pretend we start at 'infinite future' if time-based,
    # but for cost-based we can just do 0 as well. We'll do a simpler approach:
    # We'll keep time as start_time + 48h, to allow up to 2 days window backward:
    if mode == "time":
        infinite_future = start_time + timedelta(hours=48)
        for g in goal_stations:
            backward_g[g] = 0.0
            backward_time[g] = infinite_future
            hval = heuristic_km(g, start_set, latlon_cache)  # min dist to any start
            fval = 0.0 + hval
            heapq.heappush(backward_pq, (fval, g))
    else:
        for g in goal_stations:
            backward_g[g] = 0.0
            # no need for a special "backward_time" in cost mode, but keep structure consistent
            backward_time[g] = None
            hval = heuristic_km(g, start_set, latlon_cache)
            fval = 0.0 + hval
            heapq.heappush(backward_pq, (fval, g))

    best_path_cost = INF
    meeting_station = None

    # Helper to reconstruct path once we find a meeting
    def check_meeting(station):
        nonlocal best_path_cost, meeting_station
        # If station is visited in both directions, we can see total cost
        if station in forward_g and station in backward_g:
            total_cost = forward_g[station] + backward_g[station]
            if total_cost < best_path_cost:
                best_path_cost = total_cost
                meeting_station = station

    # Expand function
    def expand_forward():
        if not forward_pq:
            return
        fval, st = heapq.heappop(forward_pq)
        # If st is stale, skip
        if st not in forward_g:
            return
        gval = forward_g[st]
        hval = heuristic_km(st, goal_set, latlon_cache)
        if (gval + hval) < fval - 1e-9:
            return  # stale

        # Possibly check for meeting
        check_meeting(st)

        cur_time = forward_time.get(st, start_time) if mode == "time" else None

        # Expand neighbors
        nbrs = forward_neighbors(api, transport_graph, st, gval, cur_time, mode, latlon_cache)
        for (nbr_code, edge_cost, arrival_time, edge_mode, dist_km) in nbrs:
            candidate_g = gval + edge_cost
            old_g = forward_g.get(nbr_code, INF)
            if candidate_g < old_g:
                forward_g[nbr_code] = candidate_g
                forward_parent[nbr_code] = (st, edge_mode, edge_cost,
                                            arrival_time if mode == "time" else None)
                if mode == "time":
                    forward_time[nbr_code] = arrival_time
                # push new f
                hval_nbr = heuristic_km(nbr_code, goal_set, latlon_cache)
                fval_nbr = candidate_g + hval_nbr
                heapq.heappush(forward_pq, (fval_nbr, nbr_code))
                # check meeting
                check_meeting(nbr_code)

    def expand_backward():
        if not backward_pq:
            return
        fval, st = heapq.heappop(backward_pq)
        # If st is stale, skip
        if st not in backward_g:
            return
        gval = backward_g[st]
        # compute heuristic to the start set
        hval = heuristic_km(st, start_set, latlon_cache)
        if (gval + hval) < fval - 1e-9:
            return  # stale

        check_meeting(st)

        cur_time = backward_time.get(st, None)

        nbrs = backward_neighbors(api, transport_graph, st, gval, cur_time, mode, latlon_cache)
        for (nbr_code, edge_cost, departure_time, edge_mode, dist_km) in nbrs:
            candidate_g = gval + edge_cost
            old_g = backward_g.get(nbr_code, INF)
            if candidate_g < old_g:
                backward_g[nbr_code] = candidate_g
                backward_parent[nbr_code] = (st, edge_mode, edge_cost,
                                             departure_time if mode == "time" else None)
                if mode == "time":
                    backward_time[nbr_code] = departure_time
                hval_nbr = heuristic_km(nbr_code, start_set, latlon_cache)
                fval_nbr = candidate_g + hval_nbr
                heapq.heappush(backward_pq, (fval_nbr, nbr_code))
                check_meeting(nbr_code)

    # Main loop
    while forward_pq and backward_pq:
        # Check if we already have a meeting station with cost < next frontier
        if meeting_station is not None:
            # Let f_min be the smallest f in either frontier
            f_fwd = forward_pq[0][0] if forward_pq else INF
            f_bwd = backward_pq[0][0] if backward_pq else INF
            f_min = min(f_fwd, f_bwd)
            if best_path_cost <= f_min:
                # We can stop
                break

        # Decide which frontier to expand: whichever has the smaller top f
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

    # We have a meeting station with best_path_cost. Reconstruct the path
    return reconstruct_bidirectional_path(meeting_station,
                                          forward_parent,
                                          backward_parent,
                                          mode)

def reconstruct_bidirectional_path(meeting_station, forward_parent, backward_parent, mode):
    """
    Build the final path of edges from start -> meeting_station -> goal.
    Each edge as: (src, dst, mode, cost, [time or None]).
    For forward_parent, forward_parent[X] = (prev, edge_mode, cost_of_edge, arrival_time?)
    For backward_parent, backward_parent[X] = (next, edge_mode, cost_of_edge, departure_time?)
    """
    # forward path
    forward_edges = []
    cur = meeting_station
    while cur in forward_parent:
        (prev_st, edge_mode, edge_cost, arr_time) = forward_parent[cur]
        forward_edges.append((prev_st, cur, edge_mode, edge_cost, arr_time))
        cur = prev_st
    forward_edges.reverse()

    # backward path
    backward_edges = []
    cur = meeting_station
    while cur in backward_parent:
        (next_st, edge_mode, edge_cost, dep_time) = backward_parent[cur]
        # That means in forward direction it's (cur -> next_st)
        backward_edges.append((cur, next_st, edge_mode, edge_cost, dep_time))
        cur = next_st
    # no need to reverse, but we want to attach it end-to-end
    # The backward_edges are from meeting_station -> goal in forward orientation,
    # but they are stored in forward direction order. So let's just do it that way:

    path = forward_edges + backward_edges
    return path

###########################################################
# Wrapping in your "search_settlements_bidirectional" call
###########################################################

def search_settlements_bidirectional(api, transport_graph,
                                     start_stations,
                                     goal_stations,
                                     start_time=None,
                                     mode="time",
                                     debug=False):
    """
    Wrapper that calls our bidirectional A* with whichever 'mode' you want:
      - mode="time" => minimize total travel time
      - mode="cost" => minimize cost (distance × ratio, walk=0)
    Returns a list of edges: 
      [ (src, dst, mode, cost, arrivalOrDepartureTime?), ... ]
    or [] if none found.
    """
    return bidirectional_a_star(api, transport_graph,
                                start_stations,
                                goal_stations,
                                start_time=start_time,
                                mode=mode,
                                debug=debug)

