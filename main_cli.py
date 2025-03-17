#!/usr/bin/env python3

import sys
import logging
from datetime import datetime
from yapi import yAPI
from lazygraph import TransportGraph
from a_star import search_settlements_bidirectional
import time
import sys
import requests

sys.stdin.reconfigure(encoding='utf-8')
def wait_for_localhost_7474():
    """
    Ping http://localhost:7474 every 2 seconds until an HTTP connection succeeds.
    
    If a connection attempt fails once, prints a "please wait" message once.
    Exits as soon as a GET request to the URL succeeds.
    """
    url = "http://neo4j:7474"
    printed_wait = False

    while True:
        try:
            # Attempt a GET request with a short timeout.
            response = requests.get(url, timeout=1)
            # If we get a response, assume the connection succeeded.
            return
        except requests.exceptions.RequestException:
            if not printed_wait:
                print("Please wait, connecting to http://neo4j:7474...")
                printed_wait = True
            time.sleep(2)
ASCII_BANNER = r"""
        ____                                   ______  ____
       / __ \____ _      _____  ________  ____/ / __ )/  _/
      / /_/ / __ \ | /| / / _ \/ ___/ _ \/ __  / __  |/ /  
     / ____/ /_/ / |/ |/ /  __/ /  /  __/ /_/ / /_/ // /   
    /_/    \____/|__/|__/\___/_/   \___/\__,_/_____/___/   
                                                       
    :: Yandex Rasp Pathfinder :: PoweredBI
"""

def get_user_input(prompt):
    return input(prompt)

def try_search_settlement(api, user_query):
    results = api.search_settlements(user_query)
    if results:
        return results
    truncated = user_query[:-1].strip()
    if len(truncated) > 1:
        results2 = api.search_settlements(truncated)
        if results2:
            logging.warning("No matches for '%s', but found matches for '%s'.", user_query, truncated)
            return results2
    return []

def pick_settlement(api, settlement_name):
    results = try_search_settlement(api, settlement_name)
    if not results:
        print(f"[!] No matches found for '{settlement_name}' (and truncated attempts).")
        return None

    print(f"\nFound {len(results)} matches for '{settlement_name}':")
    for i, r in enumerate(results, start=1):
        print(f"  {i}) {r['title']} (yandex_code={r['yandex_code']})")

    while True:
        choice = get_user_input("Choose one by number (or 'x' to skip): ")
        if choice.lower() == 'x':
            return None
        try:
            idx = int(choice)
            if 1 <= idx <= len(results):
                return results[idx - 1]
        except ValueError:
            pass
        print("Invalid input. Please try again.")

def get_station_title(transport_graph, station_code):
    with transport_graph.driver.session() as session:
        res = session.run("""
            MATCH (s:Station {yandex_code:$code})
            RETURN s.title AS t
        """, code=station_code).single()
        return res["t"] if res and res["t"] else station_code

def get_thread_title(api, thread_uid):
    data = api.thread_stops(uid=thread_uid)
    if data and "thread" in data and "thread" in data["thread"]:
        transport_type = data["thread"].get("transport_type", "")
        return data["thread"]["title"] + (transport_type if transport_type else "")
    return thread_uid

def main_cli():
    wait_for_localhost_7474()
    print(ASCII_BANNER)

    logging.basicConfig(level=logging.INFO)

    api = yAPI(cache_file="resp.json")
    transport_graph = TransportGraph(uri="bolt://neo4j:7687", user="neo4j", password="secretgraph")

    print("Welcome! Let's collect the settlements you'd like to include (e.g. departure, intermediate, destination).")
    print("Enter one settlement name at a time. Enter empty line when done.\n")

    chosen_settlements = []
    while True:
        user_in = get_user_input("Settlement name (ENTER to finish): ").strip()
        if not user_in:
            break
        settlement_info = pick_settlement(api, user_in)
        if settlement_info:
            chosen_settlements.append(settlement_info)
            print(f"Chosen: {settlement_info['title']} (code={settlement_info['yandex_code']})")
        else:
            print("Skipped.\n")

    if len(chosen_settlements) < 2:
        print("You need at least two settlements (from, to). Exiting.")
        transport_graph.close()
        sys.exit(0)

    print("\nChoose the path-finding mode:")
    print("  1) time     => Find the fastest route (earliest arrival)")
    print("  2) cost     => Find the cheapest route in terms of distance ratio")
    print("  3) basic    => Basic ignoring actual departure times (then verify schedule).")
    mode_map = {'1': 'time', '2': 'cost', '3': 'basic'}
    mode_choice = get_user_input("Enter 1,2,3 (default=3): ").strip()
    if mode_choice not in mode_map:
        mode_choice = '3'
    mode = mode_map[mode_choice]

    final_route = []
    current_departure_time = None

    for i in range(len(chosen_settlements) - 1):
        from_settlement = chosen_settlements[i]
        to_settlement   = chosen_settlements[i+1]

        if i == 0:
            default_time = datetime.now()
        else:
            default_time = current_departure_time

        print(f"\nFor leg from {from_settlement['title']} to {to_settlement['title']}:")
        print("Enter departure date/time in the format YYYY-MM-DD HH:MM (24h).")
        print("If left blank, we'll use the default:", default_time.strftime("%Y-%m-%d %H:%M"))
        date_str = get_user_input("Departure datetime (optional): ").strip()
        if not date_str:
            current_departure_time = default_time
        else:
            try:
                current_departure_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            except ValueError:
                print("Invalid format. Falling back to default time.")
                current_departure_time = default_time

        from_stations = api.get_settlement_station_codes(from_settlement['yandex_code'])
        to_stations   = api.get_settlement_station_codes(to_settlement['yandex_code'])

        if not from_stations:
            print(f"[!] No station codes found for settlement={from_settlement['yandex_code']}. Aborting.")
            transport_graph.close()
            sys.exit(0)
        if not to_stations:
            print(f"[!] No station codes found for settlement={to_settlement['yandex_code']}. Aborting.")
            transport_graph.close()
            sys.exit(0)

        print(f"\nSearching for a path from {from_settlement['title']} to {to_settlement['title']}...")
        path = search_settlements_bidirectional(
            api,
            transport_graph,
            start_stations=from_stations,
            goal_stations=to_stations,
            start_time=current_departure_time,
            mode=mode,
            debug=False
        )

        if not path:
            print(f"\nNo feasible path found from {from_settlement['title']} to {to_settlement['title']}.")
            print("Sorry! Aborting the search.")
            transport_graph.close()
            sys.exit(0)

        final_route.append(path)

        if mode == 'time':
            last_edge = path[-1]
            maybe_time = last_edge[4] if len(last_edge) >= 5 else None
            if maybe_time:
                current_departure_time = maybe_time

    print("\n\n================= YOUR MULTI-LEG ROUTE =================\n")

    thread_title_cache = {}
    def get_or_fetch_thread_title(uid):
        if uid in thread_title_cache:
            return thread_title_cache[uid]
        t = get_thread_title(api, uid)
        thread_title_cache[uid] = t
        return t

    total_cost = 0.0
    displayed_arrival_time = None

    for leg_index, path in enumerate(final_route, start=1):
        print(f"Leg #{leg_index}: {chosen_settlements[leg_index - 1]['title']} -> {chosen_settlements[leg_index]['title']}")
        print("---------------------------------------------------")

        if leg_index == 1:
            displayed_arrival_time = current_departure_time if mode == 'time' else None

        for edge in path:
            src, dst, edge_mode, cost_val, arrival_time, thread_uid = edge

            src_title = get_station_title(transport_graph, src)
            dst_title = get_station_title(transport_graph, dst)

            if edge_mode == "walk":
                ttitle = "Walk"
            else:
                if thread_uid:
                    ttitle = get_or_fetch_thread_title(thread_uid)
                else:
                    ttitle = get_thread_title(api, edge_mode)

            if mode == 'time' and arrival_time and displayed_arrival_time:
                departure_str = displayed_arrival_time.strftime("%Y-%m-%d %H:%M")
                arrival_str   = arrival_time.strftime("%Y-%m-%d %H:%M")
                print(f"  {src_title} [{departure_str}] --({ttitle})--> {dst_title} [arr {arrival_str}]")
                displayed_arrival_time = arrival_time
            else:
                print(f"  {src_title} --({ttitle})--> {dst_title}  (cost={cost_val})")

            total_cost += cost_val

        print("---------------------------------------------------\n")

    if mode == 'time' and displayed_arrival_time:
        print("Overall Estimated Arrival Time:", displayed_arrival_time.strftime("%Y-%m-%d %H:%M"))
    else:
        print(f"Total cost for this route = {total_cost:.3f}")

    transport_graph.close()

if __name__ == "__main__":
    main_cli()

