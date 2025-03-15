
import a_star
from lazygraph import TransportGraph
from yapi import yAPI
from datetime import datetime, timezone, timedelta


def main():
    # neo_graph = Neo4jRouteGraph(
    #     uri="bolt://localhost:7687",
    #     user="neo4j",
    #     password="secretgraph",
    #     walk_distance_km=1.0,
    #     cost_mode="time",
    #     debug=False
    # )

    transport_graph = TransportGraph(uri="bolt://localhost:7687", user="neo4j", password="secretgraph")


    api = yAPI(cache_file="resp.json")
    # api.populate_neo4j(transport_graph)
    # return
    from_settlement_code = "c66"   # Example: Moscow region?
    to_settlement_code   = "c65"  # Example: Rostov?

    from_stations = api.get_settlement_station_codes(from_settlement_code)
    to_stations   = api.get_settlement_station_codes(to_settlement_code)

    station_infos = api.fetch_station_info(set(from_stations + to_stations))
    transport_graph.add_stations_bulk(station_infos)

    # input()
    print("From stations:", len(from_stations))
    print("To stations:", len(to_stations))

    start_time= datetime.now() + timedelta(hours = 3)

    path = a_star.time_table_search_settlements(
        api,
        transport_graph,
        start_stations=from_stations,
        goal_stations=to_stations,
        start_time=start_time,
        debug=True
    )

    if not path:
        print("No path found.")
    else:
        for (src, dst, mode, route_info, arrival_dt) in path:
            title_src = get_station_title(transport_graph, src)
            title_dst = get_station_title(transport_graph, dst)
            print(f"{mode.upper()} from {title_src} -> {title_dst}, arrive {arrival_dt}")

    # neo_graph.close()

def get_station_title(neo_graph, station_code):
    with neo_graph.driver.session() as session:
        res = session.run("""
            MATCH (s:Station {yandex_code:$code}) RETURN s.title AS t
        """, code=station_code).single()
        return res["t"] if res else station_code

if __name__ == "__main__":
    main()

