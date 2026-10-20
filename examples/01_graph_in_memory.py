from array import array

import nodusdb

with nodusdb.Graph() as g:
    g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])

    print("has 1 -> 2:", g.has_edge(1, 2))
    print("out-degree of 1:", g.degree(1))
    print("in-degree of 4:", g.in_degree(4))
    print("reachable from 1 within 3 hops:", g.khop(start=1, max_depth=3))
    print("common neighbors of 3 and 5:", g.common_neighbors(3, 5))

    g.remove_edge(3, 4)
    print("after removing 3 -> 4, 4 is still reachable through 5:", g.khop(start=1, max_depth=3))

    flat = array("q", [10, 11, 11, 12, 12, 13])
    g.add_edges_from(flat)
    print("flat buffer load:", g.khop(10, 3))

with nodusdb.Graph() as names:
    names.add_edge("user:alice", "role:admin")
    names.add_edge("user:bob", "role:admin")
    print("string keys:", names.khop("user:alice", 1))
    print("common neighbors:", names.common_neighbors("user:alice", "user:bob"))
