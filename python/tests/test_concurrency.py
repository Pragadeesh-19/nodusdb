import random
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph  # noqa: E402

TARGETS = 500
SHARED = TARGETS + 1
ROUNDS = 200


def _hub_graph():
    graph = Graph()
    for target in range(1, TARGETS + 1):
        graph.add_edge(0, target)
        graph.add_edge(SHARED, target)
    return graph


def _run(workers):
    threads = [threading.Thread(target=worker) for worker in workers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    return [thread.is_alive() for thread in threads]


class ConcurrentUseTest(unittest.TestCase):
    def test_threads_with_their_own_graphs_get_correct_answers(self):
        errors = []

        def worker():
            try:
                graph = _hub_graph()
                for _ in range(ROUNDS):
                    if graph.degree(0) != TARGETS:
                        raise AssertionError("degree changed under a private graph")
                    if set(graph.khop(0, 1)) != set(range(1, TARGETS + 1)):
                        raise AssertionError("khop returned the wrong neighbours")
                    if set(graph.common_neighbors(0, SHARED)) != set(range(1, TARGETS + 1)):
                        raise AssertionError("common_neighbors returned the wrong set")
                graph.close()
            except Exception as error:  # reported below, on the test thread
                errors.append(error)

        stuck = _run([worker for _ in range(8)])
        self.assertFalse(any(stuck), "a worker thread did not finish")
        self.assertEqual([], errors)

    def test_one_shared_graph_serves_readers_while_another_thread_writes(self):
        graph = _hub_graph()
        errors = []
        writing = threading.Event()
        writing.set()

        def writer():
            try:
                rng = random.Random(7)
                present = set(range(1, TARGETS + 1))
                for _ in range(ROUNDS * 5):
                    target = rng.randint(1, TARGETS)
                    if target in present:
                        graph.remove_edge(0, target)
                        present.discard(target)
                    else:
                        graph.add_edge(0, target)
                        present.add(target)
            except Exception as error:
                errors.append(error)
            finally:
                writing.clear()

        def reader():
            try:
                while writing.is_set():
                    for result in (graph.khop(0, 1), graph.common_neighbors(0, SHARED)):
                        if len(result) != len(set(result)):
                            raise AssertionError(f"duplicate target in a result: {result}")
                        if any(not 1 <= value <= TARGETS for value in result):
                            raise AssertionError(f"target out of range in a result: {result}")
            except Exception as error:
                errors.append(error)

        stuck = _run([writer] + [reader for _ in range(6)])
        self.assertFalse(any(stuck), "a thread did not finish")
        self.assertEqual([], errors)
        graph.close()


if __name__ == "__main__":
    unittest.main()
