# Baseline machine: windows-dev

Results in `windows-dev.json` were produced on this machine. Numbers from other hardware are not comparable.

| Item | Value |
|---|---|
| CPU | AMD Ryzen 7 5700U with Radeon Graphics |
| Cores / threads | 8 / 16 |
| Advertised clock | 1.8 GHz base (boost not disabled) |
| Memory | 7.3 GB |
| Operating system | Windows 11 Home Single Language, build 10.0.26200 |
| JDK | Oracle HotSpot 22.0.2+9-70 |
| JMH | 1.37 |

Command used:

```
java -Dfile.encoding=UTF-8 -jar bench/target/benchmarks.jar -wi 3 -i 5 -f 1 -prof gc -rf json -rff bench/baseline/windows-dev.json
```

Parameters: 3 warmup iterations, 5 measurement iterations, one fork, 10 seconds per iteration. The `-prof gc` profiler adds the allocation metrics. The benchmark jar was rebuilt from the committed sources before the run.

`shipping-windows-dev.json` holds `ShippingGateBench`, the cost the shipping gate adds to one log append (two reads of the shipper state, no I/O), taken on the same machine with:

```
java -Dfile.encoding=UTF-8 -jar bench/target/benchmarks.jar ShippingGateBench -wi 3 -i 5 -f 1 -prof gc -rf json -rff bench/baseline/shipping-windows-dev.json
```

It measured 56.0 ns per append through the gate against 18.9 ns without it, with no allocation and no collection in either. That is about 37 ns on a local write that costs about 42,000 ns through Python. The end-to-end check is `python/benchmarks/shipping_ratio.py`, which measured 1.01.

`reachability-windows-dev.json` holds `ReachabilityBench` and `CheckModelBench` for Build Step 4, taken on the same machine with:

```
java -jar bench/target/benchmarks.jar "CheckModelBench|ReachabilityBench" -f 2 -wi 3 -w 2s -i 5 -r 2s -prof gc -rf json -rff bench/baseline/reachability-windows-dev.json
```

`indexed=true` runs with the reachability index and `indexed=false` with it turned off, through `TupleStore`, per operation:

| Benchmark | Index on | Index off |
|---|---:|---:|
| Grant through 30 nested groups | 664 ns | 2,537 ns |
| Deny through 30 nested groups | 649 ns | 3,492 ns |
| Grant in a tree of 1,111 groups | 711 ns | 110,243 ns |
| Deny in a tree of 1,111 groups | 681 ns | 115,877 ns |
| Nest and unnest a leaf group (two writes) | 7,716 ns | 2,191 ns |
| Add and remove a direct member (two writes) | 1,996 ns | 1,866 ns |

`CheckModelBench` measured 384, 526 and 1,687 ns against 410, 552 and 1,781 ns in `follower-windows-dev.json`, so the index adds nothing measurable to checks that do not go through nested groups. Every check allocates the same bytes with the index on or off; that allocation is the string handling in `TupleStore.check`.

`release-windows-dev.txt` holds the measurements behind the choice of the wheel toolchain (GraalVM CE 22.0.2, CE 25.0.2 and Oracle GraalVM 25 on the same code), the comparison of the code behind the first README table with the release code, the table in the README, and `GraphTraversalBench`. The commands are in the file.

The laptop CPU boosts and throttles. Expect variation of tens of percent between runs on this machine, and compare runs only when they were taken in similar conditions.
