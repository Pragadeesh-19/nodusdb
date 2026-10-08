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

The laptop CPU boosts and throttles. Expect variation of tens of percent between runs on this machine, and compare runs only when they were taken in similar conditions.
