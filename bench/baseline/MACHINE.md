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

The laptop CPU boosts and throttles. Expect variation of tens of percent between runs on this machine, and compare runs only when they were taken in similar conditions.
