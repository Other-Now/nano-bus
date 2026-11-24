| GC | rate/s | achieved/s | p50 us | p99 us | p99.9 us | max ms | broker B/msg | GCs | longest GC pause ms | GC pause total ms | alloc stalls (longest ms) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| G1 | 20000 | 20000 | 453 | 2234 | 4751 | 7.7 | 0.11 | 0 | 0.00 | 0.0 | 0 (0.0) |
| G1 | 100000 | 100000 (saturated) | 47743 | 302514 | 323748 | 331.6 | 0.11 | 0 | 0.00 | 0.0 | 0 (0.0) |

Machine floor (4 spinning threads, 4 CPUs, idle otherwise): p99 5 us, p99.9 964 us, max 3.0 ms
