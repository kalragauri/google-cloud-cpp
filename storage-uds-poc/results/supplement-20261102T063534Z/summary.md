# UDS POC results

Source: `results/supplement-20261102T063534Z`. Latency ms (p50/p90/p99); CPU and allocations are per-row medians per byte transferred (app + sidecar); ratios are impl ÷ baseline within one cell.

## Regional · JSON · WriteObject

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| SINGLE-SHOT | 100000000 | uds | 50 | 0 | 912.54 | 1083.97 | 2203.42 | 105.3 | 2.48 (0.58) | 4.55 | 1.14× | 2.18× |
| SINGLE-SHOT | 100000000 | native-json-h1 | 50 | 0 | 802.62 | 977.51 | 1940.06 | 118.9 | 1.14 (1.14) | 0.02 |  |  |
| SINGLE-SHOT | 100000000 | pure-rust | 50 | 0 | 848.68 | 985.43 | 1128.65 | 112.7 | 0.74 (0.74) | 1.00 | 1.06× | 0.65× |
| SINGLE-SHOT | 100000000 | pure-rust-unbuffered | 50 | 0 | 804.75 | 983.09 | 1076.53 | 118.6 | 0.86 (0.86) | 1.00 | 1.00× | 0.75× |
| RESUMABLE | 100000000 | uds | 50 | 0 | 1226.71 | 1387.61 | 1599.93 | 77.8 | 2.06 (0.46) | 4.44 | 1.40× | 1.82× |
| RESUMABLE | 100000000 | native-json-h1 | 50 | 0 | 877.61 | 1044.20 | 2005.12 | 108.8 | 1.13 (1.13) | 0.04 |  |  |
| RESUMABLE | 100000000 | pure-rust | 50 | 0 | 1194.63 | 1283.15 | 1407.88 | 79.9 | 0.78 (0.78) | 1.00 | 1.36× | 0.69× |
| RESUMABLE | 100000000 | pure-rust-unbuffered | 50 | 0 | 822.14 | 1047.35 | 1375.14 | 116.1 | 0.88 (0.88) | 1.00 | 0.94× | 0.77× |

## Regional · JSON · ReadObject

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| READ[0] | 100000000 | uds | 100 | 0 | 344.56 | 412.50 | 824.64 | 277.5 | 2.52 (0.56) | 4.06 | 0.95× | 1.70× |
| READ[0] | 100000000 | native-json-h1 | 100 | 0 | 363.66 | 439.11 | 817.49 | 262.3 | 1.48 (1.48) | 0.03 |  |  |
| READ[0] | 100000000 | pure-rust | 100 | 0 | 363.02 | 429.47 | 479.00 | 262.8 | 2.23 (2.23) | 3.80 | 1.00× | 1.51× |
| READ[0] | 100000000 | pure-rust-unbuffered | 100 | 0 | 358.69 | 445.36 | 515.03 | 266.5 | 2.44 (2.44) | 3.59 | 0.99× | 1.65× |
| READ[1] | 100000000 | uds | 100 | 0 | 321.00 | 492.30 | 746.37 | 297.7 | 2.45 (0.53) | 4.07 | 0.96× | 1.67× |
| READ[1] | 100000000 | native-json-h1 | 100 | 0 | 334.49 | 397.73 | 473.17 | 285.7 | 1.46 (1.46) | 0.03 |  |  |
| READ[1] | 100000000 | pure-rust | 100 | 0 | 337.86 | 408.66 | 473.27 | 282.3 | 2.22 (2.22) | 3.79 | 1.01× | 1.51× |
| READ[1] | 100000000 | pure-rust-unbuffered | 100 | 0 | 325.62 | 402.86 | 473.32 | 293.0 | 2.43 (2.43) | 3.58 | 0.97× | 1.66× |
| READ[2] | 100000000 | uds | 100 | 0 | 307.93 | 387.68 | 446.88 | 310.1 | 2.45 (0.53) | 4.07 | 0.91× | 1.66× |
| READ[2] | 100000000 | native-json-h1 | 100 | 0 | 337.27 | 425.06 | 547.18 | 282.8 | 1.48 (1.48) | 0.03 |  |  |
| READ[2] | 100000000 | pure-rust | 100 | 0 | 351.86 | 397.81 | 473.19 | 271.9 | 2.23 (2.23) | 3.75 | 1.04× | 1.51× |
| READ[2] | 100000000 | pure-rust-unbuffered | 100 | 0 | 322.26 | 399.75 | 506.66 | 296.1 | 2.41 (2.41) | 3.61 | 0.96× | 1.63× |

## Throughput (concurrent 100 MB transfers)

| run | impl | MiB/s | CPU ms app | CPU ms sidecar | peak RSS MiB app | peak RSS MiB sidecar | ok |
|---|---|--:|--:|--:|--:|--:|--:|
| tp-bidi-read-w32-native | native-grpc-cfe | 414.5 | 63290 | 0 | 248 | 0 | 1 |
| tp-bidi-read-w32-rust | pure-rust | 907.1 | 46880 | 0 | 354 | 0 | 1 |
| tp-bidi-read-w32-uds-ch4 | uds-ch4 | 1269.2 | 13507 | 65273 | 256 | 422 | 1 |
| tp-bidi-read-w32-uds | uds | 956.9 | 12413 | 64757 | 312 | 348 | 1 |
| tp-bidi-read-w8-native | native-grpc-cfe | 991.1 | 15545 | 0 | 189 | 0 | 1 |
| tp-bidi-read-w8-rust | pure-rust | 928.0 | 11283 | 0 | 213 | 0 | 1 |
| tp-bidi-read-w8-uds-ch4 | uds-ch4 | 815.1 | 3210 | 14367 | 187 | 158 | 1 |
| tp-bidi-read-w8-uds | uds | 876.6 | 3064 | 14050 | 234 | 160 | 1 |
| tp-bidi-write-w32-native | native-grpc-cfe | 1700.5 | 25830 | 0 | 167 | 0 | 1 |
| tp-bidi-write-w32-rust | pure-rust | 985.6 | 26864 | 0 | 1019 | 0 | 1 |
| tp-bidi-write-w32-uds-ch4 | uds-ch4 | 1240.5 | 10022 | 52468 | 124 | 2627 | 1 |
| tp-bidi-write-w32-uds | uds | 901.3 | 8818 | 47991 | 131 | 1983 | 1 |
| tp-bidi-write-w8-native | native-grpc-cfe | 1312.1 | 6501 | 0 | 150 | 0 | 1 |
| tp-bidi-write-w8-rust | pure-rust | 988.3 | 6563 | 0 | 268 | 0 | 1 |
| tp-bidi-write-w8-uds-ch4 | uds-ch4 | 937.9 | 2268 | 12572 | 116 | 1073 | 1 |
| tp-bidi-write-w8-uds | uds | 967.9 | 2095 | 10865 | 132 | 618 | 1 |
| tp-json-read-w32-native-json-h1 | native-json-h1 | 4786.7 | 30928 | 0 | 245 | 0 | 1 |
| tp-json-read-w32-rust | pure-rust | 5321.7 | 21520 | 0 | 177 | 0 | 1 |
| tp-json-read-w32-uds-ch4 | uds-ch4 | 3548.6 | 21288 | 59887 | 183 | 781 | 1 |
| tp-json-read-w32-uds | uds | 1593.6 | 14141 | 42831 | 218 | 1243 | 1 |
| tp-json-read-w8-native-json-h1 | native-json-h1 | 1689.7 | 6557 | 0 | 149 | 0 | 1 |
| tp-json-read-w8-rust | pure-rust | 1653.0 | 6521 | 0 | 128 | 0 | 1 |
| tp-json-read-w8-uds-ch4 | uds-ch4 | 1714.9 | 3332 | 10590 | 165 | 91 | 1 |
| tp-json-read-w8-uds | uds | 1634.8 | 3158 | 10784 | 187 | 209 | 1 |
| tp-json-write-w32-native-json-h1 | native-json-h1 | 2146.0 | 26098 | 0 | 216 | 0 | 1 |
| tp-json-write-w32-rust-unbuffered | pure-rust-unbuffered | 2770.0 | 15193 | 0 | 132 | 0 | 1 |
| tp-json-write-w32-rust | pure-rust | 2332.9 | 15144 | 0 | 131 | 0 | 1 |
| tp-json-write-w32-uds-ch4 | uds-ch4 | 1687.5 | 8638 | 30877 | 134 | 966 | 1 |
| tp-json-write-w32-uds | uds | 660.7 | 7695 | 28040 | 146 | 569 | 1 |
| tp-json-write-w8-native-json-h1 | native-json-h1 | 774.2 | 5714 | 0 | 140 | 0 | 1 |
| tp-json-write-w8-rust-unbuffered | pure-rust-unbuffered | 736.3 | 3582 | 0 | 119 | 0 | 1 |
| tp-json-write-w8-rust | pure-rust | 561.9 | 3604 | 0 | 117 | 0 | 1 |
| tp-json-write-w8-uds-ch4 | uds-ch4 | 577.1 | 2075 | 8042 | 117 | 391 | 1 |
| tp-json-write-w8-uds | uds | 421.2 | 1946 | 7194 | 130 | 223 | 1 |

## Whole-run totals (peak RSS)

| run | impl | wall s | CPU s app | CPU s sidecar | peak RSS MiB app | peak RSS MiB sidecar |
|---|---|--:|--:|--:|--:|--:|
| tp-bidi-read-w32-native | native-grpc-cfe | 37.2 | 63.5 | 0.0 | 248 | 0 |
| tp-bidi-read-w32-rust | pure-rust | 17.3 | 47.1 | 0.0 | 354 | 0 |
| tp-bidi-read-w32-uds-ch4 | uds-ch4 | 12.4 | 13.6 | 65.6 | 256 | 422 |
| tp-bidi-read-w32-uds | uds | 16.4 | 12.5 | 65.1 | 312 | 348 |
| tp-bidi-read-w8-native | native-grpc-cfe | 4.3 | 15.7 | 0.0 | 189 | 0 |
| tp-bidi-read-w8-rust | pure-rust | 4.6 | 11.5 | 0.0 | 213 | 0 |
| tp-bidi-read-w8-uds-ch4 | uds-ch4 | 5.2 | 3.3 | 14.7 | 187 | 158 |
| tp-bidi-read-w8-uds | uds | 4.8 | 3.1 | 14.4 | 234 | 160 |
| tp-bidi-write-w32-native | native-grpc-cfe | 9.4 | 25.9 | 0.0 | 167 | 0 |
| tp-bidi-write-w32-rust | pure-rust | 15.9 | 26.9 | 0.0 | 1019 | 0 |
| tp-bidi-write-w32-uds-ch4 | uds-ch4 | 12.8 | 10.0 | 52.5 | 124 | 2627 |
| tp-bidi-write-w32-uds | uds | 17.4 | 8.8 | 48.0 | 131 | 1983 |
| tp-bidi-write-w8-native | native-grpc-cfe | 3.1 | 6.5 | 0.0 | 153 | 0 |
| tp-bidi-write-w8-rust | pure-rust | 4.0 | 6.6 | 0.0 | 268 | 0 |
| tp-bidi-write-w8-uds-ch4 | uds-ch4 | 4.2 | 2.3 | 12.6 | 116 | 1073 |
| tp-bidi-write-w8-uds | uds | 4.1 | 2.1 | 10.9 | 134 | 618 |
| tp-json-read-w32-native-json-h1 | native-json-h1 | 4.2 | 31.1 | 0.0 | 245 | 0 |
| tp-json-read-w32-rust | pure-rust | 4.2 | 21.6 | 0.0 | 177 | 0 |
| tp-json-read-w32-uds-ch4 | uds-ch4 | 5.6 | 21.3 | 60.1 | 183 | 781 |
| tp-json-read-w32-uds | uds | 10.9 | 14.2 | 43.1 | 218 | 1243 |
| tp-json-read-w8-native-json-h1 | native-json-h1 | 3.4 | 6.7 | 0.0 | 149 | 0 |
| tp-json-read-w8-rust | pure-rust | 3.6 | 6.6 | 0.0 | 128 | 0 |
| tp-json-read-w8-uds-ch4 | uds-ch4 | 3.6 | 3.4 | 10.8 | 165 | 91 |
| tp-json-read-w8-uds | uds | 3.7 | 3.2 | 11.0 | 187 | 209 |
| tp-json-write-w32-native-json-h1 | native-json-h1 | 7.5 | 26.1 | 0.0 | 216 | 0 |
| tp-json-write-w32-rust-unbuffered | pure-rust-unbuffered | 6.0 | 15.2 | 0.0 | 132 | 0 |
| tp-json-write-w32-rust | pure-rust | 7.0 | 15.2 | 0.0 | 131 | 0 |
| tp-json-write-w32-uds-ch4 | uds-ch4 | 9.5 | 8.7 | 30.9 | 134 | 966 |
| tp-json-write-w32-uds | uds | 23.6 | 7.7 | 28.1 | 146 | 569 |
| tp-json-write-w8-native-json-h1 | native-json-h1 | 5.1 | 5.9 | 0.0 | 151 | 0 |
| tp-json-write-w8-rust-unbuffered | pure-rust-unbuffered | 5.4 | 3.6 | 0.0 | 119 | 0 |
| tp-json-write-w8-rust | pure-rust | 6.9 | 3.6 | 0.0 | 117 | 0 |
| tp-json-write-w8-uds-ch4 | uds-ch4 | 6.8 | 2.1 | 8.1 | 121 | 391 |
| tp-json-write-w8-uds | uds | 9.2 | 2.0 | 7.2 | 132 | 223 |
| w1r3-100000000-native-json-h1 | native-json-h1 | 199.9 | 56.5 | 0.0 | 121 | 0 |
| w1r3-100000000-rust-unbuffered | pure-rust-unbuffered | 193.4 | 82.8 | 0.0 | 120 | 0 |
| w1r3-100000000-rust | pure-rust | 215.2 | 74.1 | 0.0 | 119 | 0 |
| w1r3-100000000-uds | uds | 221.1 | 22.3 | 76.5 | 162 | 316 |

## Gaps and errors observed

