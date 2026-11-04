# UDS POC results

Source: `results/full-20261030T090455Z`. Latency ms (p50/p90/p99); CPU and allocations are per-row medians per byte transferred (app + sidecar); ratios are impl ÷ baseline within one cell.

## Regional · JSON · WriteObject

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| SINGLE-SHOT | 100000 | uds | 100 | 0 | 59.89 | 70.67 | 232.22 | 1.6 | 10.29 (4.34) | 6.27 | 1.03× | 1.75× |
| SINGLE-SHOT | 100000 | native-json-h1 | 100 | 0 | 58.21 | 69.92 | 269.11 | 1.6 | 5.88 (5.88) | 11.16 |  |  |
| SINGLE-SHOT | 100000 | pure-rust | 100 | 0 | 55.24 | 70.25 | 127.25 | 1.7 | 3.74 (3.74) | 1.34 | 0.95× | 0.64× |
| SINGLE-SHOT | 100000 | native-json-default *(context)* | 100 | 0 | 55.80 | 63.90 | 72.59 | 1.7 | 5.96 (5.96) | 11.15 | 0.96× | 1.01× |
| SINGLE-SHOT | 100000 | native-grpc-cfe *(context)* | 100 | 0 | 59.05 | 70.34 | 87.02 | 1.6 | 8.66 (8.66) | 3.63 | 1.01× | 1.47× |
| SINGLE-SHOT | 2097152 | uds | 100 | 0 | 71.84 | 78.18 | 81.90 | 27.8 | 2.26 (0.66) | 4.62 | 1.03× | 1.51× |
| SINGLE-SHOT | 2097152 | native-json-h1 | 100 | 0 | 69.71 | 77.89 | 84.22 | 28.7 | 1.49 (1.49) | 0.54 |  |  |
| SINGLE-SHOT | 2097152 | pure-rust | 100 | 0 | 69.80 | 80.17 | 94.90 | 28.7 | 1.03 (1.03) | 1.02 | 1.00× | 0.69× |
| SINGLE-SHOT | 2097152 | native-json-default *(context)* | 100 | 0 | 69.84 | 78.45 | 92.47 | 28.6 | 1.53 (1.53) | 0.54 | 1.00× | 1.02× |
| SINGLE-SHOT | 2097152 | native-grpc-cfe *(context)* | 100 | 0 | 82.16 | 93.12 | 103.64 | 24.4 | 2.12 (2.12) | 2.56 | 1.18× | 1.42× |
| SINGLE-SHOT | 100000000 | uds | 100 | 0 | 913.33 | 1146.77 | 1365.46 | 104.4 | 2.52 (0.64) | 4.54 | 1.09× | 2.28× |
| SINGLE-SHOT | 100000000 | native-json-h1 | 100 | 0 | 839.15 | 1030.84 | 1322.66 | 113.8 | 1.11 (1.11) | 0.02 |  |  |
| SINGLE-SHOT | 100000000 | pure-rust | 100 | 0 | 833.39 | 1020.43 | 1532.80 | 114.4 | 0.75 (0.75) | 1.00 | 0.99× | 0.68× |
| SINGLE-SHOT | 100000000 | native-json-default *(context)* | 100 | 0 | 871.11 | 1054.70 | 1541.95 | 109.6 | 1.10 (1.10) | 0.02 | 1.04× | 0.99× |
| SINGLE-SHOT | 100000000 | native-grpc-cfe *(context)* | 100 | 0 | 812.61 | 1035.76 | 1340.58 | 117.5 | 1.76 (1.76) | 2.54 | 0.97× | 1.59× |
| RESUMABLE | 100000 | uds | 100 | 0 | 105.72 | 125.44 | 334.61 | 0.9 | 13.00 (5.81) | 6.48 | 1.01× | 1.40× |
| RESUMABLE | 100000 | native-json-h1 | 100 | 0 | 104.46 | 119.65 | 281.50 | 0.9 | 9.30 (9.30) | 25.00 |  |  |
| RESUMABLE | 100000 | pure-rust | 100 | 0 | 103.03 | 117.85 | 140.36 | 0.9 | 4.89 (4.89) | 1.55 | 0.99× | 0.53× |
| RESUMABLE | 100000 | native-json-default *(context)* | 100 | 0 | 102.90 | 113.10 | 129.63 | 0.9 | 9.51 (9.51) | 25.00 | 0.99× | 1.02× |
| RESUMABLE | 100000 | native-grpc-cfe *(context)* | 100 | 0 | 113.76 | 126.30 | 213.04 | 0.8 | 14.57 (14.57) | 6.78 | 1.09× | 1.57× |
| RESUMABLE | 2097152 | uds | 100 | 0 | 119.77 | 130.87 | 162.78 | 16.7 | 2.46 (0.74) | 4.63 | 1.00× | 1.51× |
| RESUMABLE | 2097152 | native-json-h1 | 100 | 0 | 119.39 | 131.15 | 208.01 | 16.8 | 1.63 (1.63) | 2.20 |  |  |
| RESUMABLE | 2097152 | pure-rust | 100 | 0 | 119.23 | 130.50 | 137.45 | 16.8 | 1.09 (1.09) | 1.03 | 1.00× | 0.67× |
| RESUMABLE | 2097152 | native-json-default *(context)* | 100 | 0 | 117.59 | 133.82 | 149.56 | 17.0 | 1.66 (1.66) | 2.20 | 0.98× | 1.02× |
| RESUMABLE | 2097152 | native-grpc-cfe *(context)* | 100 | 0 | 134.72 | 147.68 | 166.66 | 14.9 | 3.33 (3.33) | 3.71 | 1.13× | 2.04× |
| RESUMABLE | 100000000 | uds | 100 | 0 | 1245.59 | 1331.47 | 1399.81 | 76.6 | 2.00 (0.44) | 4.44 | 1.40× | 1.79× |
| RESUMABLE | 100000000 | native-json-h1 | 100 | 0 | 891.67 | 1038.52 | 1152.26 | 107.0 | 1.12 (1.12) | 0.04 |  |  |
| RESUMABLE | 100000000 | pure-rust | 100 | 0 | 1192.88 | 1275.10 | 1314.81 | 80.2 | 0.82 (0.82) | 1.00 | 1.34× | 0.74× |
| RESUMABLE | 100000000 | native-json-default *(context)* | 100 | 0 | 906.52 | 1073.72 | 1539.67 | 105.5 | 1.10 (1.10) | 0.04 | 1.02× | 0.99× |
| RESUMABLE | 100000000 | native-grpc-cfe *(context)* | 100 | 0 | 886.16 | 1115.99 | 1274.04 | 107.7 | 1.74 (1.74) | 2.54 | 0.99× | 1.56× |

## Regional · JSON · ReadObject

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| READ[0] | 100000 | uds | 200 | 0 | 33.70 | 42.69 | 57.48 | 2.8 | 11.58 (4.14) | 8.97 | 1.01× | 1.65× |
| READ[0] | 100000 | native-json-h1 | 200 | 0 | 33.21 | 42.10 | 310.74 | 2.9 | 7.01 (7.01) | 23.04 |  |  |
| READ[0] | 100000 | pure-rust | 200 | 0 | 31.77 | 41.10 | 172.75 | 3.0 | 5.23 (5.23) | 4.17 | 0.96× | 0.75× |
| READ[0] | 100000 | native-json-default *(context)* | 200 | 0 | 33.16 | 40.45 | 61.41 | 2.9 | 7.37 (7.37) | 22.95 | 1.00× | 1.05× |
| READ[0] | 100000 | native-grpc-cfe *(context)* | 200 | 0 | 37.04 | 47.25 | 88.92 | 2.6 | 13.05 (13.05) | 25.28 | 1.12× | 1.86× |
| READ[0] | 2097152 | uds | 200 | 0 | 44.20 | 52.52 | 62.15 | 45.3 | 3.60 (0.86) | 5.84 | 1.04× | 2.06× |
| READ[0] | 2097152 | native-json-h1 | 200 | 0 | 42.57 | 50.65 | 67.12 | 47.0 | 1.75 (1.75) | 1.10 |  |  |
| READ[0] | 2097152 | pure-rust | 200 | 0 | 42.07 | 49.24 | 60.15 | 47.5 | 2.41 (2.41) | 3.86 | 0.99× | 1.38× |
| READ[0] | 2097152 | native-json-default *(context)* | 200 | 0 | 43.30 | 53.09 | 69.72 | 46.2 | 1.97 (1.97) | 1.10 | 1.02× | 1.13× |
| READ[0] | 2097152 | native-grpc-cfe *(context)* | 200 | 0 | 51.30 | 60.48 | 71.56 | 39.0 | 4.70 (4.70) | 3.21 | 1.21× | 2.68× |
| READ[0] | 100000000 | uds | 200 | 0 | 386.39 | 461.17 | 560.91 | 247.1 | 2.54 (0.57) | 3.97 | 1.00× | 1.61× |
| READ[0] | 100000000 | native-json-h1 | 200 | 0 | 385.79 | 508.33 | 586.67 | 247.6 | 1.58 (1.58) | 0.03 |  |  |
| READ[0] | 100000000 | pure-rust | 200 | 0 | 425.13 | 510.03 | 662.23 | 224.4 | 2.54 (2.54) | 3.91 | 1.10× | 1.61× |
| READ[0] | 100000000 | native-json-default *(context)* | 200 | 0 | 399.23 | 490.80 | 680.33 | 239.1 | 1.55 (1.55) | 0.03 | 1.03× | 0.98× |
| READ[0] | 100000000 | native-grpc-cfe *(context)* | 200 | 0 | 489.40 | 596.43 | 764.07 | 194.9 | 4.27 (4.27) | 2.19 | 1.27× | 2.70× |
| READ[1] | 100000 | uds | 200 | 0 | 27.51 | 37.49 | 54.93 | 3.5 | 11.04 (4.11) | 9.78 | 1.03× | 2.02× |
| READ[1] | 100000 | native-json-h1 | 200 | 0 | 26.60 | 37.27 | 49.69 | 3.6 | 5.46 (5.46) | 22.96 |  |  |
| READ[1] | 100000 | pure-rust | 200 | 0 | 25.13 | 35.09 | 77.01 | 3.8 | 4.96 (4.96) | 4.31 | 0.94× | 0.91× |
| READ[1] | 100000 | native-json-default *(context)* | 200 | 0 | 25.28 | 33.73 | 49.43 | 3.8 | 6.34 (6.34) | 22.95 | 0.95× | 1.16× |
| READ[1] | 100000 | native-grpc-cfe *(context)* | 200 | 0 | 29.52 | 40.05 | 64.86 | 3.2 | 11.00 (11.00) | 25.28 | 1.11× | 2.01× |
| READ[1] | 2097152 | uds | 200 | 0 | 33.55 | 43.33 | 56.32 | 59.7 | 3.47 (0.81) | 5.71 | 0.99× | 2.02× |
| READ[1] | 2097152 | native-json-h1 | 200 | 0 | 33.91 | 44.93 | 56.62 | 59.0 | 1.72 (1.72) | 1.10 |  |  |
| READ[1] | 2097152 | pure-rust | 200 | 0 | 33.34 | 44.24 | 57.20 | 60.1 | 2.38 (2.38) | 3.89 | 0.98× | 1.38× |
| READ[1] | 2097152 | native-json-default *(context)* | 200 | 0 | 35.86 | 44.73 | 58.06 | 55.8 | 1.89 (1.89) | 1.10 | 1.06× | 1.10× |
| READ[1] | 2097152 | native-grpc-cfe *(context)* | 200 | 0 | 41.79 | 52.74 | 78.35 | 48.1 | 4.47 (4.47) | 3.22 | 1.23× | 2.60× |
| READ[1] | 100000000 | uds | 200 | 0 | 360.66 | 459.45 | 597.16 | 264.7 | 2.48 (0.56) | 3.99 | 0.97× | 1.59× |
| READ[1] | 100000000 | native-json-h1 | 200 | 0 | 371.18 | 483.49 | 567.18 | 257.3 | 1.56 (1.56) | 0.03 |  |  |
| READ[1] | 100000000 | pure-rust | 200 | 0 | 414.53 | 494.63 | 581.62 | 230.2 | 2.53 (2.53) | 3.90 | 1.12× | 1.62× |
| READ[1] | 100000000 | native-json-default *(context)* | 200 | 0 | 374.61 | 466.81 | 595.95 | 254.6 | 1.54 (1.54) | 0.03 | 1.01× | 0.99× |
| READ[1] | 100000000 | native-grpc-cfe *(context)* | 200 | 0 | 450.65 | 505.39 | 602.06 | 211.6 | 3.97 (3.97) | 2.18 | 1.21× | 2.54× |
| READ[2] | 100000 | uds | 200 | 0 | 26.95 | 37.17 | 61.18 | 3.6 | 10.88 (4.10) | 9.12 | 1.06× | 2.04× |
| READ[2] | 100000 | native-json-h1 | 200 | 0 | 25.42 | 33.83 | 55.25 | 3.8 | 5.32 (5.32) | 22.96 |  |  |
| READ[2] | 100000 | pure-rust | 200 | 0 | 24.05 | 33.28 | 54.59 | 4.0 | 4.95 (4.95) | 4.41 | 0.95× | 0.93× |
| READ[2] | 100000 | native-json-default *(context)* | 200 | 0 | 24.38 | 33.27 | 47.27 | 3.9 | 6.00 (6.00) | 22.95 | 0.96× | 1.13× |
| READ[2] | 100000 | native-grpc-cfe *(context)* | 200 | 0 | 28.71 | 39.67 | 66.49 | 3.3 | 10.86 (10.86) | 25.28 | 1.13× | 2.04× |
| READ[2] | 2097152 | uds | 200 | 0 | 32.68 | 41.24 | 48.98 | 61.2 | 3.39 (0.80) | 5.80 | 0.97× | 1.99× |
| READ[2] | 2097152 | native-json-h1 | 200 | 0 | 33.87 | 41.93 | 52.46 | 59.1 | 1.70 (1.70) | 1.10 |  |  |
| READ[2] | 2097152 | pure-rust | 200 | 0 | 33.04 | 41.89 | 54.32 | 60.6 | 2.37 (2.37) | 3.92 | 0.98× | 1.39× |
| READ[2] | 2097152 | native-json-default *(context)* | 200 | 0 | 34.52 | 45.28 | 55.59 | 58.0 | 1.88 (1.88) | 1.10 | 1.02× | 1.10× |
| READ[2] | 2097152 | native-grpc-cfe *(context)* | 200 | 0 | 41.30 | 52.80 | 67.56 | 48.4 | 4.45 (4.45) | 3.21 | 1.22× | 2.61× |
| READ[2] | 100000000 | uds | 200 | 0 | 356.28 | 451.23 | 545.83 | 267.8 | 2.49 (0.56) | 3.99 | 1.00× | 1.59× |
| READ[2] | 100000000 | native-json-h1 | 200 | 0 | 355.52 | 488.90 | 589.68 | 268.3 | 1.56 (1.56) | 0.03 |  |  |
| READ[2] | 100000000 | pure-rust | 200 | 0 | 410.47 | 485.19 | 613.78 | 232.9 | 2.54 (2.54) | 3.90 | 1.15× | 1.62× |
| READ[2] | 100000000 | native-json-default *(context)* | 200 | 0 | 376.18 | 506.86 | 699.84 | 253.6 | 1.55 (1.55) | 0.03 | 1.06× | 0.99× |
| READ[2] | 100000000 | native-grpc-cfe *(context)* | 200 | 0 | 419.48 | 505.12 | 802.09 | 227.4 | 3.84 (3.84) | 2.18 | 1.18× | 2.46× |

## Regional · gRPC · ListObjects

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| LIST-P1000 | 1000 | uds | 100 | 0 | 48.11 | 59.42 | 69.63 | 0.0 | 5433.50 (1765.00) | 6657.67 | 1.01× | 0.86× |
| LIST-P1000 | 1000 | native-grpc-cfe | 100 | 0 | 47.49 | 58.92 | 68.63 | 0.0 | 6299.00 (6299.00) | 5535.20 |  |  |
| LIST-P1000 | 1000 | pure-rust | 100 | 0 | 43.52 | 55.52 | 75.25 | 0.0 | 3202.00 (3202.00) | 4873.85 | 0.92× | 0.51× |
| LIST-P1000 | 1000 | native-json-h1 *(context)* | 100 | 0 | 60.97 | 71.53 | 83.88 | 0.0 | 18250.00 (18250.00) | 10151.22 | 1.28× | 2.90× |
| LIST-P100 | 1000 | uds | 100 | 0 | 221.97 | 253.96 | 275.12 | 0.0 | 8602.00 (3020.50) | 8180.10 | 0.97× | 0.87× |
| LIST-P100 | 1000 | native-grpc-cfe | 100 | 0 | 229.50 | 252.45 | 288.05 | 0.0 | 9868.00 (9868.00) | 6366.98 |  |  |
| LIST-P100 | 1000 | pure-rust | 100 | 0 | 218.71 | 246.62 | 297.16 | 0.0 | 4399.00 (4399.00) | 6290.99 | 0.95× | 0.45× |
| LIST-P100 | 1000 | native-json-h1 *(context)* | 100 | 0 | 232.49 | 250.78 | 297.68 | 0.0 | 19665.50 (19665.50) | 18595.08 | 1.01× | 1.99× |

## Zone · gRPC · ListObjects

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| LIST-P1000 | 1000 | uds | 100 | 0 | 71.36 | 94.96 | 110.01 | 0.0 | 4572.50 (1647.00) | 6173.10 | 1.01× | 0.78× |
| LIST-P1000 | 1000 | native-grpc-cfe | 100 | 0 | 70.95 | 94.20 | 113.36 | 0.0 | 5874.00 (5874.00) | 5326.03 |  |  |
| LIST-P1000 | 1000 | pure-rust | 100 | 0 | 68.69 | 94.90 | 109.30 | 0.0 | 2583.50 (2583.50) | 4810.36 | 0.97× | 0.44× |
| LIST-P1000 | 1000 | native-json-h1 *(context)* | 100 | 0 | 83.91 | 107.95 | 116.18 | 0.0 | 17884.00 (17884.00) | 9881.94 | 1.18× | 3.04× |
| LIST-P100 | 1000 | uds | 100 | 0 | 284.43 | 324.80 | 346.20 | 0.0 | 8906.50 (3124.00) | 7962.64 | 0.99× | 0.93× |
| LIST-P100 | 1000 | native-grpc-cfe | 100 | 0 | 286.89 | 307.46 | 325.34 | 0.0 | 9614.50 (9614.50) | 6111.06 |  |  |
| LIST-P100 | 1000 | pure-rust | 100 | 0 | 263.01 | 294.91 | 323.26 | 0.0 | 4431.50 (4431.50) | 6214.90 | 0.92× | 0.46× |
| LIST-P100 | 1000 | native-json-h1 *(context)* | 100 | 0 | 287.48 | 307.49 | 333.88 | 0.0 | 18889.00 (18889.00) | 18238.47 | 1.00× | 1.96× |

## Zone · gRPC · ReadObject (open)

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| open | 100000 | uds | 60 | 0 | 18.61 | 22.49 | 26.64 | – | 5.25 (1.67) | 1.41 | 1.14× | 1.19× |
| open | 100000 | native-grpc-cfe | 60 | 0 | 16.34 | 19.76 | 23.81 | – | 4.40 (4.40) | 1.40 |  |  |
| open | 100000 | pure-rust | 60 | 0 | 18.93 | 24.76 | 26.56 | – | 2.29 (2.29) | 0.58 | 1.16× | 0.52× |
| open | 2097152 | uds | 65 | 0 | 18.15 | 24.04 | 29.79 | – | 0.26 (0.08) | 0.07 | 1.11× | 1.13× |
| open | 2097152 | native-grpc-cfe | 65 | 0 | 16.35 | 20.02 | 24.40 | – | 0.23 (0.23) | 0.07 |  |  |
| open | 2097152 | pure-rust | 65 | 0 | 19.73 | 25.64 | 27.22 | – | 0.12 (0.12) | 0.03 | 1.21× | 0.51× |
| open | 100000000 | uds | 75 | 0 | 18.18 | 23.79 | 38.87 | – | 0.01 (0.00) | 0.00 | 1.15× | 1.10× |
| open | 100000000 | native-grpc-cfe | 75 | 0 | 15.83 | 20.91 | 25.40 | – | 0.01 (0.01) | 0.00 |  |  |
| open | 100000000 | pure-rust | 75 | 0 | 19.53 | 25.32 | 28.90 | – | 0.00 (0.00) | 0.00 | 1.23× | 0.58× |

## Zone · gRPC · ReadObject (random8k)

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| random8k | 100000 | uds | 60 | 0 | 16.65 | 25.05 | 28.29 | 0.5 | 106.02 (39.31) | 31.11 | 0.98× | 0.80× |
| random8k | 100000 | native-grpc-cfe | 60 | 0 | 17.04 | 23.81 | 73.00 | 0.5 | 132.08 (132.08) | 25.49 |  |  |
| random8k | 100000 | pure-rust | 60 | 0 | 18.69 | 27.13 | 28.59 | 0.4 | 45.04 (45.04) | 11.70 | 1.10× | 0.34× |
| random8k | 2097152 | uds | 65 | 0 | 17.97 | 22.05 | 27.77 | 0.4 | 112.06 (40.89) | 31.05 | 1.13× | 0.84× |
| random8k | 2097152 | native-grpc-cfe | 65 | 0 | 15.84 | 23.38 | 25.02 | 0.5 | 133.91 (133.91) | 25.50 |  |  |
| random8k | 2097152 | pure-rust | 65 | 0 | 17.64 | 21.76 | 24.99 | 0.4 | 47.49 (47.49) | 11.70 | 1.11× | 0.35× |
| random8k | 100000000 | uds | 75 | 0 | 17.42 | 22.50 | 27.40 | 0.4 | 120.36 (42.36) | 30.58 | 1.04× | 0.83× |
| random8k | 100000000 | native-grpc-cfe | 75 | 0 | 16.69 | 24.63 | 133.84 | 0.5 | 145.87 (145.87) | 25.53 |  |  |
| random8k | 100000000 | pure-rust | 75 | 0 | 17.10 | 24.21 | 27.30 | 0.5 | 57.25 (57.25) | 12.74 | 1.02× | 0.39× |

## Zone · gRPC · ReadObject (read50x2x4)

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| read50x2x4 | 209715200 | uds | 100 | 0 | 413.36 | 489.01 | 562.98 | 242.3 | 2.84 (0.64) | 6.40 | 1.00× | 0.72× |
| read50x2x4 | 209715200 | native-grpc-cfe | 100 | 0 | 412.26 | 496.27 | 549.89 | 243.3 | 3.95 (3.95) | 2.18 |  |  |
| read50x2x4 | 209715200 | pure-rust | 100 | 0 | 409.02 | 472.38 | 590.67 | 244.5 | 1.61 (1.61) | 5.08 | 0.99× | 0.41× |

## Zone · gRPC · ReadObject (full, bidi)

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| READ-BIDI | 100000 | uds | 60 | 0 | 20.68 | 27.48 | 33.08 | 4.6 | 14.12 (4.96) | 8.05 | 0.96× | 0.95× |
| READ-BIDI | 100000 | native-grpc-cfe | 60 | 0 | 21.48 | 27.38 | 89.73 | 4.4 | 14.93 (14.93) | 4.22 |  |  |
| READ-BIDI | 100000 | pure-rust | 60 | 0 | 22.44 | 28.15 | 32.14 | 4.3 | 5.75 (5.75) | 5.55 | 1.04× | 0.39× |
| READ-BIDI | 2097152 | uds | 65 | 0 | 31.58 | 39.06 | 43.53 | 63.3 | 3.70 (0.88) | 7.13 | 1.02× | 0.81× |
| READ-BIDI | 2097152 | native-grpc-cfe | 65 | 0 | 31.01 | 43.52 | 54.41 | 64.5 | 4.59 (4.59) | 2.28 |  |  |
| READ-BIDI | 2097152 | pure-rust | 65 | 0 | 33.47 | 41.00 | 44.53 | 59.8 | 2.42 (2.42) | 5.00 | 1.08× | 0.53× |
| READ-BIDI | 100000000 | uds | 75 | 0 | 345.16 | 409.05 | 463.46 | 276.3 | 3.05 (0.67) | 6.31 | 0.93× | 0.74× |
| READ-BIDI | 100000000 | native-grpc-cfe | 75 | 0 | 372.07 | 505.40 | 590.89 | 256.3 | 4.12 (4.12) | 2.16 |  |  |
| READ-BIDI | 100000000 | pure-rust | 75 | 0 | 399.87 | 481.21 | 522.40 | 238.5 | 1.90 (1.90) | 5.14 | 1.07× | 0.46× |

## Zone · gRPC · WriteObject (appendable)

| op | size | impl | n | err | p50 | p90 | p99 | MiB/s | CPU ns/B (app) | alloc B/B | p50 vs base | CPU vs base |
|---|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| APPENDABLE | 100000 | uds | 60 | 0 | 65.78 | 78.03 | 97.69 | 1.5 | 12.93 (4.32) | 31.15 | 1.00× | 0.80× |
| APPENDABLE | 100000 | native-grpc-cfe | 60 | 0 | 65.85 | 79.69 | 225.36 | 1.5 | 16.20 (16.20) | 3.81 |  |  |
| APPENDABLE | 100000 | pure-rust | 60 | 0 | 68.85 | 86.86 | 99.42 | 1.4 | 7.50 (7.50) | 26.27 | 1.05× | 0.46× |
| APPENDABLE | 2097152 | uds | 65 | 0 | 78.51 | 92.16 | 105.70 | 25.5 | 3.00 (0.69) | 8.15 | 1.01× | 1.22× |
| APPENDABLE | 2097152 | native-grpc-cfe | 65 | 0 | 77.65 | 88.81 | 195.16 | 25.8 | 2.46 (2.46) | 1.61 |  |  |
| APPENDABLE | 2097152 | pure-rust | 65 | 0 | 78.41 | 92.83 | 100.65 | 25.5 | 1.59 (1.59) | 4.07 | 1.01× | 0.65× |
| APPENDABLE | 100000000 | uds | 75 | 0 | 307.75 | 360.10 | 408.15 | 309.9 | 2.86 (0.44) | 7.31 | 1.01× | 1.81× |
| APPENDABLE | 100000000 | native-grpc-cfe | 75 | 0 | 304.20 | 352.46 | 392.15 | 313.5 | 1.58 (1.58) | 1.54 |  |  |
| APPENDABLE | 100000000 | pure-rust | 75 | 0 | 306.78 | 361.63 | 376.63 | 310.9 | 1.28 (1.28) | 3.80 | 1.01× | 0.81× |

## Synthetic hop floor (single-stream `--synthetic`, median CPU & alloc)

| chunk KiB | ch | op | size | n | p50 | p90 | p99 | MiB/s | CPU ns/B (app, sidecar) | alloc B/B (app, sidecar) |
|--:|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 256 | 1 | READ[0] | 100000 | 12 | 0.180 | 0.240 | 0.280 | 529.8 | 4.07 (2.40, 1.64) | 1.99 (0.28, 1.71) |
| 256 | 1 | READ[0] | 2097152 | 23 | 1.459 | 1.905 | 2.517 | 1370.8 | 1.52 (0.90, 0.62) | 1.08 (0.02, 1.06) |
| 256 | 1 | READ[0] | 100000000 | 15 | 55.490 | 57.672 | 58.404 | 1718.6 | 1.23 (0.84, 0.40) | 2.03 (0.99, 1.03) |
| 256 | 1 | SINGLE-SHOT | 100000 | 6 | 0.181 | 0.213 | 0.213 | 532.8 | 3.55 (1.96, 1.60) | 4.96 (1.29, 3.67) |
| 256 | 1 | SINGLE-SHOT | 2097152 | 12 | 1.268 | 1.526 | 1.554 | 1584.8 | 1.17 (0.59, 0.59) | 2.84 (1.03, 1.81) |
| 256 | 1 | SINGLE-SHOT | 100000000 | 7 | 42.219 | 45.512 | 47.918 | 2258.9 | 0.89 (0.45, 0.47) | 2.50 (1.02, 1.48) |
| 1024 | 1 | READ[0] | 100000 | 12 | 0.172 | 0.257 | 0.297 | 562.8 | 3.94 (2.17, 1.75) | 1.99 (0.28, 1.71) |
| 1024 | 1 | READ[0] | 2097152 | 23 | 1.491 | 2.202 | 2.344 | 1341.4 | 1.27 (0.73, 0.65) | 1.06 (0.02, 1.04) |
| 1024 | 1 | READ[0] | 100000000 | 15 | 50.530 | 58.720 | 60.190 | 1887.3 | 1.20 (0.71, 0.49) | 2.01 (1.00, 1.01) |
| 1024 | 1 | SINGLE-SHOT | 100000 | 6 | 0.183 | 0.267 | 0.267 | 558.6 | 3.39 (1.73, 1.67) | 5.01 (1.29, 3.72) |
| 1024 | 1 | SINGLE-SHOT | 2097152 | 12 | 1.736 | 2.049 | 2.089 | 1188.0 | 1.32 (0.50, 0.82) | 3.60 (1.03, 2.57) |
| 1024 | 1 | SINGLE-SHOT | 100000000 | 7 | 35.029 | 35.502 | 38.675 | 2722.5 | 0.78 (0.40, 0.38) | 2.54 (1.02, 1.53) |
| 4096 | 1 | READ[0] | 100000 | 12 | 0.182 | 0.242 | 0.359 | 532.9 | 3.88 (2.17, 1.77) | 1.99 (0.28, 1.71) |
| 4096 | 1 | READ[0] | 2097152 | 23 | 1.283 | 1.886 | 2.167 | 1558.8 | 1.19 (0.74, 0.43) | 1.05 (0.02, 1.03) |
| 4096 | 1 | READ[0] | 100000000 | 15 | 49.199 | 57.418 | 73.106 | 1938.4 | 1.19 (0.64, 0.53) | 2.00 (1.00, 1.00) |
| 4096 | 1 | SINGLE-SHOT | 100000 | 6 | 0.170 | 0.272 | 0.272 | 598.4 | 3.25 (1.79, 1.46) | 4.99 (1.29, 3.70) |
| 4096 | 1 | SINGLE-SHOT | 2097152 | 12 | 1.264 | 1.737 | 1.742 | 1696.1 | 1.07 (0.46, 0.63) | 3.09 (0.53, 2.56) |
| 4096 | 1 | SINGLE-SHOT | 100000000 | 7 | 39.152 | 43.606 | 44.142 | 2435.8 | 0.91 (0.39, 0.53) | 1.85 (0.27, 1.59) |

## Throughput (concurrent 100 MB transfers)

| run | impl | MiB/s | CPU ms app | CPU ms sidecar | peak RSS MiB app | peak RSS MiB sidecar | ok |
|---|---|--:|--:|--:|--:|--:|--:|
| synthetic-tp-c1048576-ch1 | uds-synthetic | 2376.7 | 20437 | 19700 | 218 | 220 | 1 |
| synthetic-tp-c1048576-ch4 | uds-synthetic | 5686.3 | 31160 | 23799 | 196 | 148 | 1 |
| synthetic-tp-c262144-ch1 | uds-synthetic | 1956.7 | 26196 | 20485 | 1035 | 144 | 1 |
| synthetic-tp-c262144-ch4 | uds-synthetic | 5107.5 | 37594 | 24247 | 244 | 79 | 1 |
| synthetic-tp-c4194304-ch1 | uds-synthetic | 1993.0 | 20343 | 17966 | 326 | 998 | 1 |
| synthetic-tp-c4194304-ch4 | uds-synthetic | 6186.7 | 27681 | 23788 | 291 | 665 | 1 |
| tp-bidi-read-w32-native | native-grpc-cfe | 632.8 | 61983 | 0 | 300 | 0 | 1 |
| tp-bidi-read-w32-rust | pure-rust | 1074.8 | 51321 | 0 | 345 | 0 | 1 |
| tp-bidi-read-w32-uds | uds | 775.2 | 12246 | 60706 | 286 | 341 | 1 |
| tp-bidi-read-w8-native | native-grpc-cfe | 683.9 | 16353 | 0 | 178 | 0 | 1 |
| tp-bidi-read-w8-rust | pure-rust | 844.3 | 11043 | 0 | 199 | 0 | 1 |
| tp-bidi-read-w8-uds | uds | 651.4 | 3137 | 14550 | 223 | 151 | 1 |
| tp-bidi-write-w32-native | native-grpc-cfe | 1625.3 | 25778 | 0 | 171 | 0 | 1 |
| tp-bidi-write-w32-rust | pure-rust | 1065.5 | 23230 | 0 | 1020 | 0 | 1 |
| tp-bidi-write-w32-uds | uds | 1484.4 | 8098 | 44629 | 124 | 1722 | 1 |
| tp-bidi-write-w8-native | native-grpc-cfe | 1217.6 | 6558 | 0 | 151 | 0 | 1 |
| tp-bidi-write-w8-rust | pure-rust | 1009.1 | 6344 | 0 | 240 | 0 | 1 |
| tp-bidi-write-w8-uds | uds | 1297.5 | 2051 | 11614 | 117 | 513 | 1 |
| tp-json-read-w32-native-json-h1 | native-json-h1 | 4593.0 | 30398 | 0 | 245 | 0 | 1 |
| tp-json-read-w32-rust | pure-rust | 4484.0 | 22623 | 0 | 159 | 0 | 1 |
| tp-json-read-w32-uds | uds | 1675.6 | 13769 | 41974 | 214 | 1233 | 1 |
| tp-json-read-w8-native-json-h1 | native-json-h1 | 1789.4 | 6388 | 0 | 149 | 0 | 1 |
| tp-json-read-w8-rust | pure-rust | 1339.2 | 5949 | 0 | 126 | 0 | 1 |
| tp-json-read-w8-uds | uds | 1317.4 | 3508 | 11068 | 187 | 222 | 1 |
| tp-json-write-w32-native-json-h1 | native-json-h1 | 2368.7 | 28965 | 0 | 214 | 0 | 1 |
| tp-json-write-w32-rust | pure-rust | 2339.0 | 14750 | 0 | 132 | 0 | 1 |
| tp-json-write-w32-uds | uds | 733.9 | 7481 | 27957 | 141 | 563 | 1 |
| tp-json-write-w8-native-json-h1 | native-json-h1 | 708.7 | 5865 | 0 | 141 | 0 | 1 |
| tp-json-write-w8-rust | pure-rust | 573.6 | 3539 | 0 | 117 | 0 | 1 |
| tp-json-write-w8-uds | uds | 420.0 | 1951 | 7296 | 130 | 214 | 1 |

## Whole-run totals (peak RSS)

| run | impl | wall s | CPU s app | CPU s sidecar | peak RSS MiB app | peak RSS MiB sidecar |
|---|---|--:|--:|--:|--:|--:|
| appendable-zonal-native | native-grpc-cfe | 78.3 | 43.9 | 0.0 | 161 | 0 |
| appendable-zonal-rust | pure-rust | 75.8 | 24.8 | 0.0 | 160 | 0 |
| appendable-zonal-uds | uds | 71.2 | 9.0 | 36.9 | 176 | 164 |
| list-regional-native-grpc-cfe | native-grpc-cfe | 33.8 | 2.3 | 0.0 | 143 | 0 |
| list-regional-native-json-h1 | native-json-h1 | 35.1 | 4.9 | 0.0 | 162 | 0 |
| list-regional-rust | pure-rust | 34.0 | 1.2 | 0.0 | 149 | 0 |
| list-regional-uds | uds | 33.5 | 0.8 | 1.6 | 121 | 32 |
| list-zonal-native-grpc-cfe | native-grpc-cfe | 43.0 | 2.6 | 0.0 | 143 | 0 |
| list-zonal-native-json-h1 | native-json-h1 | 44.7 | 5.1 | 0.0 | 167 | 0 |
| list-zonal-rust | pure-rust | 41.4 | 1.3 | 0.0 | 153 | 0 |
| list-zonal-uds | uds | 43.1 | 0.9 | 1.7 | 123 | 33 |
| open-regional-native | native-grpc-cfe | 0.4 | 0.0 | 0.0 | 131 | 0 |
| open-regional-rust | pure-rust | 0.4 | 0.0 | 0.0 | 114 | 0 |
| open-regional-uds | uds | 0.4 | 0.0 | 0.1 | 115 | 21 |
| open-zonal-native | native-grpc-cfe | 42.1 | 12.7 | 0.0 | 143 | 0 |
| open-zonal-rust | pure-rust | 44.0 | 10.1 | 0.0 | 151 | 0 |
| open-zonal-uds | uds | 44.2 | 3.8 | 20.5 | 115 | 170 |
| random8k-regional-native | native-grpc-cfe | 0.4 | 0.0 | 0.0 | 129 | 0 |
| random8k-regional-rust | pure-rust | 0.3 | 0.0 | 0.0 | 114 | 0 |
| random8k-regional-uds | uds | 0.3 | 0.0 | 0.1 | 113 | 23 |
| random8k-zonal-native | native-grpc-cfe | 49.4 | 13.0 | 0.0 | 147 | 0 |
| random8k-zonal-rust | pure-rust | 42.6 | 10.1 | 0.0 | 156 | 0 |
| random8k-zonal-uds | uds | 41.9 | 3.7 | 20.1 | 113 | 175 |
| read50x2x4-regional-native | native-grpc-cfe | 3.5 | 0.8 | 0.0 | 247 | 0 |
| read50x2x4-regional-rust | pure-rust | 5.2 | 0.4 | 0.0 | 218 | 0 |
| read50x2x4-regional-uds | uds | 5.5 | 0.2 | 0.8 | 217 | 69 |
| read50x2x4-zonal-native | native-grpc-cfe | 104.1 | 74.1 | 0.0 | 264 | 0 |
| read50x2x4-zonal-rust | pure-rust | 107.7 | 42.0 | 0.0 | 264 | 0 |
| read50x2x4-zonal-uds | uds | 110.4 | 16.4 | 73.4 | 279 | 209 |
| synthetic-tp-c1048576-ch1 | uds-synthetic | 12.9 | 20.5 | 19.8 | 218 | 220 |
| synthetic-tp-c1048576-ch4 | uds-synthetic | 5.4 | 31.2 | 23.9 | 196 | 148 |
| synthetic-tp-c262144-ch1 | uds-synthetic | 15.6 | 26.2 | 20.5 | 1035 | 144 |
| synthetic-tp-c262144-ch4 | uds-synthetic | 6.0 | 37.6 | 24.3 | 244 | 79 |
| synthetic-tp-c4194304-ch1 | uds-synthetic | 15.4 | 20.4 | 18.0 | 326 | 998 |
| synthetic-tp-c4194304-ch4 | uds-synthetic | 5.0 | 27.7 | 23.9 | 291 | 665 |
| synthetic-w1r3-c1048576-ch1 | uds-synthetic | 3.1 | 4.0 | 3.0 | 237 | 25 |
| synthetic-w1r3-c1048576-ch4 | uds-synthetic | 3.1 | 4.0 | 3.0 | 252 | 27 |
| synthetic-w1r3-c262144-ch1 | uds-synthetic | 3.3 | 4.6 | 2.6 | 302 | 17 |
| synthetic-w1r3-c262144-ch4 | uds-synthetic | 3.1 | 4.5 | 2.7 | 249 | 20 |
| synthetic-w1r3-c4194304-ch1 | uds-synthetic | 3.0 | 3.7 | 3.3 | 250 | 52 |
| synthetic-w1r3-c4194304-ch4 | uds-synthetic | 3.2 | 3.8 | 3.3 | 201 | 55 |
| tp-bidi-read-w32-native | native-grpc-cfe | 24.5 | 62.2 | 0.0 | 300 | 0 |
| tp-bidi-read-w32-rust | pure-rust | 14.6 | 51.5 | 0.0 | 381 | 0 |
| tp-bidi-read-w32-uds | uds | 20.1 | 12.3 | 61.0 | 286 | 341 |
| tp-bidi-read-w8-native | native-grpc-cfe | 6.0 | 16.5 | 0.0 | 178 | 0 |
| tp-bidi-read-w8-rust | pure-rust | 4.9 | 11.2 | 0.0 | 219 | 0 |
| tp-bidi-read-w8-uds | uds | 6.3 | 3.2 | 14.9 | 223 | 151 |
| tp-bidi-write-w32-native | native-grpc-cfe | 9.4 | 25.8 | 0.0 | 171 | 0 |
| tp-bidi-write-w32-rust | pure-rust | 14.3 | 23.2 | 0.0 | 1064 | 0 |
| tp-bidi-write-w32-uds | uds | 10.3 | 8.1 | 44.6 | 124 | 1722 |
| tp-bidi-write-w8-native | native-grpc-cfe | 3.1 | 6.6 | 0.0 | 151 | 0 |
| tp-bidi-write-w8-rust | pure-rust | 3.8 | 6.3 | 0.0 | 301 | 0 |
| tp-bidi-write-w8-uds | uds | 2.9 | 2.1 | 11.6 | 117 | 513 |
| tp-json-read-w32-native-json-h1 | native-json-h1 | 4.5 | 30.5 | 0.0 | 245 | 0 |
| tp-json-read-w32-rust | pure-rust | 4.8 | 22.8 | 0.0 | 172 | 0 |
| tp-json-read-w32-uds | uds | 10.4 | 13.8 | 42.2 | 214 | 1233 |
| tp-json-read-w8-native-json-h1 | native-json-h1 | 2.9 | 6.5 | 0.0 | 149 | 0 |
| tp-json-read-w8-rust | pure-rust | 4.1 | 6.1 | 0.0 | 129 | 0 |
| tp-json-read-w8-uds | uds | 4.3 | 3.6 | 11.3 | 187 | 222 |
| tp-json-write-w32-native-json-h1 | native-json-h1 | 6.4 | 29.0 | 0.0 | 214 | 0 |
| tp-json-write-w32-rust | pure-rust | 6.5 | 14.8 | 0.0 | 132 | 0 |
| tp-json-write-w32-uds | uds | 20.8 | 7.5 | 28.0 | 141 | 563 |
| tp-json-write-w8-native-json-h1 | native-json-h1 | 5.4 | 5.9 | 0.0 | 141 | 0 |
| tp-json-write-w8-rust | pure-rust | 6.7 | 3.5 | 0.0 | 117 | 0 |
| tp-json-write-w8-uds | uds | 9.1 | 2.0 | 7.3 | 130 | 214 |
| w1r3-100000-native-grpc-cfe | native-grpc-cfe | 45.6 | 1.1 | 0.0 | 38 | 0 |
| w1r3-100000-native-json-default | native-json-default | 40.2 | 0.7 | 0.0 | 25 | 0 |
| w1r3-100000-native-json-h1 | native-json-h1 | 43.6 | 0.6 | 0.0 | 26 | 0 |
| w1r3-100000-rust | pure-rust | 42.4 | 0.6 | 0.0 | 20 | 0 |
| w1r3-100000-uds | uds | 46.3 | 0.7 | 0.8 | 20 | 23 |
| w1r3-100000000-native-grpc-cfe | native-grpc-cfe | 464.0 | 276.5 | 0.0 | 182 | 0 |
| w1r3-100000000-native-json-default | native-json-default | 424.7 | 115.3 | 0.0 | 122 | 0 |
| w1r3-100000000-native-json-h1 | native-json-h1 | 414.0 | 117.7 | 0.0 | 121 | 0 |
| w1r3-100000000-rust | pure-rust | 471.5 | 170.1 | 0.0 | 119 | 0 |
| w1r3-100000000-uds | uds | 455.7 | 45.0 | 152.0 | 160 | 281 |
| w1r3-2097152-native-grpc-cfe | native-grpc-cfe | 57.3 | 7.1 | 0.0 | 66 | 0 |
| w1r3-2097152-native-json-default | native-json-default | 49.4 | 3.3 | 0.0 | 28 | 0 |
| w1r3-2097152-native-json-h1 | native-json-h1 | 49.1 | 3.0 | 0.0 | 28 | 0 |
| w1r3-2097152-rust | pure-rust | 49.0 | 3.7 | 0.0 | 24 | 0 |
| w1r3-2097152-uds | uds | 50.2 | 1.7 | 4.3 | 50 | 40 |

## Gaps and errors observed

- `open-regional` native-grpc-cfe open ×1: PERMISSION_DENIED: Permanent error; with a last message of The BidiReadObject RPC is not yet available for general use; 
- `open-regional` pure-rust open ×1: the service reports an error with code PERMISSION_DENIED described as: The BidiReadObject RPC is not yet available for g
- `open-regional` uds open ×1: PERMISSION_DENIED: The BidiReadObject RPC is not yet available for general use; or for project (110033306189) or bucket 
- `random8k-regional` native-grpc-cfe random8k ×1: PERMISSION_DENIED: Permanent error; with a last message of The BidiReadObject RPC is not yet available for general use; 
- `random8k-regional` pure-rust random8k ×1: the service reports an error with code PERMISSION_DENIED described as: The BidiReadObject RPC is not yet available for g
- `random8k-regional` uds random8k ×1: PERMISSION_DENIED: The BidiReadObject RPC is not yet available for general use; or for project (110033306189) or bucket 
- `read50x2x4-regional` native-grpc-cfe read50x2x4 ×1: PERMISSION_DENIED: Permanent error; with a last message of The BidiReadObject RPC is not yet available for general use; 
- `read50x2x4-regional` pure-rust read50x2x4 ×1: the service reports an error with code PERMISSION_DENIED described as: The BidiReadObject RPC is not yet available for g
- `read50x2x4-regional` uds read50x2x4 ×1: PERMISSION_DENIED: The BidiReadObject RPC is not yet available for general use; or for project (110033306189) or bucket 
