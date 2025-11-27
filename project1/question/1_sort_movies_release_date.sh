#!/bin/bash

{
    # In header gốc
    head -n 1 tmdb-movies.csv

    # Xử lý dữ liệu
    tail -n +2 tmdb-movies.csv | awk -F, '
    {
        iso = ""

        # tìm cột chứa MM/DD/YY
        for (i=1; i<=NF; i++) {
            if ($i ~ /^[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2}$/) {
                split($i, d, "/")

                m = sprintf("%02d", d[1])
                d2 = sprintf("%02d", d[2])
                y = d[3]

                iso = (y < 25 ? "20" y : "19" y) "-" m "-" d2
                break
            }
        }

        # In ISO + dòng gốc để phục vụ sort
        if (iso != "") {
            print iso "," $0
        }
    }' \
    | sort -t, -k1,1r \
    | cut -d',' -f2-
} > sorted_movies.csv
