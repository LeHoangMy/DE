#!/bin/bash

# 
csvtool -t COMMA -u TAB cat data/tmdb-movies.csv > temp_movies.csv

SUM_REVENUE=$(awk -F$'\t' '
    NR > 1 && $21 > 0 {
        total += $21
    }
    END {
        printf "Total Revenue: %.0f\n", total
    }' temp_movies.csv)

# 3. In kết quả tổng doanh thu ra màn hình
echo "$SUM_REVENUE"

