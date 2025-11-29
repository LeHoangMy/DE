#!/bin/bash

csvtool -t COMMA -u TAB cat data/tmdb-movies.csv > temp_movies.csv

#keep header for new file
head -1 temp_movies.csv > min_max_rev.csv

#filer revenue >0 and insert into output file
awk -F$'\t' 'NR==1 || $21 >0 ' temp_movies.csv | tail -n +2 | sort -t$'\t' -k21n | head -1 >> min_max_rev.csv

awk -F$'\t' 'NR==1 || $21 >0' temp_movies.csv | tail -n +2 | sort -t$'\t' -k21nr | head -1 >> min_max_rev.csv

#return original format with ','

csvtool -t TAB -u COMMA cat min_max_rev.csv > min_max_rev.tmp && mv min_max_rev.tmp min_max_rev.csv

