#!/bin/bash
#convert csv ',' to '\t'
csvtool -t COMMA -u TAB cat data/tmdb_movies.csv > temp_movies.csv

#filter column 18 voting_avg >7.5
awk -F'\t' 'NR==1 || $18 > 7.5' temp_movies.csv  > filter_movies.csv

#Return original format with ','
csvtool -t TAB -u COMMA cat filter_movies.csv > filter_movies.tmp && mv filter_movies.tmp filter_movies.csv


