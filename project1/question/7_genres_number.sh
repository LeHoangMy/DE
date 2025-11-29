#!/bin/bash


csvtool -t COMMA -u TAB cat data/tmdb-movies.csv > temp_movies.csv

echo "--- Thống Kê Số Lượng Phim Theo Thể Loại 

echo "Thể Loại | Số Lượng Phim"
echo "--------------------------"

# Xử lý cột 14 (Thể Loại - genres)
awk -F$'\t' '
    NR > 1 {
        # Tách các thể loại theo dấu "|"
        split($14, genres, "|"); 
        
        
        for (i in genres) {
            # Loại bỏ khoảng trắng thừa (nếu có) và đảm bảo tên không rỗng
            gsub(/^ */, "", genres[i]); 
            if (genres[i] != "") {
                count[genres[i]]++;
            }
        }
    }
    END {
        # In ra tất cả kết quả đếm
        for (name in count) {
            print name "\t" count[name];
        }
    }
' temp_movies.csv | sort -t$'\t' -k2nr
