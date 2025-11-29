#!/bin/bash

# 1. Chuyển đổi định dạng tệp gốc sang sử dụng TAB làm delimiter (tách cột)
csvtool -t COMMA -u TAB cat data/tmdb-movies.csv > temp_movies.csv

echo "--- Đạo Diễn Có Nhiều Bộ Phim Nhất (Cột 9) ---"
echo "Đạo Diễn | Số Lượng Phim"
echo "--------------------------"

# Xử lý cột 9 (Đạo Diễn)
awk -F$'\t' '
    NR > 1 {
        # Tách các tên theo dấu "|"
        split($9, directors, "|"); 
        
        # Lặp qua từng tên đạo diễn và tăng bộ đếm
        for (i in directors) {
            # Loại bỏ khoảng trắng thừa và đảm bảo tên không rỗng
            gsub(/^ */, "", directors[i]); 
            if (directors[i] != "") {
                count[directors[i]]++;
            }
        }
    }
    END {
        # In ra tất cả kết quả đếm
        for (name in count) {
            print name "\t" count[name];
        }
    }
' temp_movies.csv | sort -t$'\t' -k2nr | head -1

echo ""
echo "--- Diễn Viên Đóng Nhiều Phim Nhất (Cột 7) ---"
echo "Diễn Viên | Số Lượng Phim"
echo "--------------------------"

# Xử lý cột 7 (Diễn Viên)
awk -F$'\t' '
    NR > 1 {
        # Tách các tên theo dấu "|"
        split($7, actors, "|"); 
        
        # Lặp qua từng tên diễn viên và tăng bộ đếm
        for (i in actors) {
            # Loại bỏ khoảng trắng thừa và đảm bảo tên không rỗng
            gsub(/^ */, "", actors[i]); 
            if (actors[i] != "") {
                count[actors[i]]++;
            }
        }
    }
    END {
        # In ra tất cả kết quả đếm
        for (name in count) {
            print name "\t" count[name];
        }
    }
' temp_movies.csv | sort -t$'\t' -k2nr | head -1


