#!/bin/bash


csvtool -t COMMA -u TAB cat data/tmdb-movies.csv > temp_movies.csv

echo "--- Top 10 Phim Lợi Nhuận Ròng Cao Nhất ---"
echo "-----------------------------------"

# 2. Xử lý dữ liệu: Tính lợi nhuận, sắp xếp và lọc Top 10
awk -F$'\t' '
    NR > 1 {
        # Tính Lợi nhuận = Cột 21 (Revenue) - Cột 20 (Budget)
        profit = $21 - $20;
        
        # Lọc: Chỉ xử lý những phim có dữ liệu doanh thu và ngân sách hợp lệ
        if ($21 > 0 && $20 > 0) {
            # **SỬA LỖI:** Sử dụng printf để in Lợi Nhuận (profit) dưới dạng số nguyên (%.0f)
            # Điều này giúp loại bỏ ký hiệu khoa học (e+09) và đảm bảo sort hoạt động đúng
            printf "%s\t%.0f\n", $6, profit
        }
    }
' temp_movies.csv | \
sort -t$'\t' -k2nr | \
head -10

