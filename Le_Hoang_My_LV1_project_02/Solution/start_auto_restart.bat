@echo off
:START_CRAWLER
echo =======================================================
echo KHOI DONG LAI CRAWLER - %date% %time%
echo =======================================================

:: Lệnh gọi file Python
python Crawling_tiki_full_safe_200k.py

:: Kiem tra Error Level. Neu Python bi loi, no se thoat voi code khac 0.
if %errorlevel% NEQ 0 (
    echo.
    echo ❌ CRAWLER BI LOI DUNG GIUA CHUNG. Dang cho 30 giay de khoi dong lai...
    timeout /t 30 /nobreak
) else (
    echo.
    echo ✅ CRAWLER DA HOAN THANH BLOCK HOAC TOAN BO JOB. Khoi dong block tiep theo...
)

:: Quay lai nhan START_CRAWLER de tiep tuc hoac khoi dong lai
goto START_CRAWLER

:: Nhan Ctrl+C de dung script nay