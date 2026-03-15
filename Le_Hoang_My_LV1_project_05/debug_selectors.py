"""
Debug gender trong raw HTML
"""
import requests
from bs4 import BeautifulSoup
import re

URL = "https://www.glamira.fr/glamira-pendant-viktor.html?alloy=yellow-375"

resp = requests.get(URL, timeout=10)
soup = BeautifulSoup(resp.text, "html.parser")

print("=== TÌM 'item-gender' TRONG RAW HTML ===")
matches = re.findall(r'.{0,30}item-gender.{0,100}', resp.text)
for m in matches[:5]:
    print(f"  {m}")
print()

print("=== TÌM 'gender' TRONG RAW HTML ===")
matches = re.findall(r'.{0,20}[Gg]ender.{0,80}', resp.text)
for m in matches[:10]:
    print(f"  {m}")
print()

print("=== TÌM 'Male' hoặc 'Mâle' TRONG RAW HTML ===")
matches = re.findall(r'.{0,30}[Mm][âa]le.{0,50}', resp.text)
for m in matches[:5]:
    print(f"  {m}")