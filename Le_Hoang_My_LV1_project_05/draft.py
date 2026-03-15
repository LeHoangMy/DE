from IP2Location import IP2Location

reader = IP2Location("data/IP-COUNTRY-REGION-CITY.BIN")
rec = reader.get_all("37.170.17.183")

for attr in dir(rec):
    if not attr.startswith("_"):
        val = getattr(rec, attr)
        print(f"{attr}: {val}")