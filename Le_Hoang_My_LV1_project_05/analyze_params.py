import csv
from urllib.parse import urlparse, parse_qs
from collections import Counter

with open('data/product_urls.csv') as f:
    rows = list(csv.DictReader(f))

param_counter = Counter()
for row in rows:
    params = parse_qs(urlparse(row['url']).query)
    for key in params:
        if key not in ('gclid', 'fbclid') and not key.startswith('utm_'):
            param_counter[key] += 1

print(f'Total products: {len(rows)}')
print('\nTop params:')
for k, v in param_counter.most_common(20):
    print(f'  {k}: {v} ({v/len(rows)*100:.1f}%)')