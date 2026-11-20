# Data source and preparation note

Source: Chen, D., Sain, S. L., and Guo, K., *Online Retail*, UCI Machine Learning Repository: <https://archive.ics.uci.edu/dataset/352/online+retail>.

The supplied repository did not record the original download date. The raw CSV copy has a repository file timestamp of 11 November 2026; this is retained as provenance and is not represented as a verified download date.

Preparation is entirely programmatic:

- reject cancelled invoices whose `InvoiceNo` starts with `C`;
- reject missing customer IDs and non-positive quantities or prices;
- remove exact duplicate source rows;
- classify each entire invoice with `int(MD5(invoice)[:8], 16) % 10`: `0`-`6` online, `7`-`9` periodic;
- sort periodic rows by event time and distribute them across 48 balanced release groups;
- derive customer, product, supplier, and store attributes with the deterministic rules in the project specification.

Validated counts:

| Measure | Rows |
|---|---:|
| Raw | 541,909 |
| Passing cleaning rules | 397,884 |
| Exact duplicates removed | 5,192 |
| Final cleaned | 392,692 |
| Online | 275,154 |
| Periodic | 117,538 |
