README
1. Prerequisites
PostgreSQL 14+
Python 3.10+
pip install psycopg[binary] python-dotenv
2. Configure credentials
cp .env.example .env
# edit .env with your PostgreSQL host, port, database, user, password

3. Create the database and schema
createdb -U postgres realtime_data_stream
psql -U postgres -d realtime_data_stream -f src/warehouse-schema.sql

4. Prepare and load source data
python data/static/feed_db.py

This cleans data/raw/online-retail.csv, splits it into online / periodic streams, generates the Master Data CSVs, and loads the input schemas and staging tables.

5. Start the warehouse
python app.py

At startup the program prints the arrival mode (peak / non-peak), the seconds-per-hour scale, and the stream-buffer limit, then asks for database credentials.

While running, type any of:

online      online stream only
periodic    periodic groups only
both        both streams (online first)
peak        80 txn/s, shedding allowed
non-peak    10 txn/s, process shed rows
status      show mode, speed, buffer, H, shed counts
quit        stop threads and exit

6. Run the OLAP queries
After the fact table has been populated:

psql -U postgres -d realtime_data_stream -f src/queries1.sql
