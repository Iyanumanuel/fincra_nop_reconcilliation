import duckdb
import pandas as pd

conn = duckdb.connect("database/fincra_dev.duckdb")
# conn = duckdb.connect(":memory:")

trade_df = pd.read_csv("seeds/trade_blotter.csv")
nop_df = pd.read_csv("seeds/daily_nop_snapshot.csv")

conn.execute("CREATE TABLE IF NOT EXISTS main.trade_blotter AS SELECT * FROM trade_df")
conn.execute("CREATE TABLE IF NOT EXISTS main.daily_nop_snapshot AS SELECT * FROM nop_df")

print("Tables loaded successfully")
