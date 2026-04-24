import datetime as dt

def log(msg: str):
    now = dt.datetime.now().isoformat(timespec="seconds")
    print(f"[{now}] [ALERTING] {msg}")
