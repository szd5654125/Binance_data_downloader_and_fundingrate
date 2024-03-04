from datetime import datetime
import pandas as pd


def timestamp_to_datetime(timestamp):
    # trans ms to s
    timestamp_in_seconds = timestamp / 1000.0
    # trans timestamp to datetime
    dt_object = datetime.fromtimestamp(timestamp_in_seconds)
    # Format the datetime as str, incl. ms
    formatted_date = dt_object.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return formatted_date


# formatted_date = timestamp_to_datetime(1706745600000)
# print(formatted_date)  # output date and time
help(pd.DataFrame)