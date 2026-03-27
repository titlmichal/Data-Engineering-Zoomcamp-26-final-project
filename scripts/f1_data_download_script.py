# This is script is used in kestra flow to download and save F1 data
# didnt not manage to make kestra run it from a file, hence copy-pasting

import os
import sys

import pyarrow # GOING FOR STABLE 22.0.0
import pandas as pd
import fastf1

YEAR = int(sys.argv[1]) if len(sys.argv) > 1 else 2023
IS_TESTING = bool(int(sys.argv[2])) if len(sys.argv) > 2 else False

# in kestra flow script module using this syntax to get inputs
# YEAR = int('{{ inputs.year }}')
# IS_TESTING = '{{ inputs.is_testing }}'.lower() == 'true'

CACHE_DIR = "/app/f1_cache"
OUTPUT_DIR = "/app/f1_raw_data"
os.makedirs(CACHE_DIR, exist_ok=True) # Ensure the cache directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create output directory if it doesn't exist
fastf1.Cache.enable_cache(CACHE_DIR)  # Enable caching to speed up data retrieval

def clean_dtypes(df):
    """
    Formats timdelta columns to seconds and datetime columns to ISO string for better compatibility with Parquet format and saving to BQ.
    """
    for col in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[col]):
            df[col] = df[col].dt.total_seconds()
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return df

event_schedule = fastf1.get_event_schedule(YEAR)
for index, event in event_schedule.iterrows():
    if event["EventFormat"] == "testing":
        continue  # Skip testing events

    # processing log
    event_id = event["RoundNumber"]
    event_name = event["EventName"].replace(" ", "_").replace("/", "_")
    print(f"--> Processing (nr.{event_id}): {event_name} ({YEAR})")

    try:
        # getting data
        session = fastf1.get_session(YEAR, event_id, "R")
        session.load(telemetry=False, weather=False, messages=False)

        # laps data
        laps_df = clean_dtypes(session.laps)
        laps_path = os.path.join(OUTPUT_DIR, "laps", f"year={YEAR}", f"event_id={event_id}")
        os.makedirs(laps_path, exist_ok=True)
        laps_df.to_parquet(os.path.join(laps_path, "laps.parquet"), index=False)

        # results data
        results_df = clean_dtypes(session.results)
        results_path = os.path.join(OUTPUT_DIR, "results", f"year={YEAR}", f"event_id={event_id}")
        os.makedirs(results_path, exist_ok=True)
        results_df.to_parquet(os.path.join(results_path, "results.parquet"), index=False)

        print(f"--> Successfully processed event {event_name} ({YEAR})")

    except Exception as e:
        print(f"### Error processing event {event_name} ({YEAR}): {e}")
    
    if IS_TESTING:
        print("--> Testing mode enabled, stopping after first event.")
        break
