from helpers import runSQL

def run_transform(start_date, start_offset):
   runSQL("sql/01_events_dedup.sql",  {"start_date":start_date, "start_offset":start_offset})
   runSQL("sql/02_event_continuous_ranges.sql")
   return runSQL("sql/03_missing_offsets.sql")
