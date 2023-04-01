from prefect import flow, get_run_logger, task
from .load_race_id import load_race_id
from datetime import date, timedelta


@flow
def backfillRaceId(startdate=None, enddate=None):
    start_date = date(2008, 8, 15) 
    end_date = date(2008, 9, 15)    # perhaps date.now()

    delta = end_date - start_date   # returns timedelta

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
    load_race_id()