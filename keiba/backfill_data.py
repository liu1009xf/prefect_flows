from prefect import flow, get_run_logger, task
from .load_race_id import load_race_id
from .load_race_data import load_race_data
from datetime import date, timedelta
import datetime as dt


@flow(name="Back Fill")
def backfillRaceId(startdate:dt.date=dt.datetime.now().date(), enddate:dt.date=dt.datetime.now().date()):
    delta = enddate - startdate   # returns timedelta

    for i in range(delta.days + 1):
        day = startdate + timedelta(days=i)
        load_race_id(day)
        load_race_data(day)