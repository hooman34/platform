"""
Scheduler for the data/AI job scraper.

Runs `run_daily_job_scrape` every day at 08:00 local time and writes a
digest of the top 10 postings to the console and to ``log/jobs_YYYY-MM-DD.md``.

Usage (from the ``app/`` directory):

    python job_executor.py

Press Ctrl+C to stop the scheduler.
"""

import os
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from utils.job_scraper import run_daily_job_scrape

print("Currently working at {}".format(os.getcwd()))

sched = BlockingScheduler()


@sched.scheduled_job('cron', hour=8, minute=0, id='daily_data_ai_job_scrape')
def daily_job_scrape():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] Running daily data/AI job scrape...".format(now))
    run_daily_job_scrape(top_n=10)


if __name__ == '__main__':
    sched.start()
