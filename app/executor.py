import os
import mysql.connector
from mysql.connector import Error
import pandas as pd
import investpy
from fredapi import Fred
import json
from matplotlib import pyplot as plt
import quandl
from datetime import date
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from utils.utils import *

print("Currently working at {}".format(os.getcwd()))

sched = BlockingScheduler()

@sched.scheduled_job('interval', seconds=10, id='test_1')
def call_data():

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time)

    today = date.today().strftime("%d/%m/%Y")
    d = investing_api('stock', 'MSFT', '13/05/2022', today)
    print(d)

sched.start()
