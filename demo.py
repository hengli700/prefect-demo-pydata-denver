import requests
import json
from collections import namedtuple
from contextlib import closing
import sqlite3
import datetime

from prefect import task, Flow
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.schedules import IntervalSchedule

# setup
create_table = SQLiteScript(
    db='cfpbcomplaints.db',
    script='CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
)


# extract
@task(cache_for=datetime.timedelta(days=1))
def get_complaint_data():
    r = requests.get('https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/',
                     params={'size': 100})
    response_json = json.loads(r.text)
    print('I actually requested this!')
    return response_json['hits']['hits']


# transform
@task
def parse_complaint_data(raw):
    complaints = []
    Complaint = namedtuple('Complaint', ['date_received', 'state', 'product', 'company', 'complaint_what_happened'])
    for row in raw:
        source = row.get('_source')
        this_complaint = Complaint(
            date_received=source.get('date_received'),
            state=source.get('state'),
            product=source.get('product'),
            company=source.get('company'),
            complaint_what_happened=source.get('complaint_what_happened')
        )
        complaints.append(this_complaint)
    return complaints


# load
@task
def store_complaints(parsed):
    insert_cmd = 'INSERT INTO complaint VALUES (?, ?, ?, ?, ?)'

    with closing(sqlite3.connect('cfpbcomplaints.db')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_cmd, parsed)
            conn.commit()


schedule = IntervalSchedule(interval=datetime.timedelta(minutes=1))

with Flow('my etl flow', schedule=schedule) as f:
    db_table = create_table()
    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)
    populated_table = store_complaints(parsed)
    populated_table.set_upstream(db_table)

f.run()
