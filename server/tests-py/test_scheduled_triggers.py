#!/usr/bin/env python3

import pytest
from datetime import datetime
from datetime import timedelta
from croniter import croniter
import time

def stringify_datetime(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def get_events_of_scheduled_trigger(hge_ctx,trigger_name):
    events_count_sql = '''
    select count(*) from hdb_catalog.hdb_scheduled_events where name = '{}'
    '''.format(trigger_name)
    q = {
        "type":"run_sql",
        "args":{
            "sql":events_count_sql
        }
    }
    return hge_ctx.v1q(q)

def check_schedule_of_generated_events(hge_ctx,trigger_name,cron,time):
    future_schedule_timestamps = []
    iter = croniter(cron,time)
    sql = '''
    select scheduled_time from hdb_catalog.hdb_scheduled_events where name = '{}' order by scheduled_time asc limit 5;
    '''
    q = {
        "type":"run_sql",
        "args":{
            "sql":sql.format(trigger_name)
        }
    }
    for i in range(5):
        future_schedule_timestamps.append(iter.next(datetime))
    st,resp = hge_ctx.v1q(q)
    assert st == 200
    ts_resp = resp['result'][1:]
    scheduled_events_ts = []
    for ts in ts_resp:
        datetime_ts = datetime.strptime(ts[0],"%Y-%m-%d %H:%M:%S")
        scheduled_events_ts.append(datetime_ts)
    assert future_schedule_timestamps == scheduled_events_ts


def check_if_scheduled_event_exists(hge_ctx,trigger_name,retries=1,interval=4.0):
    while (retries > 0):
        st,resp = get_events_of_scheduled_trigger(hge_ctx,trigger_name)
        if int(resp['result'][1][0]) == 1:             #parses the count from the commented_map
            return
        time.sleep(interval)
        retries = retries -1
    assert False       # not found any scheduled events, so fail the test

class TestSubscriptionTrigger(object):

    def test_cron_scheduled_trigger(self,hge_ctx,evts_webhook):
        current_time_str = stringify_datetime(datetime.now())
        trigger_name = "a scheduled trigger - " + current_time_str
        cron_schedule = "5 * * * *"
        q = {
            "type":"create_scheduled_trigger",
            "args":{
                "name":trigger_name,
                "webhook":"http://127.0.0.1/5592",
                "schedule":{
                    "type":"Cron",
                    "value":cron_schedule
                }
            },
            "payload":"{}"
        }
        url = '/v1/query'
        st_code,st_resp,_ = hge_ctx.anyq(url,q,{})
        time.sleep(60.0)
        check_schedule_of_generated_events(hge_ctx,trigger_name,cron_schedule,datetime.utcnow())
        assert st_code == 200,st_resp

    def test_one_off_scheduled_trigger(self,hge_ctx,evts_webhook):
        time = datetime.now()
        current_time_str = stringify_datetime(time)
        print("current time str is",current_time_str)
        trigger_name = "adhoc_trigger-" + current_time_str
        create_trigger_query = {
            "type":"create_scheduled_trigger",
            "args":{
                "name":trigger_name,
                "webhook":"http://127.0.0.1/5592",
                "schedule":{
                    "type":"AdHoc",
                    "value":current_time_str
                },
            },
                "payload":"{\"foo\":\"baz\"}"
        }
        url = '/v1/query'
        st_code,resp,_ = hge_ctx.anyq(url,create_trigger_query,{})
        assert st_code == 200,resp
        check_if_scheduled_event_exists(hge_ctx,trigger_name,10,6.0)
