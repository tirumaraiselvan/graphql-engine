#!/usr/bin/env python3

import pytest
from datetime import datetime
from datetime import timedelta
from croniter import croniter
from validate import validate_event_webhook,validate_event_headers
from _queue import Empty
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

@pytest.mark.usefixtures("evts_webhook")
class TestScheduledTrigger(object):

    cron_trigger_name = "a_scheduled_trigger"
    adhoc_trigger_name = "adhoc_trigger"
    cron_schedule = None
    init_time = None
    webhook_payload = {"foo":"baz"}
    webhook_path = "/hello"
    retries = 12
    interval_in_secs = 5.0

    @classmethod
    def dir(cls):
        return 'queries/scheduled_triggers'

    def test_create_schedule_triggers(self,hge_ctx,evts_webhook):
        q = {
            "type":"run_sql",
            "args":{
                "sql":"set time zone 'UTC'"
            }
        }
        st,resp = hge_ctx.v1q(q)
        assert st == 200,resp
        next_min = (datetime.utcnow() + timedelta(minutes=1)).minute
        current_time_str = stringify_datetime(datetime.utcnow())
        # Setting the cron schedule to with the minute offset of the next min.
        # Doing this will help test for the first scheduled generated event.
        TestScheduledTrigger.cron_schedule = "{} * * * *".format(next_min)
        cron_st_api_query = {
            "type":"create_scheduled_trigger",
            "args":{
                "name":self.cron_trigger_name,
                "webhook":"http://127.0.0.1:5592" + "/foo",
                "schedule":{
                    "type":"cron",
                    "value":self.cron_schedule
                },
                "headers":[
                    {
                        "name":"foo",
                        "value":"baz"
                    }
                ],
                "payload":self.webhook_payload
            }
        }
        adhoc_st_api_query = {
            "type":"create_scheduled_trigger",
            "args":{
                "name":self.adhoc_trigger_name,
                "webhook":"http://127.0.0.1:5592" + self.webhook_path,
                "schedule":{
                    "type":"adhoc",
                    "value":current_time_str
                },
                "payload":self.webhook_payload,
                "headers":[
                    {
                        "name":"header-1",
                        "value":"header-1-value"
                    }
                ]
            }
        }
        url = '/v1/query'
        cron_st_code,cron_st_resp,_ = hge_ctx.anyq(url,cron_st_api_query,{})
        TestScheduledTrigger.init_time = datetime.utcnow()
        adhoc_st_code,adhoc_st_resp,_ = hge_ctx.anyq(url,adhoc_st_api_query,{})
        assert cron_st_code == adhoc_st_code == 200
        assert cron_st_resp['message'] ==  adhoc_st_resp['message'] == 'success'

    def test_check_generated_scheduled_events(self,hge_ctx,evts_webhook):
        future_schedule_timestamps = []
        iter = croniter(self.cron_schedule,self.init_time)
        for i in range(100):
            future_schedule_timestamps.append(iter.next(datetime))
        sql = '''
    select scheduled_time from hdb_catalog.hdb_scheduled_events where
        name = '{}' order by scheduled_time asc;
    '''
        q = {
            "type":"run_sql",
            "args":{
                "sql":sql.format(self.cron_trigger_name)
            }
        }
        st,resp = hge_ctx.v1q(q)
        assert st == 200
        ts_resp = resp['result'][1:]
        assert len(ts_resp) == 100 # 100 events are generated in a cron ST
        scheduled_events_ts = []
        for ts in ts_resp:
            datetime_ts = datetime.strptime(ts[0],"%Y-%m-%d %H:%M:%S")
            scheduled_events_ts.append(datetime_ts)
        assert future_schedule_timestamps == scheduled_events_ts
        adhoc_event_st,adhoc_event_resp = get_events_of_scheduled_trigger(hge_ctx,self.adhoc_trigger_name)
        assert int(adhoc_event_resp['result'][1][0]) == 1 # An adhoc ST should create exactly one schedule event

    def test_check_webhook_event(self,hge_ctx,evts_webhook):
        counter = 0
        queue_size = 2 # 1 adhoc ST and 1st scheduled event of the cron ST
        queue_counter = 0
        while (counter < self.retries):
            try:
                ev_full = evts_webhook.get_event(3)
                queue_counter = queue_counter + 1
                if ev_full['path'] == '/hello':
                    validate_event_headers(ev_full['headers'],{"header-1":"header-1-value"})
                    assert ev_full['body'] == self.webhook_payload
                elif ev_full['path'] == '/foo':
                    validate_event_headers(ev_full['headers'],{"foo":"baz"})
                counter = counter + 1
                if queue_counter == queue_size:
                    break
            except Empty:
                # Instead of waiting for a full minute for this test,
                # check if the webhook has been hit every `self.interval_in_secs`
                # seconds for `self.retries` times
                counter = counter + 1
                time.sleep(self.interval_in_secs)
            except Exception as e:
                print("unknown exception", e)
                assert False # unexpected exception
                return
        assert queue_counter == queue_size

    # Apart from checking that no additional events were triggered.
    # This test is necessary because if there are events in the webhook queue
    # the test suite won't shut down, until the events are popped from the queue.
    def test_empty_webhook_queue(self,hge_ctx,evts_webhook):
        counter = 0
        while True:
            try:
                ev_full = evts_webhook.get_event(3)
                counter = counter + 1
            except Empty:
                assert counter == 0
                return

    def test_delete_scheduled_triggers(self,hge_ctx):
        st_code,resp = hge_ctx.v1q_f(self.dir() + '/basic/teardown.yaml')
        assert st_code == 200,resp
