#!/usr/bin/env python3

import pytest
from datetime import datetime
from datetime import timedelta
from croniter import croniter
from validate import validate_event_webhook
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
class TestSubscriptionTrigger(object):

    cron_trigger_name = ""
    adhoc_trigger_name = ""
    cron_schedule = "5 * * * *"
    init_time = datetime.now()
    webhook_payload = {"foo":"baz"}
    webhook_path = "/hello"

    def test_create_schedule_triggers(self,hge_ctx,evts_webhook):
        current_time_str = stringify_datetime(datetime.utcnow())
        TestSubscriptionTrigger.cron_trigger_name = "a scheduled trigger - " + current_time_str
        TestSubscriptionTrigger.adhoc_trigger_name = "adhoc trigger - " + current_time_str
        TestSubscriptionTrigger.cron_schedule = "5 * * * *"
        cron_st_api_query = {
            "type":"create_scheduled_trigger",
            "args":{
                "name":self.cron_trigger_name,
                "webhook":"http://example.com",
                "schedule":{
                    "type":"cron",
                    "value":self.cron_schedule
                }
            },
            "payload":"{}"
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
                "payload":self.webhook_payload
            }
        }
        url = '/v1/query'
        cron_st_code,cron_st_resp,_ = hge_ctx.anyq(url,cron_st_api_query,{})
        TestSubscriptionTrigger.init_time = datetime.utcnow()
        adhoc_st_code,adhoc_st_resp,_ = hge_ctx.anyq(url,adhoc_st_api_query,{})
        assert cron_st_code == adhoc_st_code == 200
        assert cron_st_resp['message'] ==  adhoc_st_resp['message'] == 'success'
        time.sleep(60.0)

    def test_check_generated_scheduled_events(self,hge_ctx,evts_webhook):
        future_schedule_timestamps = []
        iter = croniter(self.cron_schedule,self.init_time)
        for i in range(5):
            future_schedule_timestamps.append(iter.next(datetime))
        sql = '''
    select scheduled_time from hdb_catalog.hdb_scheduled_events where name = '{}' order by scheduled_time asc limit 5;
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
        scheduled_events_ts = []
        for ts in ts_resp:
            datetime_ts = datetime.strptime(ts[0],"%Y-%m-%d %H:%M:%S")
            scheduled_events_ts.append(datetime_ts)
        assert future_schedule_timestamps == scheduled_events_ts
        adhoc_event_st,adhoc_event_resp = get_events_of_scheduled_trigger(hge_ctx,self.adhoc_trigger_name)
        assert int(adhoc_event_resp['result'][1][0]) == 1

    def test_check_webhook_event(self,hge_ctx,evts_webhook):
        ev_full = evts_webhook.get_event(3)
        validate_event_webhook(ev_full['path'],self.webhook_path)
        assert ev_full['body'] == self.webhook_payload
