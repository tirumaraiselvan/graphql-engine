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

class TestSubscriptionTrigger(object):

    cron_trigger_name = ""
    adhoc_trigger_name = ""
    cron_schedule = "5 * * * *"
    init_time = datetime.now()

    def test_create_schedule_triggers(self,hge_ctx):
        current_time_str = stringify_datetime(datetime.now())
        TestSubscriptionTrigger.cron_trigger_name = "a scheduled trigger - " + current_time_str
        TestSubscriptionTrigger.adhoc_trigger_name = "adhoc trigger - " + current_time_str
        TestSubscriptionTrigger.cron_schedule = "5 * * * *"
        cron_st_api_query = {
            "type":"create_scheduled_trigger",
            "args":{
                "name":self.cron_trigger_name,
                "webhook":"http://127.0.0.1/5592",
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
                "webhook":"http://127.0.0.1/5592",
                "schedule":{
                    "type":"adhoc",
                    "value":current_time_str
                },
            },
            "payload":"{\"foo\":\"baz\"}"
        }
        url = '/v1/query'
        cron_st_code,cron_st_resp,_ = hge_ctx.anyq(url,cron_st_api_query,{})
        TestSubscriptionTrigger.init_time = datetime.utcnow()
        adhoc_st_code,adhoc_st_resp,_ = hge_ctx.anyq(url,adhoc_st_api_query,{})
        assert cron_st_code == adhoc_st_code == 200
        assert cron_st_resp['message'] ==  adhoc_st_resp['message'] == 'success'
        time.sleep(60.0)

    def test_check_generated_scheduled_events(self,hge_ctx):
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
        print("resp",resp)
        ts_resp = resp['result'][1:]
        scheduled_events_ts = []
        for ts in ts_resp:
            datetime_ts = datetime.strptime(ts[0],"%Y-%m-%d %H:%M:%S")
            scheduled_events_ts.append(datetime_ts)
        assert future_schedule_timestamps == scheduled_events_ts
        adhoc_event_st,adhoc_event_resp = get_events_of_scheduled_trigger(hge_ctx,self.adhoc_trigger_name)
        assert int(adhoc_event_resp['result'][1][0]) == 1


    # def test_cron_scheduled_trigger(self,hge_ctx,evts_webhook):
    #     current_time_str = stringify_datetime(datetime.now())
    #     trigger_name = "a scheduled trigger - " + current_time_str
    #     cron_schedule = "5 * * * *"
    #     q = {
    #         "type":"create_scheduled_trigger",
    #         "args":{
    #             "name":trigger_name,
    #             "webhook":"http://127.0.0.1/5592",
    #             "schedule":{
    #                 "type":"cron",
    #                 "value":cron_schedule
    #             }
    #         },
    #         "payload":"{}"
    #     }
    #     url = '/v1/query'
    #     st_code,st_resp,_ = hge_ctx.anyq(url,q,{})
    #     assert st_code == 200
    #     time.sleep(60.0)
    #     check_schedule_of_generated_events(hge_ctx,trigger_name,cron_schedule,datetime.utcnow())

    # def test_one_off_scheduled_trigger(self,hge_ctx,evts_webhook):
    #     time = datetime.now()
    #     current_time_str = stringify_datetime(time)
    #     trigger_name = "adhoc_trigger-" + current_time_str
    #     create_trigger_query = {
    #         "type":"create_scheduled_trigger",
    #         "args":{
    #             "name":trigger_name,
    #             "webhook":"http://127.0.0.1/5592",
    #             "schedule":{
    #                 "type":"adhoc",
    #                 "value":current_time_str
    #             },
    #         },
    #             "payload":"{\"foo\":\"baz\"}"
    #     }
    #     url = '/v1/query'
    #     st_code,resp,_ = hge_ctx.anyq(url,create_trigger_query,{})
    #     assert st_code == 200,resp
    #     check_if_scheduled_event_exists(hge_ctx,trigger_name,10,1.0) #max wait time for 10s
