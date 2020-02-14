#!/usr/bin/env python3

import pytest
from datetime import datetime

class TestCreateSubscriptionTrigger(object):

    def test_cron_scheduled_trigger(self,hge_ctx,evts_webhook):
        current_time_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        q = {
            "type":"create_scheduled_trigger",
            "args":{
                "name":"a scheduled trigger" + current_time_str,
                "webhook":"http://127.0.0.1/5592",
                "schedule":{
                    "type":"Cron",
                    "value":"* * * * *"
                }
            },
            "payload":"{}"
        }
        url = '/v1/query'
        st_code,resp,_ = hge_ctx.anyq(url,q,{})
        assert st_code == 200,resp

    def test_one_off_scheduled_trigger(self,hge_ctx,evts_webhook):
        current_time_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        create_trigger_query = {
            "type":"create_scheduled_trigger",
            "args":{
                "name":"one_off_trigger" + current_time_str,
                "webhook":"http://127.0.0.1/5592",
                "schedule":{
                    "type":"OneOff",
                    "value":current_time_str
                },
            },
                "payload":"{\"foo\":\"baz\"}"
        }
        url = '/v1/query'
        st_code,resp,_ = hge_ctx.anyq(url,create_trigger_query,{})
        assert st_code == 200,resp
