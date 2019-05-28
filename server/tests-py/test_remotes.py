#!/usr/bin/env python3

import pytest

from validate import check_query_f, check_query

class TestTopLevelMixedFields:

    @pytest.fixture(autouse=True)
    def transact(self, request, hge_ctx):
        print("In setup method")
        st_code, resp = hge_ctx.v1q_f('queries/remote_schemas/setup_mixed.yaml')
        assert st_code == 200, resp
        st_code, resp = hge_ctx.v1q_f('queries/remote_schemas/setup_remote_relationship.yaml')
        assert st_code == 200, resp
        yield
        st_code, resp = hge_ctx.v1q_f('queries/remote_schemas/teardown_mixed.yaml')
        assert st_code == 200, resp

    def test_basic(self, hge_ctx):
        check_query_f(hge_ctx, 'queries/remote_schemas/basic_mixed.yaml')

# class TestRemoteRelationships:

#     @pytest.fixture(autouse=True)
#     def transact(self, request, hge_ctx):
#         print("In setup method")
#         st_code, resp = hge_ctx.v1q_f('queries/remote_schemas/setup_relationship.yaml')
#         assert st_code == 200, resp
#         yield
#         st_code, resp = hge_ctx.v1q_f('queries/remote_schemas/teardown_relationship.yaml')
#         assert st_code == 200, resp

#     def test_basic(self, hge_ctx):
#         check_query_f(hge_ctx, 'queries/remote_schemas/basic_remote_relationship.yaml')

