/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#include <vector>
#include <thread>
#include <iostream>
#include <cstdio>
#include <ctime>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/net/ob_addr.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_gts_rpc.h"
#include "storage/tx/ob_gts_define.h"

int64_t total_cnt;

namespace oceanbase
{
using namespace common;
using namespace transaction;
using namespace obrpc;
namespace unittest
{

class MyResponseRpc : public ObIGtsResponseRpc
{
public:
  MyResponseRpc() {}
  ~MyResponseRpc() {}
public:
  void set_valid_arg(const uint64_t tenant_id,
                     const int status,
                     const ObAddr &sender,
                     const ObAddr self)
  {
    tenant_id_ = tenant_id;
    status_ = status;
    sender_ = sender;
    self_ = self;
  }
  int post(const uint64_t tenant_id, const ObAddr &server, const ObGtsErrResponse &msg)
  {
    int ret = OB_SUCCESS;
    if (!msg.is_valid() ||
        tenant_id != tenant_id_ ||
        msg.get_status() != status_ ||
        msg.get_sender() != sender_ ||
        server != self_) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(server), K(msg), K(*this));
    }
    return ret;
  }
  TO_STRING_KV(K_(tenant_id), K_(status), K_(sender), K_(self));
private:
  uint64_t tenant_id_;
  int status_;
  ObAddr sender_;
  ObAddr self_;
};

class MyTimestampService : public ObTimestampService
{
public:
  MyTimestampService() {}
  ~MyTimestampService() {}
  void init(const ObAddr &self)
  {
    self_ = self;
    service_type_ = ServiceType::TimestampService;
    pre_allocated_range_ = TIMESTAMP_PREALLOCATED_RANGE;
    last_id_ = 0;
    limited_id_ = ObTimeUtility::current_time_ns() + TIMESTAMP_PREALLOCATED_RANGE;
    ATOMIC_STORE(&last_gts_, 0);
    ATOMIC_STORE(&last_request_ts_, 0);
    ATOMIC_STORE(&check_gts_speed_lock_, 0);
  }
public:
  int handle_request(const ObGtsRequest &request, ObGtsRpcResult &result)
  {
    static int64_t total_cnt = 0;
    static int64_t total_rt = 0;
    static const int64_t STATISTICS_INTERVAL_US = 10000000;
    const MonotonicTs start = MonotonicTs::current_time();
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!request.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), K(request));
    } else {
      TRANS_LOG(DEBUG, "handle gts request", K(request));
      int64_t gts = 0;
      const MonotonicTs srr = request.get_srr();
      const uint64_t tenant_id = request.get_tenant_id();
      const ObAddr &requester = request.get_sender();
      int64_t end_id;
      if (requester == self_) {
        // Go local call to get gts
        TRANS_LOG(DEBUG, "handle local gts request", K(requester));
        ret = handle_local_request_(request, result);
      } else if (OB_FAIL(get_timestamp(gts))) {
        if (EXECUTE_COUNT_PER_SEC(10)) {
          TRANS_LOG(WARN, "get timestamp failed", KR(ret));
        }
        int tmp_ret = OB_SUCCESS;
        ObGtsErrResponse response;
        if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
          TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
        } else if (OB_SUCCESS != (tmp_ret = response.init(tenant_id, srr, ret, self_))) {
          TRANS_LOG(WARN, "gts err response init failed", K(tmp_ret), K(request));
        } else if (OB_SUCCESS != (tmp_ret = rpc_.post(tenant_id, requester, response))) {
          TRANS_LOG(WARN, "post gts err response failed", K(tmp_ret), K(response));
        } else {
          TRANS_LOG(DEBUG, "post gts err response success", K(response));
        }
      } else {
        if (OB_FAIL(result.init(tenant_id, ret, srr, gts, gts))) {
          TRANS_LOG(WARN, "gts result init failed", KR(ret), K(request));
        }
      }
    }
    //obtain gts information:
    //(1) How many gts requests are processed per second
    //(2) How long does it take to process a request
    const MonotonicTs end = MonotonicTs::current_time();
    const int64_t cost_us = request.get_srr().mts_ - end.mts_;
    //Print the gts request that takes a long time for network transmission
    if (cost_us > 500 * 1000) {
      TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "gts request fly too much time", K(request), K(result), K(cost_us));
    }
    ATOMIC_INC(&total_cnt);
    // ObTransStatistic::get_instance().add_gts_request_total_count(request.get_tenant_id(), 1);
    (void)ATOMIC_FAA(&total_rt, end.mts_ - start.mts_);
    if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL_US)) {
      TRANS_LOG(INFO, "handle gts request statistics", K(total_rt), K(total_cnt),
          "avg_rt", (double)total_rt / (double)(total_cnt + 1),
          "avg_cnt", (double)total_cnt / (double)(STATISTICS_INTERVAL_US / 1000000));
      ATOMIC_STORE(&total_cnt, 0);
      ATOMIC_STORE(&total_rt, 0);
    }
    return ret;
  }
  int handle_local_request_(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result)
  {
    int ret = OB_SUCCESS;
    int64_t gts = 0;
    const uint64_t tenant_id = request.get_tenant_id();
    const MonotonicTs srr = request.get_srr();
    int64_t end_id;
    if (OB_FAIL(get_number(1, ObTimeUtility::current_time_ns(), gts, end_id))) {
      TRANS_LOG(WARN, "get timestamp failed", KR(ret));
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = result.init(tenant_id, ret, srr, 0, 0))) {
        TRANS_LOG(WARN, "gts result init failed", K(tmp_ret), K(request));
      }
    } else {
      if (OB_FAIL(result.init(tenant_id, ret, srr, gts, gts))) {
        TRANS_LOG(WARN, "local gts result init failed", KR(ret), K(request));
      }
    }
    return ret;
  }
private:
  int64_t last_gts_;
  // the time of last request，updated periodically, nanosecond
  int64_t last_request_ts_;
  // the lock of checking the gts service's advancing speed, used in get_timestamp to avoid
  // concurrent threads all pushing the gts ahead
  int64_t check_gts_speed_lock_;

  int get_timestamp(int64_t &gts)
  {
    int ret = OB_SUCCESS;
    int64_t unused_id;
    // 100ms
    const int64_t CHECK_INTERVAL = 100000000;
    const int64_t current_time = ObClockGenerator::getClock() * 1000;
    int64_t last_request_ts = ATOMIC_LOAD(&last_request_ts_);
    int64_t time_delta = current_time - last_request_ts;

    ret = get_number(1, current_time, gts, unused_id);

    if (OB_SUCC(ret)) {
      if ((last_request_ts == 0 || time_delta < 0) && ATOMIC_BCAS(&check_gts_speed_lock_, 0, 1)) {
        last_request_ts = ATOMIC_LOAD(&last_request_ts_);
        time_delta = current_time - last_request_ts;
        // before, we only do a fast check, and we should check again after we get the lock
        if (last_request_ts == 0 || time_delta < 0) {
          ATOMIC_STORE(&last_request_ts_, current_time);
          ATOMIC_STORE(&last_gts_, gts);
        }
        ATOMIC_STORE(&check_gts_speed_lock_, 0);
      } else if (time_delta > CHECK_INTERVAL && ATOMIC_BCAS(&check_gts_speed_lock_, 0, 1)) {
        last_request_ts = ATOMIC_LOAD(&last_request_ts_);
        time_delta = current_time - last_request_ts;
        // before, we only do a fast check, and we should check again after we get the lock
        if (time_delta > CHECK_INTERVAL) {
          const int64_t last_gts = ATOMIC_LOAD(&last_gts_);
          const int64_t gts_delta = gts - last_gts;
          const int64_t compensation_threshold = time_delta / 2;
          const int64_t compensation_value = time_delta / 10;
          // if the gts service advanced too slowly, then we add it up with `compensation_value`
          if (time_delta - gts_delta > compensation_threshold) {
            ret = get_number(compensation_value, current_time, gts, unused_id);
            TRANS_LOG(WARN, "the gts service advanced too slowly", K(ret), K(current_time),
                K(last_request_ts), K(time_delta), K(last_gts), K(gts), K(gts_delta),
                K(compensation_value));
          }
          if (OB_SUCC(ret)) {
            ATOMIC_STORE(&last_request_ts_, current_time);
            ATOMIC_STORE(&last_gts_, gts);
          }
          TRANS_LOG(DEBUG, "check the gts service advancing speed", K(ret), K(current_time),
              K(last_request_ts), K(time_delta), K(last_gts), K(gts), K(gts_delta),
              K(compensation_value));
        }
        ATOMIC_STORE(&check_gts_speed_lock_, 0);
      }
    }

    return ret;
  }
  int get_number(const int64_t range, const int64_t base_id, int64_t &start_id, int64_t &end_id)
  {
    int ret = OB_SUCCESS;
    int64_t tmp_id = 0;
    const int64_t last_id = ATOMIC_LOAD(&last_id_);
    int64_t limit_id = ATOMIC_LOAD(&limited_id_);
    const int64_t allocated_range = min(min(limit_id - base_id, limit_id - last_id), range);
    if (allocated_range <= 0) {
      ret = OB_EAGAIN;
    } else {
      if (base_id > last_id) {
        if (ATOMIC_BCAS(&last_id_, last_id, base_id + allocated_range)) {
          tmp_id = base_id;
        } else {
          tmp_id = ATOMIC_FAA(&last_id_, allocated_range);
        }
      } else {
        tmp_id = ATOMIC_FAA(&last_id_, allocated_range);
      }
      // Caution: get limit id again, compete with switch_to_follower_gracefully
      limit_id = ATOMIC_LOAD(&limited_id_);
      if (tmp_id >= limit_id) {
        ret = OB_EAGAIN;
      } else {
        start_id = tmp_id;
        end_id = min(start_id + allocated_range, limit_id);
      }
    }
    if (OB_EAGAIN == ret || (limited_id_ - last_id_) < (pre_allocated_range_ * 2 / 3)) {
      const int64_t pre_allocated_id = min(max_pre_allocated_id_(base_id), max(base_id, limited_id_) + max(range * 10, pre_allocated_range_));
      submit_log_with_lock_(pre_allocated_id, pre_allocated_id);
    }
    if (TC_REACH_TIME_INTERVAL(100000)) {
      TRANS_LOG(INFO, "get number", K(ret), K(service_type_), K(range), K(base_id), K(start_id), K(end_id));
    }
    return ret;
  }

  int64_t max_pre_allocated_id_(const int64_t base_id)
  {
    int64_t max_pre_allocated_id = INT64_MAX;
    if (TimestampService == service_type_) {
      if (base_id > ATOMIC_LOAD(&limited_id_)) {
        (void)inc_update(&last_id_, base_id);
      }
      max_pre_allocated_id = ATOMIC_LOAD(&last_id_) + 2 * pre_allocated_range_;
    }
    return max_pre_allocated_id;
  }
  
  MyResponseRpc rpc_;
};

class TestObGtsMgr : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

//////////////////////basic function test//////////////////////////////////////////

TEST_F(TestObGtsMgr, handle_gts_request_by_leader)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ts_service.init(server);

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, client);
  ObGtsRequest request;
  ObGtsRpcResult result;
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, client));
  EXPECT_EQ(OB_SUCCESS, ts_service.handle_request(request, result));
  EXPECT_EQ(tenant_id, result.get_tenant_id());
  EXPECT_EQ(OB_SUCCESS, result.get_status());
  EXPECT_EQ(srr, result.get_srr());
  EXPECT_EQ(ts_range - 1, result.get_gts_end() - result.get_gts_start());
  //EXPECT_TRUE(result.get_gts_start() >= srr);
}

TEST_F(TestObGtsMgr, handle_local_gts_request)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ts_service.init(server);

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, server);
  ObGtsRequest request;
  ObGtsRpcResult result;
  EXPECT_EQ(OB_SUCCESS, request.init(tenant_id, srr, ts_range, server));
  EXPECT_EQ(OB_SUCCESS, ts_service.handle_request(request, result));
  EXPECT_EQ(tenant_id, result.get_tenant_id());
  EXPECT_EQ(OB_SUCCESS, result.get_status());
  EXPECT_EQ(srr, result.get_srr());
  EXPECT_EQ(ts_range - 1, result.get_gts_end() - result.get_gts_start());
  // EXPECT_TRUE(result.get_gts_start() >= srr);
}

TEST_F(TestObGtsMgr, invalid_argument)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const MonotonicTs stc;
  const int64_t ts_range = 1;
  ObGtsRequest request;
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(0, srr, ts_range, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, stc, ts_range, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, srr, 0, client));
  EXPECT_EQ(OB_INVALID_ARGUMENT, request.init(tenant_id, srr, ts_range, ObAddr()));
}

void f(MyTimestampService &ts_service, const ObGtsRequest &request, ObGtsRpcResult &result) {
  std::time_t c = std::time(nullptr);
  int64_t cnt = 0;
  while (std::difftime(time(nullptr), c) < 30) {
    ts_service.handle_request(request, result);
    cnt++;
  }
  ::total_cnt += cnt;
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::unittest;

int main(int argc, char **argv)
{
  const ObAddr client(ObAddr::IPV4, "10.0.0.1", 10000);
  const ObAddr server(ObAddr::IPV4, "10.0.0.1", 20000);
  MyTimestampService ts_service;
  MyResponseRpc response_rpc;
  ts_service.init(server);

  const uint64_t tenant_id = 1001;
  const MonotonicTs srr = MonotonicTs::current_time();
  const int64_t ts_range = 1;
  response_rpc.set_valid_arg(tenant_id, OB_SUCCESS, server, client);
  ObGtsRequest request;
  ObGtsRpcResult result;
  request.init(tenant_id, srr, ts_range, client);
  std::vector<std::thread> threads;
  const int THREAD_NUM = 100;
  for (int i = 0; i < THREAD_NUM; i++) {
    threads.emplace_back(std::thread(&oceanbase::unittest::f, std::ref(ts_service), std::cref(request), std::ref(result)));
  }
  for (int i = 0; i < THREAD_NUM; i++) {
    threads[i].join();
  }
  std::cout << ::total_cnt << std::endl;
  printf("%.0f\n", ::total_cnt / 30.0);
  return 0;
}
