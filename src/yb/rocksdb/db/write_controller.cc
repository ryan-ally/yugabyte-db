//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include "yb/rocksdb/db/write_controller.h"

#include <atomic>

#include "yb/rocksdb/env.h"

#include "yb/util/atomic.h"
#include "yb/util/flag_tags.h"

DEFINE_test_flag(bool, allow_stop_writes, true,
                 "Whether it is allowed to stop writes in tests.");

namespace rocksdb {

std::unique_ptr<WriteControllerToken> WriteController::GetStopToken() {
  CHECK(yb::GetAtomicFlag(&FLAGS_TEST_allow_stop_writes));
  ++total_stopped_;
  return std::unique_ptr<WriteControllerToken>(new StopWriteToken(this));
}

std::unique_ptr<WriteControllerToken> WriteController::GetDelayToken(
    uint64_t write_rate) {
  total_delayed_++;
  // Reset counters.
  last_refill_time_ = 0;
  bytes_left_ = 0;
  set_delayed_write_rate(write_rate);
  return std::unique_ptr<WriteControllerToken>(new DelayWriteToken(this));
}

std::unique_ptr<WriteControllerToken>
WriteController::GetCompactionPressureToken() {
  ++total_compaction_pressure_;
  return std::unique_ptr<WriteControllerToken>(
      new CompactionPressureToken(this));
}

bool WriteController::IsStopped() const { return total_stopped_ > 0; }
// This is inside DB mutex, so we can't sleep and need to minimize
// frequency to get time.
// If it turns out to be a performance issue, we can redesign the thread
// synchronization model here.
// The function trust caller will sleep micros returned.
uint64_t WriteController::GetDelay(Env* env, uint64_t num_bytes) {
  if (total_stopped_ > 0) {
    return 0;
  }
  if (total_delayed_ == 0) {
    return 0;
  }

  const uint64_t kMicrosPerSecond = 1000000;
  const uint64_t kRefillInterval = 1024U;

  if (bytes_left_ >= num_bytes) {
    bytes_left_ -= num_bytes;
    return 0;
  }
  // The frequency to get time inside DB mutex is less than one per refill
  // interval.
  auto time_now = env->NowMicros();

  uint64_t sleep_debt = 0;
  uint64_t time_since_last_refill = 0;
  if (last_refill_time_ != 0) {
    if (last_refill_time_ > time_now) {
      sleep_debt = last_refill_time_ - time_now;
    } else {
      time_since_last_refill = time_now - last_refill_time_;
      bytes_left_ +=
          static_cast<uint64_t>(static_cast<double>(time_since_last_refill) /
                                kMicrosPerSecond * delayed_write_rate_);
      if (time_since_last_refill >= kRefillInterval &&
          bytes_left_ > num_bytes) {
        // If refill interval already passed and we have enough bytes
        // return without extra sleeping.
        last_refill_time_ = time_now;
        bytes_left_ -= num_bytes;
        return 0;
      }
    }
  }

  uint64_t single_refill_amount =
      delayed_write_rate_ * kRefillInterval / kMicrosPerSecond;
  if (bytes_left_ + single_refill_amount >= num_bytes) {
    // Wait until a refill interval
    // Never trigger expire for less than one refill interval to avoid to get
    // time.
    bytes_left_ = bytes_left_ + single_refill_amount - num_bytes;
    last_refill_time_ = time_now + kRefillInterval;
    return kRefillInterval + sleep_debt;
  }

  // Need to refill more than one interval. Need to sleep longer. Check
  // whether expiration will hit

  // Sleep just until `num_bytes` is allowed.
  uint64_t sleep_amount =
      static_cast<uint64_t>(num_bytes /
                            static_cast<long double>(delayed_write_rate_) *
                            kMicrosPerSecond) +
      sleep_debt;
  last_refill_time_ = time_now + sleep_amount;
  return sleep_amount;
}

StopWriteToken::~StopWriteToken() {
  assert(controller_->total_stopped_ >= 1);
  --controller_->total_stopped_;
}

DelayWriteToken::~DelayWriteToken() {
  controller_->total_delayed_--;
  assert(controller_->total_delayed_ >= 0);
}

CompactionPressureToken::~CompactionPressureToken() {
  controller_->total_compaction_pressure_--;
  assert(controller_->total_compaction_pressure_ >= 0);
}

}  // namespace rocksdb
