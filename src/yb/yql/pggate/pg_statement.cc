//--------------------------------------------------------------------------------------------------
// Copyright (c) YugaByte, Inc.
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
//--------------------------------------------------------------------------------------------------

#include "yb/yql/pggate/pg_statement.h"

#include "yb/client/yb_op.h"

#include "yb/util/debug-util.h"

namespace yb {
namespace pggate {

//--------------------------------------------------------------------------------------------------
// Class PgStatement
//--------------------------------------------------------------------------------------------------

PgStatement::PgStatement(PgSession::ScopedRefPtr pg_session)
    : pg_session_(std::move(pg_session)), arena_(std::make_shared<Arena>()) {
}

PgStatement::~PgStatement() {
}

}  // namespace pggate
}  // namespace yb
