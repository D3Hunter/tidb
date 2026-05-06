// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package deploymode stores the process-wide deployment mode for TiDB X
// (NextGen) deployments.
//
// TiDB X Premium Reserved keeps the Premium product capability set, but uses a
// fixed-resource deployment shape instead of the standard Premium elastic shape.
// The resource scope is decided when the cluster starts. TiDB-worker,
// TiKV-worker, and coprocessor-worker are not scaled on demand, so background
// tasks must not assume that additional worker resources will be created
// automatically when task demand increases.
//
// Premium Reserved adapts Premium behavior to that fixed-resource shape by
// avoiding TiKV-worker and coprocessor-worker deployment, and by merging TiDB
// and TiDB-worker behavior. User traffic and distributed task execution run
// directly on TiDB nodes, and they all run on the SYSTEM keyspace.
package deploymode
