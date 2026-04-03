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

package ddl

import (
	"context"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
)

type mockCodec struct {
	tikv.Codec
}

func (mockCodec) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	return append([]byte("k:"), start...), append([]byte("k:"), end...)
}

type mockHelperStorage struct {
	helper.Storage
	codec tikv.Codec
}

func (s mockHelperStorage) GetCodec() tikv.Codec {
	return s.codec
}

type mockPDHTTPClient struct {
	pdhttp.Client
	regionInfos []*pdhttp.RegionsInfo
	callCount   int
	firstRange  *pdhttp.KeyRange
	firstLimit  int
}

func (c *mockPDHTTPClient) GetRegionsByKeyRange(_ context.Context, keyRange *pdhttp.KeyRange, limit int) (*pdhttp.RegionsInfo, error) {
	if c.callCount == 0 {
		c.firstRange = keyRange
		c.firstLimit = limit
	}
	if c.callCount >= len(c.regionInfos) {
		return &pdhttp.RegionsInfo{}, nil
	}
	info := c.regionInfos[c.callCount]
	c.callCount++
	return info, nil
}

func expectedRegionRange(tableID int64) ([]byte, []byte) {
	tableStart, tableEnd := tablecodec.GetTableHandleKeyRange(tableID)
	return mockCodec{}.EncodeRegionRange(tableStart, tableEnd)
}

func TestEstimateTableSizeByIDPrefersApproximateKVSize(t *testing.T) {
	pdCli := &mockPDHTTPClient{
		regionInfos: []*pdhttp.RegionsInfo{
			{
				Count: 2,
				Regions: []pdhttp.RegionInfo{
					{ID: 1, ApproximateSize: 64, ApproximateKvSize: 5},
					{ID: 2, ApproximateSize: 7, ApproximateKvSize: 0},
				},
			},
			{},
		},
	}

	size, err := estimateTableSizeByID(context.Background(), pdCli, mockHelperStorage{codec: mockCodec{}}, 42)
	require.NoError(t, err)
	require.Equal(t, int64(12*units.MiB), size)
	require.Equal(t, 2, pdCli.callCount)
	expectedStart, expectedEnd := expectedRegionRange(42)
	require.NotNil(t, pdCli.firstRange)
	require.Equal(t, 128, pdCli.firstLimit)
	require.Equal(t, expectedStart, pdCli.firstRange.StartKey)
	require.Equal(t, expectedEnd, pdCli.firstRange.EndKey)
}
