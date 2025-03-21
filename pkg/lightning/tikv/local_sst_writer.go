// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"encoding/binary"

	rocks "github.com/cockroachdb/pebble"
	rocksbloom "github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	rockssst "github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// writeCFWriter generates SST files for TiKV's write column family.
type writeCFWriter struct {
	sstWriter *rockssst.Writer
	ts        uint64
}

// newWriteCFWriter creates a new writeCFWriter. Currently `identity` is acquired
// from a real SST file generated by TiKV.
func newWriteCFWriter(
	sstPath string,
	ts uint64,
) (*writeCFWriter, error) {
	f, err := vfs.Default.Create(sstPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	writable := objstorageprovider.NewFileWritable(f)
	writer := rockssst.NewWriter(writable, rockssst.WriterOptions{
		// TODO(lance6716): should read TiKV config to know these values.
		BlockSize:   32 * 1024,
		Compression: rocks.ZstdCompression,
		// TODO(lance6716): should check the behaviour is the exactly same.
		FilterPolicy: rocksbloom.FilterPolicy(10),
		MergerName:   "nullptr",
		TablePropertyCollectors: []func() rockssst.TablePropertyCollector{
			func() rockssst.TablePropertyCollector {
				return newMVCCPropCollector(ts)
			},
			func() rockssst.TablePropertyCollector {
				return newRangePropertiesCollector()
			},
			// titan is only triggered when SST compaction at TiKV side.
			func() rockssst.TablePropertyCollector {
				return mockCollector{name: "BlobFileSizeCollector"}
			},
		},
	})
	return &writeCFWriter{sstWriter: writer, ts: ts}, nil
}

// set mimic TiKV's TxnSstWriter logic to encode key and value and write to SST.
func (w *writeCFWriter) set(key, value []byte) error {
	intest.Assert(isShortValue(value), "not implemented, need to write to default CF")

	// key layout in this case:
	// z{mem-comparable encoded key}{bit-wise reversed TS}
	actualKey := make([]byte, 0, 1+codec.EncodedBytesLength(len(key))+8)
	// keys::data_key will add the 'z' prefix [1] at `TxnSstWriter.put` [2].
	//
	// [1] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/keys/src/lib.rs#L206
	// [2] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/sst_importer/src/sst_writer.rs#L92
	actualKey = append(actualKey, 'z')
	// Key::from_raw [3] will encode the key as bytes at `TxnSstWriter.write` [4],
	// which is the caller of `TxnSstWriter.put` [2].
	//
	// [3] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/txn_types/src/types.rs#L55
	// [4] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/sst_importer/src/sst_writer.rs#L74
	actualKey = codec.EncodeBytes(actualKey, key)
	// Key::append_ts [5] will append the bit-wise reverted ts at
	// `TxnSstWriter.write` [4].
	//
	// [5] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/txn_types/src/types.rs#L118
	actualKey = binary.BigEndian.AppendUint64(actualKey, ^w.ts)

	// value layout in this case:
	// P{varint-encoded TS}v{value length}{value}
	actualValue := make([]byte, 0, 1+binary.MaxVarintLen64+1+1+len(value))
	// below logic can be found at `WriteRef.to_bytes` [6].
	//
	// [6] https://github.com/tikv/tikv/blob/7793f1d5dc40206fe406ca001be1e0d7f1b83a8f/components/txn_types/src/write.rs#L362
	actualValue = append(actualValue, 'P')
	actualValue = binary.AppendUvarint(actualValue, w.ts)
	actualValue = append(actualValue, 'v')
	actualValue = append(actualValue, byte(len(value)))
	actualValue = append(actualValue, value...)

	return errors.Trace(w.sstWriter.Set(actualKey, actualValue))
}

func (w *writeCFWriter) close() error {
	return errors.Trace(w.sstWriter.Close())
}

func isShortValue(val []byte) bool {
	return len(val) <= 255
}
