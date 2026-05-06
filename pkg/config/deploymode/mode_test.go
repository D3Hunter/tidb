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

package deploymode

import (
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestModeJSON(t *testing.T) {
	data, err := json.Marshal(PremiumReserved)
	require.NoError(t, err)
	require.Equal(t, `"premium_reserved"`, string(data))

	var mode Mode
	require.NoError(t, json.Unmarshal([]byte(`"premium"`), &mode))
	require.Equal(t, Premium, mode)

	require.NoError(t, json.Unmarshal([]byte(`"premium_reserved"`), &mode))
	require.Equal(t, PremiumReserved, mode)

	require.ErrorContains(t, json.Unmarshal([]byte(`"unknown"`), &mode), `invalid deploy mode "unknown"`)
	require.Error(t, json.Unmarshal([]byte(`1`), &mode))
}

func TestModeText(t *testing.T) {
	var cfg struct {
		Mode Mode `toml:"deploy-mode"`
	}

	_, err := toml.Decode(`deploy-mode = "premium_reserved"`, &cfg)
	require.NoError(t, err)
	require.Equal(t, PremiumReserved, cfg.Mode)

	text, err := Premium.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "premium", string(text))
}

func TestCurrentMode(t *testing.T) {
	original := Get()
	t.Cleanup(func() {
		require.NoError(t, Set(original))
	})

	require.Equal(t, Premium, Get())
	require.NoError(t, Set(PremiumReserved))
	require.Equal(t, PremiumReserved, Get())
	require.ErrorContains(t, Set(Mode(100)), "invalid deploy mode")
}
