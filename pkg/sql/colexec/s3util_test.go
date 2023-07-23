// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package colexec

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSortKey(t *testing.T) {
	proc := testutil.NewProc()
	proc.Ctx = context.TODO()
	batch1 := &batch.Batch{
		Attrs: []string{"a"},
		Vecs: []*vector.Vector{
			testutil.MakeUint16Vector([]uint16{1, 2, 0}, nil),
		},
	}
	batch1.SetRowCount(3)
	err := sortByKey(proc, batch1, 0, false, proc.GetMPool())
	require.NoError(t, err)
	cols := vector.ExpandFixedCol[uint16](batch1.Vecs[0])
	for i := range cols {
		require.Equal(t, int(cols[i]), i)
	}

	batch2 := &batch.Batch{
		Attrs: []string{"a"},
		Vecs: []*vector.Vector{
			testutil.MakeTextVector([]string{"b", "a", "c"}, nil),
		},
	}
	batch2.SetRowCount(3)
	res := []string{"a", "b", "c"}
	err = sortByKey(proc, batch2, 0, false, proc.GetMPool())
	require.NoError(t, err)
	cols2 := vector.ExpandStrCol(batch2.Vecs[0])
	for i := range cols {
		require.Equal(t, cols2[i], res[i])
	}
}
