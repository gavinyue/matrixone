// Copyright 2024 Matrix Origin
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

package fifocache

import (
	"hash/maphash"
	"unsafe"

	"golang.org/x/exp/constraints"
)

var seed = maphash.MakeSeed()

func ShardInt[T constraints.Integer](v T) uint8 {
	return uint8(maphash.Bytes(
		seed,
		unsafe.Slice(
			(*byte)(unsafe.Pointer(&v)),
			unsafe.Sizeof(v),
		),
	))
}
