// Copyright 2022 Matrix Origin
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

package db_holder

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

func TestGetPrepareSQL(t *testing.T) {
	tbl := &table.Table{
		Database: "testDB",
		Table:    "testTable",
	}
	columns := 3
	rowNum := 10

	sqls := getPrepareSQL(tbl, columns, rowNum)

	if sqls.rowNum != rowNum {
		t.Errorf("Expected rowNum to be %d, but got %d", rowNum, sqls.rowNum)
	}

	if sqls.columns != columns {
		t.Errorf("Expected columns to be %d, but got %d", columns, sqls.columns)
	}

	expectedOneRow := "INSERT INTO `testDB`.`testTable` VALUES  (?,?,?)"
	if sqls.oneRow != expectedOneRow {
		t.Errorf("Expected oneRow to be %s, but got %s", expectedOneRow, sqls.oneRow)
	}
	expectedMultiRows := "INSERT INTO `testDB`.`testTable` VALUES (?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?)"
	if sqls.multiRows != expectedMultiRows {
		t.Errorf("Expected multiRows to be %s, but got %s", expectedMultiRows, sqls.multiRows)
	}
}
