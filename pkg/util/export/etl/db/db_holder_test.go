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
	"context"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestBulkInsert(t *testing.T) {

	tbl := &table.Table{
		Account:  "test",
		Database: "testDB",
		Table:    "testTable",
		Columns: []table.Column{
			table.Column{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"},
			table.Column{Name: "int64", ColType: table.TInt64, Default: "0", Comment: "int64 column"},
			table.Column{Name: "float64", ColType: table.TFloat64, Default: "0.0", Comment: "float64 column"},
			table.Column{Name: "uint64", ColType: table.TUint64, Default: "0", Comment: "uint64 column"},
			table.Column{Name: "datetime_6", ColType: table.TDatetime, Default: "", Comment: "datetime.6 column"},
			table.Column{Name: "json_col", ColType: table.TJson, Default: "{}", Comment: "json column"},
		},
	}

	records := [][]string{
		{"str1", "1", "1.1", "1", "2023-05-16T00:00:00Z", `{"key1":"value1"}`},
		{"str2", "2", "2.2", "2", "2023-05-16T00:00:00Z", `{"key2":"value2"}`},
	}

	db, mock, err := sqlmock.New() // creating sqlmock
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectBegin()

	stmt := mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `testDB`.`testTable` VALUES (?,?,?,?,?,?)"))

	for _, record := range records {
		driverValues := make([]driver.Value, len(record))
		for i, v := range record {
			driverValues[i] = driver.Value(v)
		}
		stmt.ExpectExec().
			WithArgs(driverValues...).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error)
	go bulkInsert(ctx, done, db, records, tbl, MAX_CHUNK_SIZE)

	err = <-done
	if err != nil {
		t.Errorf("expected no error, but got: %v", err)
	}

	err = mock.ExpectationsWereMet()
	if err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
