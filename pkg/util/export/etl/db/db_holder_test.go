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
	"database/sql"
	"regexp"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestBulkInsert(t *testing.T) {

	tbl := &table.Table{
		Account:  "test",
		Database: "testDB",
		Table:    "testTable",
		Columns: []table.Column{
			{Name: "str", ColType: table.TVarchar, Scale: 32, Default: "", Comment: "str column"},
			{Name: "int64", ColType: table.TInt64, Default: "0", Comment: "int64 column"},
			{Name: "float64", ColType: table.TFloat64, Default: "0.0", Comment: "float64 column"},
			{Name: "uint64", ColType: table.TUint64, Default: "0", Comment: "uint64 column"},
			{Name: "datetime_6", ColType: table.TDatetime, Default: "", Comment: "datetime.6 column"},
			{Name: "json_col", ColType: table.TJson, Default: "{}", Comment: "json column"},
		},
	}

	records := [][]string{
		{"str1", "1", "1.1", "1", "2023-05-16T00:00:00Z", `{"key1":"value1 \n test , \r 'test'"}`},
		{"str2", "2", "2.2", "2", "2023-05-16T00:00:00Z", `{"key2":"value2"}`},
	}

	db, mock, err := sqlmock.New() // creating sqlmock
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	mock.ExpectExec(regexp.QuoteMeta(`LOAD DATA INLINE FORMAT='csv', DATA='str1,1,1.1,1,2023-05-16T00:00:00Z,"{""key1"":""value1 \\n test , \\r ''test''""}"
str2,2,2.2,2,2023-05-16T00:00:00Z,"{""key2"":""value2""}"
' INTO TABLE testDB.testTable`)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bulkInsert(ctx, db, records, tbl)

	err = mock.ExpectationsWereMet()
	if err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestIsRecordExisted(t *testing.T) {
	// Mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Mock DBConnProvider function for the test
	mockDBConnProvider := func() (*sql.DB, error) {
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tbl := &table.Table{Table: "statement_info"}
	record := []string{"test-statement-id", "success"}

	// Define expectations for the SQL query.
	mock.ExpectQuery("SELECT EXISTS(.+)").
		WithArgs(record[0], record[1]).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true)) // Mocking a result that indicates the record exists.

	// Call the function with the mocked database connection provider.
	exists, err := IsRecordExisted(ctx, record, tbl, mockDBConnProvider)
	if err != nil {
		t.Errorf("error was not expected while checking if record exists: %s", err)
	}
	if !exists {
		t.Errorf("expected record to exist, but it does not")
	}

	// Ensure all expectations are met.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
