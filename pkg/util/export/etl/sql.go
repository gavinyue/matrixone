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

package etl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"go.uber.org/zap"
)

// MAX_CHUNK_SIZE is the maximum size of a chunk of records to be inserted in a single insert.
const MAX_CHUNK_SIZE = 1024 * 1024 * 4

const MAX_INSERT_TIME_LIMIT = 20 * time.Second

var _ SqlWriter = (*DefaultSqlWriter)(nil)

// DefaultSqlWriter SqlWriter is a writer that writes data to a SQL database.
type DefaultSqlWriter struct {
	ctx       context.Context
	csvWriter *CSVWriter
	tbl       *table.Table
	buffer    [][]string
	mux       sync.Mutex
}

func NewSqlWriter(ctx context.Context, tbl *table.Table, csv *CSVWriter) *DefaultSqlWriter {
	return &DefaultSqlWriter{
		ctx:       ctx,
		csvWriter: csv,
		tbl:       tbl,
	}
}

type SqlWriter interface {
	table.RowWriter
	WriteRowRecords(records [][]string, tbl *table.Table, is_merge bool) (int, error)
}

func (sw *DefaultSqlWriter) GetContent() string {
	return ""
}

func (sw *DefaultSqlWriter) WriteStrings(record []string) error {
	return nil
}

func (sw *DefaultSqlWriter) WriteRow(row *table.Row) error {
	sw.buffer = append(sw.buffer, row.ToStrings())
	return nil
}

func (sw *DefaultSqlWriter) flushBuffer(force bool) (int, error) {
	logutil.Debug("sqlWriter flushBuffer started", zap.Int("buffer size", len(sw.buffer)), zap.Bool("force", force))
	now := time.Now()
	sw.mux.Lock()
	defer sw.mux.Unlock()

	var err error
	var cnt int
	cnt, err = sw.WriteRowRecords(sw.buffer, sw.tbl, false)

	if err != nil {
		sw.dumpBufferToCSV()
	}
	_, err = sw.csvWriter.FlushAndClose()
	logutil.Debug("sqlWriter flushBuffer finished", zap.Int("cnt", cnt), zap.Error(err), zap.Duration("time", time.Since(now))
	return cnt, err
}

func (sw *DefaultSqlWriter) dumpBufferToCSV() error {
	if len(sw.buffer) == 0 {
		return nil
	}
	// write sw.buffer to csvWriter
	for _, row := range sw.buffer {
		sw.csvWriter.WriteStrings(row)
	}
	return nil
}

func (sw *DefaultSqlWriter) FlushAndClose() (int, error) {
	if sw.buffer != nil && len(sw.buffer) == 0 {
		return 0, nil
	}
	cnt, err := sw.flushBuffer(true)
	sw.buffer = nil
	sw.tbl = nil
	sw.csvWriter = nil
	return cnt, err
}

func bulkInsert(sqlDb *sql.DB, records [][]string, tbl *table.Table, maxLen int) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	baseStr := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", tbl.Database, tbl.Table)

	sb := strings.Builder{}
	defer sb.Reset()

	tx, err := sqlDb.Begin()
	if err != nil {
		return 0, err
	}

	for idx, row := range records {
		if len(row) == 0 {
			continue
		}

		sb.WriteString("(")
		for i, field := range row {
			if i != 0 {
				sb.WriteString(",")
			}
			if tbl.Columns[i].ColType == table.TJson {
				var js interface{}
				_ = json.Unmarshal([]byte(field), &js)
				escapedJSON, _ := json.Marshal(js)
				sb.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(strings.ReplaceAll(string(escapedJSON), "\\", "\\\\"), "'", "\\'")))
			} else {
				escapedStr := strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\'"), "'", "\\'")
				if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr[:tbl.Columns[i].Scale-1]))
				} else {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr))
				}
			}
		}
		sb.WriteString(")")

		if sb.Len() >= maxLen || idx == len(records)-1 {
			stmt := baseStr + sb.String() + ";"
			//time_limit := 3000 * time.Second
			//if tbl.Table == "rawlog" {
			//	time_limit = 3 * time.Second
			//}
			//ctx, cancel := context.WithTimeout(context.Background(), time_limit)
			//defer cancel() // it's important to ensure all paths call cancel to avoid resource leak
			_, err := tx.Exec(stmt)
			if err != nil {
				tx.Rollback()
				return 0, err
			}
			sb.Reset()
		} else {
			sb.WriteString(",")
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return len(records), nil
}

func (sw *DefaultSqlWriter) WriteRowRecords(records [][]string, tbl *table.Table, is_merge bool) (int, error) {

	var err error
	var cnt int
	dbConn, err := db_holder.InitOrRefreshDBConn(false)
	if err != nil {
		logutil.Error("sqlWriter db init failed", zap.Error(err))
		return 0, err
	}
	now := time.Now()
	cnt, err = bulkInsert(dbConn, records, tbl, MAX_CHUNK_SIZE)
	if err != nil {
		logutil.Error("sqlWriter bulk insert failed", zap.Error(err), zap.Duration("duration", time.Since(now)), zap.String("table", tbl.Table), zap.Int("record_count", len(records)))
		return 0, err
	} else {
		logutil.Debug("sqlWriter finished the bulk insert", zap.Duration("duration", time.Since(now)), zap.String("table", tbl.Table), zap.Int("record_count", len(records)))
	}
	return cnt, nil
}
