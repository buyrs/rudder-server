package postgres

import (
	"database/sql"
	"fmt"
	"strings"
)

type QueryExecution struct {
	txn                 *sql.Tx
	db                  *sql.DB
	query               string
	enableWithQueryPlan bool
}

// handleQueryExecution
// Print execution plan if enableWithQueryPlan is set to true else return result set.
// Currently, these statements are supported by EXPLAIN ANALYZE
// Any SELECT, INSERT, UPDATE, DELETE, VALUES, EXECUTE, DECLARE, CREATE TABLE AS, or CREATE MATERIALIZED VIEW AS statement, whose execution plan you wish to see.
func handleQueryExecution(e *QueryExecution) (result sql.Result, err error) {
	sqlStatement := e.query

	if e.enableWithQueryPlan {
		sqlStatement = "EXPLAIN " + e.query

		var rows *sql.Rows
		if e.txn != nil {
			rows, err = e.txn.Query(sqlStatement)
		} else if e.db != nil {
			rows, err = e.db.Query(sqlStatement)
		} else {
			err = fmt.Errorf("[WH][POSTGRES] Not able to handle query execution for statement: %s as both txn and db are nil", sqlStatement)
			return
		}
		if err != nil {
			err = fmt.Errorf("[WH][POSTGRES] error occurred while handling transaction for query: %s with err: %w", sqlStatement, err)
			return
		}
		defer rows.Close()

		var response []string
		for rows.Next() {
			var s string
			if err = rows.Scan(&s); err != nil {
				err = fmt.Errorf("[WH][POSTGRES] Error occurred while processing destination revisionID query %+v with err: %w", e, err)
				return
			}
			response = append(response, s)
		}
		pkgLogger.Infof(fmt.Sprintf(`[WH][POSTGRES] Execution Query plan for statement: %s is %s`, sqlStatement, strings.Join(response, `
`)))
	}
	if e.txn != nil {
		result, err = e.txn.Exec(sqlStatement)
	} else if e.db != nil {
		result, err = e.db.Exec(sqlStatement)
	} else {
		err = fmt.Errorf("[WH][POSTGRES] Not able to handle query execution for statement: %s as both txn and db are nil", sqlStatement)
		return
	}
	return
}
