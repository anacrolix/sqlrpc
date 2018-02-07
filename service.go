package sqlrpc

import (
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/anacrolix/missinggo/slices"
	"github.com/anacrolix/sqlrpc/refs"
	"github.com/bradfitz/iter"
)

var errBadRef = errors.New("bad ref")

type Service struct {
	Refs *refs.Manager
	DB   *sql.DB
}

func (me *Service) Begin(args struct{}, txId *RefId) (err error) {
	tx, err := me.DB.Begin()
	if err != nil {
		return
	}
	*txId = me.Refs.New(tx, tx.Rollback)
	return
}

func (me *Service) Commit(txId RefId, reply *struct{}) (err error) {
	_tx := me.Refs.Pop(txId)
	if _tx == nil {
		return errBadRef
	}
	tx := _tx.(*sql.Tx)
	return tx.Commit()
}

func (me *Service) Rollback(txId RefId, reply *struct{}) (err error) {
	_tx := me.Refs.Pop(txId)
	if _tx == nil {
		return errBadRef
	}
	tx := _tx.(*sql.Tx)
	return tx.Rollback()
}

func (me *Service) Prepare(args PrepareArgs, stmtRef *RefId) (err error) {
	var ppr interface {
		Prepare(string) (*sql.Stmt, error)
	}
	if args.InTx {
		sqlObj := me.Refs.Get(args.TxId)
		if sqlObj == nil {
			return errBadRef
		}
		ppr = sqlObj.(*sql.Tx)
	} else {
		ppr = me.DB
	}
	stmt, err := ppr.Prepare(args.Query)
	if err != nil {
		return
	}
	*stmtRef = me.Refs.New(stmt, stmt.Close)
	return
}

func (me *Service) Query(args QueryArgs, reply *RowsReply) (err error) {
	_stmt := me.Refs.Get(args.StmtRef)
	if _stmt == nil {
		return errBadRef
	}
	stmt := _stmt.(*sql.Stmt)
	rows, err := stmt.Query(slices.ToEmptyInterface(driverNamedValuesToNamedArgs(args.Values))...)
	if err != nil {
		return
	}
	reply.Columns, err = rows.Columns()
	reply.RowsId = me.Refs.New(rows, rows.Close)
	return
}

func (me *Service) RowsClose(rowsId RefId, reply *interface{}) (err error) {
	_rows := me.Refs.Pop(rowsId)
	if _rows == nil {
		return errBadRef
	}
	rows := _rows.(*sql.Rows)
	err = rows.Close()
	return
}

func driverNamedValuesToNamedArgs(nvs []driver.NamedValue) (nas []sql.NamedArg) {
	for _, nv := range nvs {
		nas = append(nas, sql.Named(nv.Name, nv.Value))
	}
	return
}

func (me *Service) ExecStmt(args ExecArgs, reply *ResultReply) (err error) {
	_stmt := me.Refs.Get(args.StmtRef)
	if _stmt == nil {
		return errBadRef
	}
	stmt := _stmt.(*sql.Stmt)
	res, err := stmt.Exec(slices.ToEmptyInterface(driverNamedValuesToNamedArgs(args.Values))...)
	if err != nil {
		return
	}
	reply.LastInsertId, reply.LastInsertIdErr = res.LastInsertId()
	reply.RowsAffected, reply.RowsAffectedErr = res.RowsAffected()
	return
}

func (me *Service) RowsNext(args RowsNextArgs, reply *RowsNextReply) (err error) {
	_rows := me.Refs.Get(args.RowsRef)
	if _rows == nil {
		return errBadRef
	}
	rows := _rows.(*sql.Rows)
	reply.Values = make([]interface{}, args.NumValues)
	dest := make([]interface{}, args.NumValues)
	for i := range iter.N(args.NumValues) {
		dest[i] = &reply.Values[i]
	}
	if rows.Next() {
		err = rows.Scan(dest...)
		return
	}
	err = rows.Err()
	if err == nil {
		reply.EOF = true
	}
	return
}

func (me *Service) CloseStmt(stmtRef RefId, reply *struct{}) (err error) {
	me.Refs.Release(stmtRef)
	return
}
