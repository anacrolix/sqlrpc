package sqlrpc

import (
	"database/sql"
	"database/sql/driver"

	"github.com/anacrolix/missinggo/slices"
	"github.com/bradfitz/iter"
)

type Service struct {
	Server interface {
		newRef(interface{}) RefId
		popRef(RefId) (interface{}, error)
		ref(RefId) (interface{}, error)
		releaseRef(RefId) error
		db() *sql.DB
	}
}

func (me *Service) Begin(args struct{}, txId *RefId) (err error) {
	tx, err := me.Server.db().Begin()
	if err != nil {
		return
	}
	*txId = me.Server.newRef(tx)
	return
}

func (me *Service) Commit(txId RefId, reply *struct{}) (err error) {
	_tx, err := me.Server.popRef(txId)
	if err != nil {
		return
	}
	tx := _tx.(*sql.Tx)
	return tx.Commit()
}

func (me *Service) Rollback(txId RefId, reply *struct{}) (err error) {
	_tx, err := me.Server.popRef(txId)
	if err != nil {
		return
	}
	tx := _tx.(*sql.Tx)
	return tx.Rollback()
}

func (me *Service) Prepare(args PrepareArgs, stmtRef *RefId) (err error) {
	var ppr interface {
		Prepare(string) (*sql.Stmt, error)
	}
	if args.InTx {
		var sqlObj interface{}
		sqlObj, err = me.Server.ref(args.TxId)
		if err != nil {
			return
		}
		ppr = sqlObj.(*sql.Tx)
	} else {
		ppr = me.Server.db()
	}
	stmt, err := ppr.Prepare(args.Query)
	if err != nil {
		return
	}
	*stmtRef = me.Server.newRef(stmt)
	return
}

func (me *Service) Query(args QueryArgs, reply *RowsReply) (err error) {
	_stmt, err := me.Server.ref(args.StmtRef)
	if err != nil {
		return
	}
	stmt := _stmt.(*sql.Stmt)
	rows, err := stmt.Query(slices.ToEmptyInterface(driverNamedValuesToNamedArgs(args.Values))...)
	if err != nil {
		return
	}
	reply.Columns, err = rows.Columns()
	reply.RowsId = me.Server.newRef(rows)
	return
}

func (me *Service) RowsClose(rowsId RefId, reply *interface{}) (err error) {
	_rows, err := me.Server.popRef(rowsId)
	if err != nil {
		return
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
	_stmt, err := me.Server.ref(args.StmtRef)
	if err != nil {
		return
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
	_rows, err := me.Server.ref(args.RowsRef)
	if err != nil {
		return
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
	err = me.Server.releaseRef(stmtRef)
	return
}
