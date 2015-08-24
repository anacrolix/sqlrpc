package sqlrpc

import (
	"database/sql"
	"log"
	"sync"

	"github.com/bradfitz/iter"
)

const logRefs = false

type Server struct {
	DB *sql.DB

	mu      sync.Mutex
	refs    map[int]interface{}
	nextRef int
}

func (me *Server) newRef(obj interface{}) (ret int) {
	me.mu.Lock()
	if me.refs == nil {
		me.refs = make(map[int]interface{})
	}
	for {
		if _, ok := me.refs[me.nextRef]; !ok {
			break
		}
		me.nextRef++
	}
	me.refs[me.nextRef] = obj
	ret = me.nextRef
	me.nextRef++
	if logRefs {
		log.Print(me.refs)
	}
	me.mu.Unlock()
	return
}

func (me *Server) popRef(id int) (ret interface{}) {
	me.mu.Lock()
	ret = me.refs[id]
	delete(me.refs, id)
	if logRefs {
		log.Print(me.refs)
	}
	me.mu.Unlock()
	return
}

func (me *Server) ref(id int) (ret interface{}) {
	me.mu.Lock()
	ret = me.refs[id]
	me.mu.Unlock()
	return
}

func (me *Server) Begin(args struct{}, txId *int) (err error) {
	tx, err := me.DB.Begin()
	if err != nil {
		return
	}
	*txId = me.newRef(tx)
	return
}

func (me *Server) Commit(txId int, reply *struct{}) (err error) {
	tx := me.popRef(txId).(*sql.Tx)
	return tx.Commit()
}

func (me *Server) Prepare(args PrepareArgs, stmtRef *int) (err error) {
	var ppr interface {
		Prepare(string) (*sql.Stmt, error)
	}
	if args.InTx {
		ppr = me.ref(args.TxId).(*sql.Tx)
	} else {
		ppr = me.DB
	}
	stmt, err := ppr.Prepare(args.Query)
	if err != nil {
		return
	}
	*stmtRef = me.newRef(stmt)
	return
}

func (me *Server) Query(args ExecArgs, reply *RowsReply) (err error) {
	stmt := me.ref(args.StmtRef).(*sql.Stmt)
	rows, err := stmt.Query(args.Values...)
	if err != nil {
		return
	}
	reply.RowsId = me.newRef(rows)
	reply.Columns, err = rows.Columns()
	return
}

func (me *Server) RowsClose(rowsId int, reply *interface{}) (err error) {
	err = me.popRef(rowsId).(*sql.Rows).Close()
	return
}

func (me *Server) ExecStmt(args ExecArgs, reply *ResultReply) (err error) {
	stmt := me.ref(args.StmtRef).(*sql.Stmt)
	res, err := stmt.Exec(args.Values...)
	if err != nil {
		return
	}
	reply.LastInsertId, reply.LastInsertIdErr = res.LastInsertId()
	reply.RowsAffected, reply.RowsAffectedErr = res.RowsAffected()
	return
}

func (me *Server) RowsNext(args RowsNextArgs, reply *RowsNextReply) (err error) {
	rows := me.ref(args.RowsRef).(*sql.Rows)
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

func (me *Server) CloseStmt(stmtRef int, reply *struct{}) (err error) {
	err = me.popRef(stmtRef).(*sql.Stmt).Close()
	return
}
