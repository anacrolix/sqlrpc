package sqlrpc

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/bradfitz/iter"
)

const logRefs = false

type ref struct {
	sqlObj interface{}
	timer  *time.Timer
}

type Service struct {
	DB     *sql.DB
	Expiry time.Duration

	mu      sync.Mutex
	refs    map[int]*ref
	nextRef int
}

func (me *Service) expiry() time.Duration {
	if me.Expiry == 0 {
		return math.MaxInt64
	}
	return me.Expiry
}

func (me *Service) Refs() (ret map[int]interface{}) {
	me.mu.Lock()
	defer me.mu.Unlock()
	ret = make(map[int]interface{}, len(me.refs))
	for k, v := range me.refs {
		ret[k] = v
	}
	return
}

func releaseSqlObj(obj interface{}) error {
	switch v := obj.(type) {
	case *sql.Stmt:
		return v.Close()
	case *sql.Rows:
		return v.Close()
	case *sql.Tx:
		return v.Rollback()
	default:
		return fmt.Errorf("unexpected type: %T", obj)
	}
}

func (me *Service) newRef(obj interface{}) (ret int) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.refs == nil {
		me.refs = make(map[int]*ref)
	}
	for {
		if _, ok := me.refs[me.nextRef]; !ok {
			break
		}
		me.nextRef++
	}
	me.refs[me.nextRef] = &ref{
		sqlObj: obj,
		timer:  time.AfterFunc(me.expiry(), me.expireRef(me.nextRef, obj)),
	}
	ret = me.nextRef
	me.nextRef++
	if logRefs {
		log.Print(me.refs)
	}
	return
}

func (me *Service) expireRef(id int, obj interface{}) func() {
	return func() {
		log.Printf("expiring %d: %T", id, obj)
		so, err := me.popRef(id)
		if err == errBadRef {
			return
		}
		if err != nil {
			log.Print(err)
			return
		}
		releaseSqlObj(so)
	}
}

var errBadRef = errors.New("bad ref")

func (me *Service) popRef(id int) (ret interface{}, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	ref, ok := me.refs[id]
	if !ok {
		err = errBadRef
		return
	}
	ref.timer.Stop()
	ret = ref.sqlObj
	delete(me.refs, id)
	if logRefs {
		log.Print(me.refs)
	}
	return
}

func (me *Service) ref(id int) (ret interface{}, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	ref, ok := me.refs[id]
	if !ok {
		err = errBadRef
		return
	}
	ref.timer.Reset(me.expiry())
	ret = ref.sqlObj
	return
}

func (me *Service) Begin(args struct{}, txId *int) (err error) {
	tx, err := me.DB.Begin()
	if err != nil {
		return
	}
	*txId = me.newRef(tx)
	return
}

func (me *Service) Commit(txId int, reply *struct{}) (err error) {
	_tx, err := me.popRef(txId)
	if err != nil {
		return
	}
	tx := _tx.(*sql.Tx)
	return tx.Commit()
}

func (me *Service) Rollback(txId int, reply *struct{}) (err error) {
	_tx, err := me.popRef(txId)
	if err != nil {
		return
	}
	tx := _tx.(*sql.Tx)
	return tx.Rollback()
}

func (me *Service) Prepare(args PrepareArgs, stmtRef *int) (err error) {
	var ppr interface {
		Prepare(string) (*sql.Stmt, error)
	}
	if args.InTx {
		var sqlObj interface{}
		sqlObj, err = me.ref(args.TxId)
		if err != nil {
			return
		}
		ppr = sqlObj.(*sql.Tx)
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

func (me *Service) Query(args ExecArgs, reply *RowsReply) (err error) {
	_stmt, err := me.ref(args.StmtRef)
	if err != nil {
		return
	}
	stmt := _stmt.(*sql.Stmt)
	rows, err := stmt.Query(args.Values...)
	if err != nil {
		return
	}
	reply.Columns, err = rows.Columns()
	reply.RowsId = me.newRef(rows)
	return
}

func (me *Service) RowsClose(rowsId int, reply *interface{}) (err error) {
	_rows, err := me.popRef(rowsId)
	if err != nil {
		return
	}
	rows := _rows.(*sql.Rows)
	err = rows.Close()
	return
}

func (me *Service) ExecStmt(args ExecArgs, reply *ResultReply) (err error) {
	_stmt, err := me.ref(args.StmtRef)
	if err != nil {
		return
	}
	stmt := _stmt.(*sql.Stmt)
	res, err := stmt.Exec(args.Values...)
	if err != nil {
		return
	}
	reply.LastInsertId, reply.LastInsertIdErr = res.LastInsertId()
	reply.RowsAffected, reply.RowsAffectedErr = res.RowsAffected()
	return
}

func (me *Service) RowsNext(args RowsNextArgs, reply *RowsNextReply) (err error) {
	_rows, err := me.ref(args.RowsRef)
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

func (me *Service) releaseRef(id int) (err error) {
	sqlObj, err := me.popRef(id)
	if err == errBadRef {
		err = nil
		return
	}
	if err != nil {
		return
	}
	err = releaseSqlObj(sqlObj)
	return
}

func (me *Service) CloseStmt(stmtRef int, reply *struct{}) (err error) {
	err = me.releaseRef(stmtRef)
	return
}
