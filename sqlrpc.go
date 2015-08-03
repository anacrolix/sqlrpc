/*
Package sqlrpc provides an RPC server that exposes a `database/sql.*DB`, and a SQL driver that can connect to it. Any `database/sql.*DB` can be exposed, but the primary motivation is to expose the excellent SQLite3. `cmd/sqlite3server` is provided for this purpose, and can easily be adapted to other DBs.
*/
package sqlrpc

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"net/rpc"
	"sync"

	"github.com/bradfitz/iter"
)

func init() {
	sql.Register("sqlrpc", &rpcsqlDriver{})
}

type rpcsqlDriver struct{}

type Server struct {
	DB *sql.DB

	mu      sync.Mutex
	refs    map[int]interface{}
	nextRef int
}

type Client struct {
	rpcCl   *rpc.Client
	address string
}

func (me *Client) Close() error {
	return me.rpcCl.Close()
}

func (me *Client) Call(method string, args, reply interface{}) (err error) {
	err = me.rpcCl.Call(method, args, reply)
	if err == rpc.ErrShutdown {
		err = driver.ErrBadConn
	}
	return
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
	me.mu.Unlock()
	return
}

func (me *Server) popRef(id int) (ret interface{}) {
	me.mu.Lock()
	ret = me.refs[id]
	delete(me.refs, id)
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

func (me *Server) Prepare(query string, stmtRef *int) (err error) {
	stmt, err := me.DB.Prepare(query)
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

type ExecArgs struct {
	StmtRef int
	Values  []interface{}
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

type conn struct {
	client *Client
}

func (me rpcsqlDriver) Open(name string) (ret driver.Conn, err error) {
	cl, err := rpc.DialHTTP("tcp", name)
	if err != nil {
		return
	}
	conn := &conn{&Client{cl, name}}
	ret = conn
	return
}

func (me *conn) Begin() (ret driver.Tx, err error) {
	var txId int
	err = me.client.Call("Server.Begin", struct{}{}, &txId)
	if err == rpc.ErrShutdown {
		err = driver.ErrBadConn
	}
	if err != nil {
		return
	}
	ret = &tx{txId, me}
	return
}

type tx struct {
	id   int
	conn *conn
}

type CommitArgs struct {
	TxID interface{}
}

func (me *tx) Commit() error {
	return me.conn.client.Call("Server.Commit", me.id, nil)
}

func (me *tx) Rollback() error {
	return me.conn.client.Call("Server.Rollback", me.id, nil)
}

func (me *conn) Close() (err error) {
	err = me.client.Close()
	me.client = nil
	return
}

type stmt struct {
	conn *conn
	ref  int
}

func (me *stmt) Close() error {
	return me.conn.client.Call("Server.CloseStmt", me.ref, nil)
}

func (me *stmt) NumInput() int {
	return -1
}

type RowsReply struct {
	Columns []string
	RowsId  int
}

type rows struct {
	cl *Client
	rr *RowsReply
}

func (me *rows) Close() error {
	var replyErr error
	return me.cl.Call("Server.RowsClose", me.rr.RowsId, &replyErr)
}

type RowsNextArgs struct {
	RowsRef   int
	NumValues int
}

type RowsNextReply struct {
	Values []interface{}
	EOF    bool
}

func (me *rows) Next(dest []driver.Value) (err error) {
	var reply RowsNextReply
	err = me.cl.Call("Server.RowsNext", RowsNextArgs{
		me.rr.RowsId,
		len(me.Columns()),
	}, &reply)
	if err != nil {
		return
	}
	if reply.EOF {
		return io.EOF
	}
	for i, v := range reply.Values {
		dest[i] = v
	}
	return
}

func (me *rows) Columns() []string {
	return me.rr.Columns
}

func (me *stmt) Query(args []driver.Value) (ret driver.Rows, err error) {
	var reply RowsReply
	err = me.conn.client.Call("Server.Query", ExecArgs{me.ref, func() (ret []interface{}) {
		for _, v := range args {
			ret = append(ret, v)
		}
		return
	}()}, &reply)
	if err != nil {
		return
	}
	ret = &rows{me.conn.client, &reply}
	return
}

type ResultReply struct {
	LastInsertId    int64
	LastInsertIdErr error
	RowsAffected    int64
	RowsAffectedErr error
}

type result struct {
	rr ResultReply
}

func (me *result) LastInsertId() (int64, error) {
	return me.rr.LastInsertId, me.rr.LastInsertIdErr
}

func (me *result) RowsAffected() (int64, error) {
	return me.rr.RowsAffected, me.rr.RowsAffectedErr
}

func (me *stmt) Exec(args []driver.Value) (ret driver.Result, err error) {
	var rr ResultReply
	err = me.conn.client.Call("Server.ExecStmt", ExecArgs{me.ref, func() (ret []interface{}) {
		for _, v := range args {
			ret = append(ret, v)
		}
		return
	}()}, &rr)
	if err != nil {
		return
	}
	ret = &result{rr}
	return
}

func (me *conn) Prepare(query string) (ret driver.Stmt, err error) {
	var ref int
	err = me.client.Call("Server.Prepare", query, &ref)
	if err == rpc.ErrShutdown {
		err = driver.ErrBadConn
	}
	if err != nil {
		return
	}
	ret = &stmt{me, ref}
	return
}
