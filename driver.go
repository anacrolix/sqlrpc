package sqlrpc

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"net/rpc"
)

func init() {
	sql.Register("sqlrpc", &rpcsqlDriver{})
}

type rpcsqlDriver struct{}

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

type rows struct {
	cl *Client
	rr *RowsReply
}

func (me *rows) Close() error {
	var replyErr error
	return me.cl.Call("Server.RowsClose", me.rr.RowsId, &replyErr)
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
