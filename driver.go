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
	txId   int
	inTx   bool
}

func (me rpcsqlDriver) Open(name string) (ret driver.Conn, err error) {
	cl, err := rpc.DialHTTP("tcp", name)
	if err != nil {
		return
	}
	conn := &conn{
		client: &Client{cl, name},
	}
	ret = conn
	return
}

func (me *conn) Begin() (ret driver.Tx, err error) {
	var txId int
	err = me.client.Call("Begin", struct{}{}, &txId)
	if err != nil {
		return
	}
	me.txId = txId
	me.inTx = true
	ret = &tx{txId, me}
	return
}

type tx struct {
	id   int
	conn *conn
}

func (me *tx) Commit() (err error) {
	if !me.conn.inTx {
		panic("not in tx")
	}
	err = me.conn.client.Call("Commit", me.id, nil)
	me.conn.inTx = false
	return
}

func (me *tx) Rollback() (err error) {
	if !me.conn.inTx {
		panic("not in tx")
	}
	err = me.conn.client.Call("Rollback", me.id, nil)
	me.conn.inTx = false
	return
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
	return me.conn.client.Call("CloseStmt", me.ref, nil)
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
	return me.cl.Call("RowsClose", me.rr.RowsId, &replyErr)
}

func (me *rows) Next(dest []driver.Value) (err error) {
	var reply RowsNextReply
	err = me.cl.Call("RowsNext", RowsNextArgs{
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
	err = me.conn.client.Call(
		"Query",
		ExecArgs{me.ref, func() (ret []interface{}) {
			for _, v := range args {
				ret = append(ret, v)
			}
			return
		}()},
		&reply)
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
	err = me.conn.client.Call("ExecStmt", ExecArgs{me.ref, func() (ret []interface{}) {
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
	err = me.client.Call(
		"Prepare",
		PrepareArgs{query, me.txId, me.inTx},
		&ref)
	if err != nil {
		return
	}
	ret = &stmt{me, ref}
	return
}
