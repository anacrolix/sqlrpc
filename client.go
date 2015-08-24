package sqlrpc

import (
	"database/sql/driver"
	"net/rpc"
)

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
