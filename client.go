package sqlrpc

import (
	"database/sql/driver"
	"log"
	"net/rpc"
)

const logCalls = false

type Client struct {
	rpcCl   *rpc.Client
	address string
}

func (me *Client) Close() error {
	return me.rpcCl.Close()
}

func (me *Client) Call(method string, args, reply interface{}) (err error) {
	if logCalls {
		log.Print(method)
	}
	err = me.rpcCl.Call(method, args, reply)
	if err == rpc.ErrShutdown {
		err = driver.ErrBadConn
	}
	if err != nil {
		log.Print(err)
	}
	return
}
