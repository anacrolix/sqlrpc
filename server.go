package sqlrpc

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
)

const logRefs = false

type ref struct {
	sqlObj interface{}
}

type RefId int

type Server struct {
	DB *sql.DB

	mu      sync.Mutex
	refs    map[RefId]*ref
	nextRef RefId

	// RPC
	Service
}

func (me *Server) db() *sql.DB {
	return me.DB
}

func (me *Server) Refs() (ret map[RefId]interface{}) {
	me.mu.Lock()
	defer me.mu.Unlock()
	ret = make(map[RefId]interface{}, len(me.refs))
	for k, v := range me.refs {
		ret[k] = v.sqlObj
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

func (me *Server) newRef(obj interface{}, expire bool) (ret RefId) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.refs == nil {
		me.refs = make(map[RefId]*ref)
	}
	for {
		if _, ok := me.refs[me.nextRef]; !ok {
			break
		}
		me.nextRef++
	}
	me.refs[me.nextRef] = &ref{
		sqlObj: obj,
	}
	ret = me.nextRef
	me.nextRef++
	if logRefs {
		log.Print(me.refs)
	}
	return
}

func (me *Server) expireRef(id RefId, obj interface{}) func() {
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

func (me *Server) popRef(id RefId) (ret interface{}, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	ref, ok := me.refs[id]
	if !ok {
		err = errBadRef
		return
	}
	ret = ref.sqlObj
	delete(me.refs, id)
	if logRefs {
		log.Print(me.refs)
	}
	return
}

func (me *Server) ref(id RefId) (ret interface{}, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	ref, ok := me.refs[id]
	if !ok {
		err = errBadRef
		return
	}
	ret = ref.sqlObj
	return
}

func (me *Server) releaseRef(id RefId) (err error) {
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
