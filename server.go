package sqlrpc

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

const logRefs = false

type ref struct {
	sqlObj interface{}
	timer  *time.Timer
}

type RefId int

type Server struct {
	DB     *sql.DB
	Expiry time.Duration

	mu      sync.Mutex
	refs    map[RefId]*ref
	nextRef RefId

	Service
}

func (me *Server) expiry() time.Duration {
	if me.Expiry == 0 {
		return math.MaxInt64
	}
	return me.Expiry
}

// net/rpc complains about this methods signature, but it needs to be public
// to export this information to a status page.
func (me *Server) Refs() (ret map[RefId]interface{}) {
	me.mu.Lock()
	defer me.mu.Unlock()
	ret = make(map[RefId]interface{}, len(me.refs))
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

func (me *Server) newRef(obj interface{}) (ret RefId) {
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
		timer:  time.AfterFunc(me.expiry(), me.expireRef(me.nextRef, obj)),
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
	ref.timer.Stop()
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
	ref.timer.Reset(me.expiry())
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
