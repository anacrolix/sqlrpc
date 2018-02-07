package refs

import (
	"sync"
)

type Id int

type ref struct {
	value  interface{}
	closer func() error
}

type Manager struct {
	mu      sync.Mutex
	refs    map[Id]*ref
	nextRef Id
}

func (me *Manager) Len() int {
	me.mu.Lock()
	defer me.mu.Unlock()
	return len(me.refs)
}

func (me *Manager) GetAll() (ret map[Id]interface{}) {
	me.mu.Lock()
	defer me.mu.Unlock()
	ret = make(map[Id]interface{}, len(me.refs))
	for k, v := range me.refs {
		ret[k] = v.value
	}
	return
}

func (me *Manager) New(obj interface{}, closer func() error) Id {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.refs == nil {
		me.refs = make(map[Id]*ref)
	}
	for {
		if _, ok := me.refs[me.nextRef]; !ok {
			break
		}
		me.nextRef++
	}
	ret := me.nextRef
	me.nextRef++
	me.refs[ret] = &ref{
		value:  obj,
		closer: closer,
	}
	return ret
}

func (me *Manager) Get(id Id) interface{} {
	me.mu.Lock()
	defer me.mu.Unlock()
	return me.refs[id].value
}

func (me *Manager) Pop(id Id) interface{} {
	me.mu.Lock()
	defer me.mu.Unlock()
	ret := me.refs[id].value
	delete(me.refs, id)
	return ret
}

func (me *Manager) Release(id Id) error {
	me.mu.Lock()
	defer me.mu.Unlock()
	ref := me.refs[id]
	delete(me.refs, id)
	return ref.closer()
}
