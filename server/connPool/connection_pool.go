package connPool

import (
	"github.com/pkg/errors"
	"net"
	"sync"
	"time"
)

type PooledConn struct {
	net.Conn
	pool      *ConnPool
	timestamp time.Time
}

func newPooledConn(network string, addr string, pool *ConnPool) (*PooledConn, error) {
	conn := &PooledConn{pool: pool}
	var err error
	conn.Conn, err = net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (p *PooledConn) id() string {
	return p.Conn.RemoteAddr().String() + p.LocalAddr().String()
}

func (p *PooledConn) Close() error {
	p.timestamp = time.Now()
	p.pool.addToPool(p)
	p.pool.closeTimedOut()
	return nil
}

type ConnPool struct {
	pool     map[string]*PooledConn
	poolLock sync.Mutex
	timeout  time.Duration
}

func NewConnPool(timeout time.Duration) *ConnPool {
	pool := make(map[string]*PooledConn)
	return &ConnPool{pool: pool, timeout: timeout}
}

func (cp *ConnPool) Dial(network, addr string) (net.Conn, error) {
	conn, err := newPooledConn(network, addr, cp)
	if err != nil {
		return nil, err
	}
	cp.removeFromPool(conn)
	return conn, nil
}

func (cp *ConnPool) Close() error {
	var err error
	for _, conn := range cp.pool {

		err2 := conn.Conn.Close()
		if err2 != nil {
			err = errors.Wrap(err, err2.Error())
		}
	}
	cp.poolLock.Lock()
	defer cp.poolLock.Unlock()
	cp.pool = make(map[string]*PooledConn)

	return err
}

func (cp *ConnPool) addToPool(p *PooledConn) {
	cp.poolLock.Lock()
	defer cp.poolLock.Unlock()
	cp.pool[p.id()] = p
}

func (cp *ConnPool) removeFromPool(conn *PooledConn) {
	cp.poolLock.Lock()
	defer cp.poolLock.Unlock()
	delete(cp.pool, conn.id())
}

func (cp *ConnPool) closeTimedOut() {
	for _, conn := range cp.pool {
		if time.Since(conn.timestamp) > cp.timeout {
			_ = conn.Conn.Close()
			cp.removeFromPool(conn)
		}
	}
}
