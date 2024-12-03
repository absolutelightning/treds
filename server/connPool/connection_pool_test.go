package connPool

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func startTestServer(t *testing.T) (addr string, closeServer func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0") // Random available port
	require.NoError(t, err)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	return listener.Addr().String(), func() { _ = listener.Close() }
}

func TestNewPooledConn(t *testing.T) {
	addr, closeServer := startTestServer(t)
	defer closeServer()

	pool := NewConnPool(5 * time.Second)
	conn, err := newPooledConn("tcp", addr, pool)
	require.NoError(t, err)
	require.NotNil(t, conn)
	_ = conn.Close()
}

func TestPooledConn_Close(t *testing.T) {
	addr, closeServer := startTestServer(t)
	defer closeServer()

	pool := NewConnPool(5 * time.Second)
	pooledConn, err := newPooledConn("tcp", addr, pool)
	require.NoError(t, err)

	pool.addToPool(pooledConn)

	require.Len(t, pool.pool, 1)
	err = pooledConn.Close()
	require.NoError(t, err)
	require.Len(t, pool.pool, 1) // Connection recycled
}

func TestConnPool_AddAndRemoveFromPool(t *testing.T) {
	pool := NewConnPool(5 * time.Second)

	addr, closeServer := startTestServer(t)
	defer closeServer()

	pooledConn, err := newPooledConn("tcp", addr, pool)
	require.NoError(t, err)

	// Add to pool
	pool.addToPool(pooledConn)
	require.Len(t, pool.pool, 1)

	// Remove from pool
	pool.removeFromPool(pooledConn)
	require.Len(t, pool.pool, 0)
}

func TestConnPool_Dial(t *testing.T) {
	addr, closeServer := startTestServer(t)
	defer closeServer()

	pool := NewConnPool(5 * time.Second)
	conn, err := pool.Dial("tcp", addr)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.Len(t, pool.pool, 0)
	err = conn.Close()
	require.NoError(t, err)
	require.Len(t, pool.pool, 1)
}

func TestConnPool_Dial_Error(t *testing.T) {
	pool := NewConnPool(5 * time.Second)

	// Dial invalid address
	conn, err := pool.Dial("tcp", "invalid:1234")
	require.Error(t, err)
	require.Nil(t, conn)
	require.Len(t, pool.pool, 0)
}

func TestConnPool_Close(t *testing.T) {
	addr, closeServer := startTestServer(t)
	defer closeServer()

	pool := NewConnPool(5 * time.Second)

	conn1, err := pool.Dial("tcp", addr)
	require.NoError(t, err)
	require.NotNil(t, conn1)

	conn2, err := pool.Dial("tcp", addr)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	require.Len(t, pool.pool, 0)

	err = conn1.Close()
	require.NoError(t, err)
	require.Len(t, pool.pool, 1)

	err = conn2.Close()
	require.NoError(t, err)
	require.Len(t, pool.pool, 2)
	// Close pool
	err = pool.Close()
	require.NoError(t, err)
	require.Empty(t, pool.pool)
}

func TestConnPool_CloseTimedOut(t *testing.T) {
	addr, closeServer := startTestServer(t)
	defer closeServer()

	pool := NewConnPool(5 * time.Millisecond)

	conn1, err := pool.Dial("tcp", addr)
	require.NoError(t, err)
	require.NotNil(t, conn1)

	conn2, err := pool.Dial("tcp", addr)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	require.Len(t, pool.pool, 0)

	err = conn1.Close()
	require.NoError(t, err)
	require.Len(t, pool.pool, 1)

	// wait for conn1 to timeout in the pool
	time.Sleep(2 * time.Second)
	err = conn2.Close()
	require.NoError(t, err)
	require.Len(t, pool.pool, 1)

}
