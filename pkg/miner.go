/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: a LiteMiner miner.
 */

package pkg

import (
	"fmt"
	"go.uber.org/atomic"
	"io"
	"net"
	"time"
)

// HeartbeatFreq is the frequency at which a miner sends heartbeats to the pool
const HeartbeatFreq = 1000 * time.Millisecond

// Miner represents a LiteMiner miner. We use atomic types here to
// make it clear that these types are being concurrently accessed
// and to eliminate use of mutexes so that implementation is more clear
type Miner struct {
	IsShutdown   *atomic.Bool   // Represents if the miner has shut down and should stop replying/sending requests
	Mining       *atomic.Bool   // Represents if the miner is currently mining
	NumProcessed *atomic.Uint64 // Number of values miner has currently processed for the CURRENT mining range
	speed        time.Duration  // Number of nanoseconds it delays every time it needs to do anything

}

// CreateMiner creates a new miner connected to the pool at the specified address.
func CreateMiner(addr string) (*Miner, error) {
	m := &Miner{
		Mining:       atomic.NewBool(false),
		NumProcessed: atomic.NewUint64(0),
		IsShutdown:   atomic.NewBool(false),
	}

	// Connect miner to the pool address. Returns a two-way TCP connection
	conn, err := MinerConnect(addr)
	if err != nil {
		return nil, fmt.Errorf("received error %v when connecting to pool %v", err, addr)
	}

	// Start miner goroutines (sending & receiving msgs)
	go m.receiveFromPool(conn)
	go m.sendHeartBeats(conn)

	return m, nil
}

// receiveFromPool processes messages sent from the pool represented by conn.
func (m *Miner) receiveFromPool(conn MiningConn) {
	for {
		if m.IsShutdown.Load() {
			conn.Conn.Close() // Close the connection
			return
		}

		msg, err := RecvMsg(conn)
		if err != nil {
			if _, ok := err.(*net.OpError); ok || err == io.EOF {
				Err.Printf("Lost connection to pool %v\n", conn.Conn.RemoteAddr())
				conn.Conn.Close() // Close the connection
				return
			}

			Err.Printf(
				"Received error %v when processing pool %v\n",
				err,
				conn.Conn.RemoteAddr(),
			)
			continue
		}

		if msg.Type != MineRequest {
			Err.Printf(
				"Received unexpected message of type %v from pool %v\n",
				msg.Type,
				conn.Conn.RemoteAddr(),
			)
		}

		// Service the mine request
		nonce := m.Mine(msg.Data, msg.Lower, msg.Upper)

		// Send result
		res := ProofOfWorkMsg(msg.Data, nonce, Hash(msg.Data, nonce))
		SendMsg(conn, res)
	}
}

// sendHeartBeats periodically sends heartbeats in the form of StatusUpdateMsgs
// to the pool represented by conn while mining. sendHeartBeats should NOT send
// heartbeats to the pool if the miner is not mining. It should close the connection
// and return if the miner is shutdown. You may want to use time.NewTicker to
// to trigger heartbeats at a specified frequency.
func (m *Miner) sendHeartBeats(conn MiningConn) {
	// TODO: Students should send a StatusUpdate message every HEARTBEAT_FREQ
	// while mining.
	return
}

// Mine is given a data string, a lower bound (inclusive), and an upper bound
// (exclusive), and returns the nonce in the range [lower, upper) that
// corresponds to the lowest hash value. With each value processed in the range,
// NumProcessed should be incremented.
func (m *Miner) Mine(data string, lower, upper uint64) (nonce uint64) {
	// TODO: Students should implement this. Make sure to use the Hash method
	// in hash.go
	return
}

// Shutdown marks the miner as shutdown and asynchronously disconnects it from
// its pool.
func (m *Miner) Shutdown() {
	Debug.Printf("Shutting down")
	m.IsShutdown.Store(true)
}
