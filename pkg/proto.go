/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: defines the LiteMiner protocol.
 */

package pkg

// Enums for MsgType
type MsgType int

const (
	ClientHello MsgType = iota
	MinerHello
	Error
	ProofOfWork
	StatusUpdate
	MineRequest
	Transaction
	BusyPool
)

// Message struct supporting all types of communication amongst LiteMiner miners,
// pools, and clients
type Message struct {
	Type         MsgType
	Data         string
	Lower, Upper uint64
	NumProcessed uint64
	Hash, Nonce  uint64
}

// Creates a ClientHello message – these messages are sent from clients to a
// pool upon connecting to it.
func ClientHelloMsg() *Message {
	return &Message{
		Type: ClientHello,
	}
}

// Creates a MinerHello message – these messages are sent from miners to a
// pool upon connecting to it.
func MinerHelloMsg() *Message {
	return &Message{
		Type: MinerHello,
	}
}

// Creates an Error message
func ErrorMsg(err string) *Message {
	return &Message{
		Type: Error,
		Data: err,
	}
}

// Creates a ProofOfWork message – these messages are sent from a miner to a
// pool after completing a mine request.
func ProofOfWorkMsg(data string, nonce uint64, hash uint64) *Message {
	return &Message{
		Type:  ProofOfWork,
		Data:  data,
		Nonce: nonce,
		Hash:  hash,
	}
}

// Creates a StatusUpdate message – these messages are sent from a miner to a
// pool while mining.
func StatusUpdateMsg(numProcessed uint64) *Message {
	// TODO: Students should implement this.
	return &Message{}
}

// Creates a MineRequest message – these messages are sent from a pool
// to a miner when distributing work.
func MineRequestMsg(data string, lower uint64, upper uint64) *Message {
	// TODO: Students should implement this.
	return &Message{}
}

// Creates a Transaction message – these messages are sent from a client to a
// pool.
func TransactionMsg(data string, upper uint64) *Message {
	// TODO: Students should implement this.
	return &Message{}
}

// Creates a BusyPool message – these messages are sent from a busy pool to a
// client trying to connect.
func BusyPoolMsg() *Message {
	// TODO: Students should implement this.
	return &Message{}
}
