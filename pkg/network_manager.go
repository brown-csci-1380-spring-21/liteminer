/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: contains methods for creating connections and sending and
 *  receiving messages.
 */

package pkg

import (
	"encoding/gob"
	"net"
)

// MiningConn is a wrapper for sending/receiving and encoding/decoding
// TCP msgs sent between the client <-> pool and pool <-> miner.
type MiningConn struct {
	Enc  *gob.Encoder
	Dec  *gob.Decoder
	Conn net.Conn
}

// MinerConnect return a miner connection to the mining pool at addr
func MinerConnect(addr string) (MiningConn, error) {
	miningConn := MiningConn{}

	Debug.Printf("Miner connecting to %s\n", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return miningConn, err
	}

	miningConn.Conn = conn
	miningConn.Enc = gob.NewEncoder(conn)
	miningConn.Dec = gob.NewDecoder(conn)

	// Send MinerHello Message
	SendMsg(miningConn, MinerHelloMsg())

	return miningConn, nil
}

// ClientConnect returns a client connection to the mining pool at addr
func ClientConnect(addr string) (MiningConn, error) {
	miningConn := MiningConn{}

	Debug.Printf("Client connecting to %s\n", addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return miningConn, err
	}

	miningConn.Conn = conn
	miningConn.Enc = gob.NewEncoder(conn)
	miningConn.Dec = gob.NewDecoder(conn)

	// Send ClientHello Message
	SendMsg(miningConn, ClientHelloMsg())

	return miningConn, nil
}

// SendMsg sends a message over miningConn
func SendMsg(miningConn MiningConn, message *Message) {
	miningConn.Enc.Encode(message)
}

// RecvMsg receives and returns the next message from miningConn
func RecvMsg(miningConn MiningConn) (Message, error) {
	var msg Message
	err := miningConn.Dec.Decode(&msg)
	return msg, err
}
