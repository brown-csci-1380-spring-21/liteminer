/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: sets up several loggers.
 */

package pkg

import (
	"io/ioutil"
	"log"
	"os"
)

// Debug is optional logger for debugging
var Debug *log.Logger

// Out is logger to Stdout
var Out *log.Logger

// Err is logger to Stderr
var Err *log.Logger

// init initializes the loggers.
func init() {
	Debug = log.New(ioutil.Discard, "DEBUG: ", log.Ltime|log.Lshortfile)
	Out = log.New(os.Stdout, "INFO: ", log.Ltime|log.Lshortfile)
	Err = log.New(os.Stderr, "ERROR: ", log.Ltime|log.Lshortfile)
}

// SetDebug turns debug print statements on or off.
func SetDebug(enabled bool) {
	if enabled {
		Debug.SetOutput(os.Stdout)
	} else {
		Debug.SetOutput(ioutil.Discard)
	}
}
