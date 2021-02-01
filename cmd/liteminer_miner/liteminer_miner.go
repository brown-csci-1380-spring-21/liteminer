/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: a CLI for LiteMiner miners.
 */

package main

import (
	"flag"
	liteminer "liteminer/pkg"

	"github.com/abiosoft/ishell"
	"strings"
)

func main() {
	var addr string
	var debug bool

	flag.StringVar(&addr, "connect", "", "Address of the mining pool to connect to.")
	flag.StringVar(&addr, "c", "", "Address of the mining pool to connect to.")

	flag.BoolVar(&debug, "debug", false, "Turn debug message printing on or off – defaults to off.")
	flag.BoolVar(&debug, "d", false, "Turn debug message printing on or off – defaults to off.")

	flag.Parse()

	liteminer.SetDebug(debug)

	// Kick off shell
	shell := ishell.New()

	// Connect to mining pool
	m, err := liteminer.CreateMiner(addr)
	if err != nil {
		shell.Printf("Error starting miner: %v\n", err)
		return
	}

	shell.AddCmd(&ishell.Cmd{
		Name: "debug",
		Help: "Turn debug statements on or off",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: debug <on|off>")
				return
			}

			debugState := strings.ToLower(c.Args[0])

			switch debugState {
			case "on":
				liteminer.SetDebug(true)
				shell.Println("Debug turned on")
			case "off":
				liteminer.SetDebug(false)
				shell.Println("Debug turned off")
			default:
				shell.Println("Usage: debug <on|off>")
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "shutdown",
		Help: "Shuts down the miner",
		Func: func(c *ishell.Context) {
			m.Shutdown()
		},
	})

	shell.Println(shell.HelpText())
	shell.Run()
}
