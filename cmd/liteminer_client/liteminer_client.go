/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: a CLI for LiteMiner clients.
 */

package main

import (
	"flag"
	liteminer "liteminer/pkg"

	"github.com/abiosoft/ishell"
	"strconv"
	"strings"
)

func main() {
	var addrs string
	var debug bool

	flag.StringVar(&addrs, "connect", "", "Addresses of the mining pool(s) to connect to (comma-separated).")
	flag.StringVar(&addrs, "c", "", "Addresses of the mining pool(s) to connect to (comma-separated).")

	flag.BoolVar(&debug, "debug", false, "Turn debug message printing on or off – defaults to off.")
	flag.BoolVar(&debug, "d", false, "Turn debug message printing on or off – defaults to off.")

	flag.Parse()

	liteminer.SetDebug(debug)

	// Kick off shell
	shell := ishell.New()

	// Connect to the mining pools
	client := liteminer.CreateClient(strings.Split(addrs, ","))

	shell.AddCmd(&ishell.Cmd{
		Name: "connect",
		Help: "Connect to the specified pool(s)",
		Func: func(c *ishell.Context) {
			if len(c.Args) < 1 {
				shell.Println("Usage: connect <pool addresses>")
				return
			}

			client.Connect(c.Args[0:])
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "mine",
		Help: "Send a mine request to any connected pool(s)",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 2 {
				shell.Println("Usage: mine <data> <upper bound on nonce>")
				return
			}

			upperBound, err := strconv.ParseUint(c.Args[1], 10, 64)
			if err != nil {
				shell.Println("Usage: mine <data> <upper bound on nonce>")
				return
			}

			nonces, err := client.Mine(c.Args[0], upperBound)
			if err != nil {
				shell.Println(err.Error())
			} else {
				for addr, nonce := range nonces {
					shell.Printf("Pool %v: %v\n", addr, nonce)
				}
			}
		},
	})

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
		Name: "pools",
		Help: "Print the pools that the client is currently connected to",
		Func: func(c *ishell.Context) {
			for addr, _ := range client.PoolConns {
				shell.Println(addr)
			}
		},
	})

	shell.Println(shell.HelpText())
	shell.Run()
}
