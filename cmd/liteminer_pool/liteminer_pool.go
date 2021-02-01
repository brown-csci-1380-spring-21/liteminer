/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: a CLI for LiteMiner pools.
 */

package main

import (
	"flag"
	liteminer "liteminer/pkg"

	"github.com/abiosoft/ishell"
	"strings"
)

func main() {
	var port string
	var debug bool

	flag.StringVar(&port, "port", "0", "The port to bind to – defaults to a random port.")
	flag.StringVar(&port, "p", "0", "The port to bind to – defaults to a random port.")

	flag.BoolVar(&debug, "debug", false, "Turn debug message printing on or off – defaults to off.")
	flag.BoolVar(&debug, "d", false, "Turn debug message printing on or off – defaults to off.")

	flag.Parse()

	liteminer.SetDebug(debug)

	// Kick off shell
	shell := ishell.New()

	// Start the pool
	p, err := liteminer.CreatePool(port)
	if err != nil {
		shell.Printf("Error starting pool: %v\n", err)
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
		Name: "miners",
		Help: "Prints the miner(s) connected to the pool",
		Func: func(c *ishell.Context) {
			miners := p.GetMiners()
			if len(miners) == 0 {
				shell.Println("No miners connected.")
				return
			}
			for _, miner := range miners {
				shell.Println(miner)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "client",
		Help: "Prints the pool's current client",
		Func: func(c *ishell.Context) {
			client := p.GetClient()
			if client == nil {
				shell.Println("No client connected.")
				return
			}
			shell.Println(client)
		},
	})

	shell.Println(shell.HelpText())
	shell.Run()
}
