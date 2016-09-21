package main

import (
	"flag"

	"github.intel.com/hpdd/logging/debug"
)

var enableDebug bool

func init() {
	flag.BoolVar(&enableDebug, "debug", false, "enable debug logging")
}

func foo() {
	debug.Assertf(1 == 2, "it turns out that %s isn't true", "1 == 2")
}

func main() {
	flag.Parse()
	if enableDebug {
		debug.Enable()
	}
	foo()
}
