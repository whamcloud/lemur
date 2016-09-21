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
	a := 123
	d := 123.123
	debug.Print("call from foo() ", a, d)
	debug.Printf("call from foo() %v %v", a, d)

}

func main() {
	flag.Parse()
	if enableDebug {
		debug.Enable()
	}
	debug.Printf("inside %s", "main")
	foo()
}
