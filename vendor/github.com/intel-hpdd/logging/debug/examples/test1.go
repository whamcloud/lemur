// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"

	"github.com/intel-hpdd/logging/debug"
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
