// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"os"
	"strings"

	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/debug"

	"gopkg.in/urfave/cli.v1"
)

var commands []cli.Command
var version string // Set by build environment

func main() {
	app := cli.NewApp()
	app.Usage = "Data Movement Control for Lustre* software"
	app.Commands = commands
	app.Version = version
	app.Authors = []cli.Author{
		{
			Name:  "Intel® Enterprise Edition for Lustre* software Team",
			Email: "HPDD-enterprise-lustre@intel.com",
		},
	}
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "debug",
			Usage: "Display debug logging to console",
		},
		cli.StringFlag{
			Name:  "logfile, l",
			Usage: "Log tool activity to this file",
			Value: "",
		},
	}
	app.Before = configureLogging
	if err := app.Run(os.Args); err != nil {
		alert.Abort(err)
	}
}

func configureLogging(c *cli.Context) error {
	if c.Bool("debug") {
		debug.Enable()
	}

	return nil
}

func logContext(c *cli.Context) {
	for {
		if c.Parent() == nil {
			break
		}
		c = c.Parent()
	}

	debug.Printf("Context: %s", strings.Join(c.Args(), " "))
}
