package main

import (
	"os"
	"strings"

	"github.com/codegangsta/cli"

	"github.intel.com/hpdd/logging/applog"
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
		applog.Fail(err)
	}
}

func configureLogging(c *cli.Context) error {
	if c.Bool("debug") {
		applog.SetLevel(applog.DEBUG)
	}
	applog.SetJournal(c.String("logfile"))

	return nil
}

func logContext(c *cli.Context) {
	for {
		if c.Parent() == nil {
			break
		}
		c = c.Parent()
	}

	applog.Trace("Context: %s", strings.Join(c.Args(), " "))
}
