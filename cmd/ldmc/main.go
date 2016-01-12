package main

import (
	"os"
	"strings"

	"github.com/codegangsta/cli"

	"github.intel.com/hpdd/applog"
	"github.intel.com/hpdd/liblog"
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
			Usage: "Enable debug logging",
		},
		cli.StringFlag{
			Name:  "logfile",
			Usage: "Logfile for debug logging",
			Value: "stderr",
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
		liblog.Enable()
	}
	applog.SetJournal(c.String("logfile"))
	liblog.SetWriter(applog.Writer("liblog"))

	return nil
}

func logContext(c *cli.Context) {
	for {
		if c.Parent() == nil {
			break
		}
		c = c.Parent()
	}

	applog.Debug("Context: %s", strings.Join(c.Args(), " "))
}
