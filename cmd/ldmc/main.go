package main

import (
	"os"

	"github.com/codegangsta/cli"

	"github.intel.com/hpdd/ce-tools/pkg/applog"
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
		liblog.Enable()
	}
	liblog.SetWriter(c.String("logfile"))

	return nil
}
