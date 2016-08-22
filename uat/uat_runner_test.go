package uat

import (
	"os"
	"testing"

	"github.com/DATA-DOG/godog"
)

var runDir = "."
var raceBinPath = "."

func TestMain(m *testing.M) {
	// Run the features tests from the compiled-in location.
	if err := os.Chdir(runDir); err != nil {
		panic(err)
	}

	// Prefix the path so that we can find our race-compiled binaries.
	os.Setenv("PATH", raceBinPath+":"+os.Getenv("PATH"))

	status := godog.Run(func(suite *godog.Suite) {
		ConfigureSuite(suite)
	})
	os.Exit(status)
}
