package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"
)

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			glog.Infoln("signal received:", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()

}
