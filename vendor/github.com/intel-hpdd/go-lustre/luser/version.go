// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package luser

import (
	"bufio"
	"os"
	"strings"
)

// Version represents the version of Lustre.
type Version struct {
	Lustre string `yaml:"lustre"`
	Kernel string `yaml:"kernel"`
	Build  string `yaml:"build"`
}

// GetVersion returns current Lustre version.
func GetVersion() (*Version, error) {
	var ver Version

	fp, err := os.Open("/proc/fs/lustre/version")
	if err != nil {
		return nil, err
	}
	b := bufio.NewReader(fp)
	for {
		label, err := b.ReadString(':')
		if err != nil {
			return &ver, nil
		}
		value, err := b.ReadString('\n')
		if err != nil {
			return &ver, nil
		}
		value = strings.TrimSpace(value)
		switch label {
		case "build:":
			ver.Build = value
		case "kernel:":
			ver.Kernel = value
		case "lustre:":
			ver.Lustre = value
		}
	}
}
