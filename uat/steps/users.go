// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package steps

import (
	"fmt"
	"os/user"

	"github.com/pkg/errors"
)

func init() {
	addStep(`^I am the (\w+) user$`, iAmTheSpecifiedUser)
}

func iAmTheSpecifiedUser(specified string) error {
	current, err := user.Current()
	if err != nil {
		return errors.Wrap(err, "Could not get current user")
	}

	if current.Username != specified {
		return fmt.Errorf("Current user is %s, not %s", current.Username, specified)
	}

	return nil
}
