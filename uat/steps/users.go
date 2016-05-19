package steps

import (
	"fmt"
	"os/user"

	"github.com/pkg/errors"
)

func init() {
	addStep(`^I am the (\w+) user$`, Context.iAmTheSpecifiedUser)
}

func (sc *stepContext) iAmTheSpecifiedUser(specified string) error {
	current, err := user.Current()
	if err != nil {
		return errors.Wrap(err, "Could not get current user")
	}

	if current.Username != specified {
		return fmt.Errorf("Current user is %s, not %s", current.Username, specified)
	}

	return nil
}
