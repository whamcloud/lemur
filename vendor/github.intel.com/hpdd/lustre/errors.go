package lustre

import "errors"

// ErrUnimplemented is returned when a function is not available on
// the current platform.
var ErrUnimplemented = errors.New("not implemented")
