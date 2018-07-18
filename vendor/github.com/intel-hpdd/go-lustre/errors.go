// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package lustre

import "errors"

// ErrUnimplemented is returned when a function is not available on
// the current platform.
var ErrUnimplemented = errors.New("not implemented")
