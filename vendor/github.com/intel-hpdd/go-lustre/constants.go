// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package lustre

const (
	// AcceptorPort is the port used for LNet traffic over TCP
	AcceptorPort = 988

	// MaxExtentLength is a value sent by the coordinator to
	// signify that an action should apply from the offset to
	// EOF. In liblustreapi, this is represented by
	// math.MaxUint64.
	MaxExtentLength int64 = -1
)
