package mntent

import "bytes"

// TestEntries returns a new Entries slice based on the supplied string
func TestEntries(raw string) (Entries, error) {
	return getEntries(bytes.NewBufferString(raw))
}
