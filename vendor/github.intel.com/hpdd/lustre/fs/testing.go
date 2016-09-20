package fs

// TestID returns an ID value suitable for testing without an actual
// lustre filesystem.
func TestID(name string) ID {
	return ID(RootDir{path: name})
}
