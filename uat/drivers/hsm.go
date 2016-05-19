package drivers

// HsmDriver defines an interface to be implemented by a HSM tool driver
// e.g. lfs hsm_*, ldmc, etc.
type HsmDriver interface {
	// Archive store's the file's data in the archive backend
	Archive(string) error
	// Restore explicitly restores the file
	Restore(string) error
	// Remove removes the restored file from the archive backend
	Remove(string) error
	// Release releases the archived file's space on the filesystem
	Release(string) error
	// GetState returns the HsmState for the file
	GetState(string) (HsmState, error)
}

// HsmState indicates the file's status
type HsmState string

const (
	// Unknown indicates that the file state is unknown
	Unknown HsmState = "unknown"

	// Unarchived indicates that the file exists but is unarchived
	Unarchived HsmState = "unarchived"

	// Archived indicates that the file is archived
	Archived HsmState = "archived"

	// Released indicates that the file is archived and released
	Released HsmState = "released"
)

func (h HsmState) String() string {
	return string(h)
}
