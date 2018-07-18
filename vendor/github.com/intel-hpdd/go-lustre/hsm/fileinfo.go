// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package hsm

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/intel-hpdd/go-lustre/llapi"
)

// FileStatus describes a file's current HSM status, including its
// associated archive ID (if any), and HSM state
type FileStatus struct {
	ArchiveID uint32
	state     llapi.HsmFileState
}

// Exists is true if an HSM archive action has been initiated for a file. A
// copy or partial copy of the file may exist in the backend. Or it might not.
func (f *FileStatus) Exists() bool {
	return f.state.HasFlag(llapi.HsmFileExists)
}

// Archived is true if a complete (but possibly stale) copy of the file
// contents are stored in the archive.
func (f *FileStatus) Archived() bool {
	return f.state.HasFlag(llapi.HsmFileArchived)
}

// Dirty means the file has been modified since the last time it was archived.
func (f *FileStatus) Dirty() bool {
	return f.state.HasFlag(llapi.HsmFileDirty)
}

// Released is true if the contents of the file have been removed from the
// filesystem. Only possible if the file has been Archived.
func (f *FileStatus) Released() bool {
	return f.state.HasFlag(llapi.HsmFileReleased)
}

// NoRelease prevents the file data from being relesed, even if it is Archived.
func (f *FileStatus) NoRelease() bool {
	return f.state.HasFlag(llapi.HsmFileNoRelease)
}

// NoArchive inhibits archiving the file. (Useful for temporary files perhaps.)
func (f *FileStatus) NoArchive() bool {
	return f.state.HasFlag(llapi.HsmFileNoArchive)
}

// Lost means the copy of the file in the archive is not accessible.
func (f *FileStatus) Lost() bool {
	return f.state.HasFlag(llapi.HsmFileLost)
}

// Flags returns a slice of HSM state flag strings
func (f *FileStatus) Flags() []string {
	return f.state.Flags()
}

func (f *FileStatus) String() string {
	return FileStatusString(f, true)
}

// GetFileStatus returns a *FileStatus for the given path
func GetFileStatus(filePath string) (*FileStatus, error) {
	s, id, err := llapi.GetHsmFileStatus(filePath)
	if err != nil {
		return nil, err
	}
	return &FileStatus{ArchiveID: id, state: s}, nil
}

func summarizeStatus(s *FileStatus) string {
	var buf bytes.Buffer

	if s.Exists() {
		switch {
		case s.Released():
			fmt.Fprintf(&buf, "released")
			if s.Lost() {
				fmt.Fprintf(&buf, "+lost")
			}
		case s.Lost():
			fmt.Fprintf(&buf, "lost")
		case s.Dirty():
			fmt.Fprintf(&buf, "dirty")
		case s.Archived():
			fmt.Fprintf(&buf, "archived")
		default:
			fmt.Fprintf(&buf, "unarchived")
		}
	} else {
		fmt.Fprintf(&buf, "-")
	}

	if s.NoRelease() {
		fmt.Fprintf(&buf, "@")
	}

	if s.NoArchive() {
		fmt.Fprintf(&buf, "%%")
	}

	return buf.String()
}

// FileStatusString returns a string describing the given FileStatus
func FileStatusString(s *FileStatus, summarize bool) string {
	// NB: On the fence about whether or not this stuff belongs here --
	// it's arguably application-specific display logic, but it seems
	// like it'd be nice to not have to reinvent this wheel all the time.
	var buf bytes.Buffer

	if s.Exists() {
		fmt.Fprintf(&buf, "%d", s.ArchiveID)
	} else {
		fmt.Fprintf(&buf, "-")
	}

	if summarize {
		fmt.Fprintf(&buf, " %s", summarizeStatus(s))
	} else {
		if len(s.Flags()) > 0 {
			fmt.Fprintf(&buf, " %s", strings.Join(s.Flags(), ","))
		} else {
			fmt.Fprintf(&buf, " -")
		}
	}

	return buf.String()
}

// SetFileStatus updates the file's HSM flags and/or archive ID
func SetFileStatus(filePath string, setMask, clearMask uint64, archiveID uint32) error {
	return llapi.SetHsmFileStatus(filePath, setMask, clearMask, archiveID)
}

// GetStatusMask converts a slice of flag strings to a bitmask
func GetStatusMask(flagNames []string) (uint64, error) {
	flagNameToFlags := make(map[string]llapi.HsmStateFlag)
	for flag, name := range llapi.HsmStateFlags {
		flagNameToFlags[name] = flag
	}

	mask := uint64(0)
	for _, name := range flagNames {
		if flag, ok := flagNameToFlags[name]; ok {
			mask |= uint64(flag)
		} else {
			return 0, fmt.Errorf("Unknown HSM status flag name: %s", name)
		}
	}

	return mask, nil
}

// GetStateFlagNames returns a slice of HSM state flag names
func GetStateFlagNames() []string {
	var names []string

	for _, name := range llapi.HsmStateFlags {
		names = append(names, name)
	}

	return names
}

// CurrentFileAction represents the current HSM action being performed
// for a file, if any.
type CurrentFileAction struct {
	action llapi.HsmUserAction
	state  llapi.HsmProgressState

	Path        string
	BytesCopied int64
}

// Action returns a string representation of the current action
func (cfa *CurrentFileAction) Action() string {
	return strings.ToLower(cfa.action.String())
}

// State returns a string representation of the current action's state
func (cfa *CurrentFileAction) State() string {
	return strings.ToLower(cfa.state.String())
}

// Waiting indicates that the action is waiting to run
func (cfa *CurrentFileAction) Waiting() bool {
	return cfa.state == llapi.HsmProgressWaiting
}

// Running indicates that the action is running
func (cfa *CurrentFileAction) Running() bool {
	return cfa.state == llapi.HsmProgressRunning
}

// Done indicates that the action is done
func (cfa *CurrentFileAction) Done() bool {
	return cfa.state == llapi.HsmProgressDone
}

// IsArchive indicates whether or not the action is Archive
func (cfa *CurrentFileAction) IsArchive() bool {
	return cfa.action == llapi.HsmUserArchive
}

// IsRestore indicates whether or not the action is Restore
func (cfa *CurrentFileAction) IsRestore() bool {
	return cfa.action == llapi.HsmUserRestore
}

// IsRelease indicates whether or not the action is Release
func (cfa *CurrentFileAction) IsRelease() bool {
	return cfa.action == llapi.HsmUserRelease
}

// IsRemove indicates whether or not the action is Remove
func (cfa *CurrentFileAction) IsRemove() bool {
	return cfa.action == llapi.HsmUserRemove
}

// IsCancel indicates whether or not the action is Cancel
func (cfa *CurrentFileAction) IsCancel() bool {
	return cfa.action == llapi.HsmUserCancel
}

// IsNone indicates whether or not the action is None
func (cfa *CurrentFileAction) IsNone() bool {
	return cfa.action == llapi.HsmUserNone
}

func (cfa *CurrentFileAction) String() string {
	return fmt.Sprintf("[%s:%s] (%dB)", cfa.Action(), cfa.State(), cfa.BytesCopied)
}

// GetFileAction returns the current HSM action for the given path, if any
func GetFileAction(filePath string) (*CurrentFileAction, error) {
	hca, err := llapi.GetHsmCurrentAction(filePath)
	if err != nil {
		return nil, err
	}

	return &CurrentFileAction{
		action: hca.Action,
		state:  hca.State,

		Path:        filePath,
		BytesCopied: hca.Location.Length, // Offset is always 0
	}, nil
}
