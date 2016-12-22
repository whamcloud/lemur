// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"gopkg.in/urfave/cli.v1"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/hsm"
)

func init() {
	hsmStateFlags := strings.Join(hsm.GetStateFlagNames(), ",")

	hsmCommands := []cli.Command{
		{
			Name:      "archive",
			Usage:     "Initiate HSM archive of specified paths",
			ArgsUsage: "[path [path...]]",
			Action:    hsmRequestAction(hsm.RequestArchive),
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "id, i",
					Usage: "Numeric ID of archive backend",
				},
				cli.BoolFlag{
					Name:  "null, 0",
					Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
				},
			},
		},
		{
			Name:      "release",
			Usage:     "Release local data of HSM-archived paths",
			ArgsUsage: "[path [path...]]",
			Action:    hsmRequestAction(hsm.RequestRelease),
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "null, 0",
					Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
				},
			},
		},
		{
			Name:      "restore",
			Usage:     "Explicitly restore local data of HSM-archived paths",
			ArgsUsage: "[path [path...]]",
			Action:    hsmRequestAction(hsm.RequestRestore),
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "null, 0",
					Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
				},
			},
		},
		{
			Name:      "remove",
			Usage:     "Remove HSM-archived data of specified paths (local data is not removed)",
			ArgsUsage: "[path [path...]]",
			Action:    hsmRequestAction(hsm.RequestRemove),
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "null, 0",
					Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
				},
			},
		},
		{
			Name:      "cancel",
			Usage:     "Cancel HSM operations being performed on specified paths",
			ArgsUsage: "[path [path...]]",
			Action:    hsmRequestAction(hsm.RequestCancel),
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "null, 0",
					Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
				},
			},
		},
		{
			Name:      "set",
			Usage:     "Set HSM flags or archive ID for specified paths",
			ArgsUsage: "[path [path...]]",
			Action:    hsmSetAction,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "null, 0",
					Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
				},
				cli.IntFlag{
					Name:  "id, i",
					Usage: "Numeric ID of archive backend",
				},
				cli.StringSliceFlag{
					Name:  "flag, f",
					Usage: fmt.Sprintf("HSM flag to set (%s)", hsmStateFlags),
					Value: &cli.StringSlice{},
				},
				cli.StringSliceFlag{
					Name:  "clear, F",
					Usage: fmt.Sprintf("HSM flag to clear (%s)", hsmStateFlags),
					Value: &cli.StringSlice{},
				},
			},
		},
		{
			Name:      "status",
			Usage:     "Display HSM status for specified paths",
			ArgsUsage: "[path [path...]]",
			Action:    hsmStatusAction,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "action, a",
					Usage: "Include current HSM action",
				},
				cli.BoolFlag{
					Name:  "hide-path, H",
					Usage: "Hide pathname in output",
				},
				cli.BoolFlag{
					Name:  "long, l",
					Usage: "Show long-form states",
				},
				cli.BoolFlag{
					Name:  "progress, p",
					Usage: "Show copy progress for archive/restore actions",
				},
				cli.BoolFlag{
					Name:  "null, 0",
					Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
				},
			},
		},
		{
			Name:      "import",
			Usage:     "Import an HSM-backed file.",
			ArgsUsage: "path",
			Action:    hsmImportAction,
			Flags: []cli.Flag{
				cli.UintFlag{
					Name:  "id, i",
					Usage: "Numeric ID of archive backend",
				},
				cli.StringFlag{
					Name:  "uuid",
					Usage: "File's UUID",
				},
				cli.StringFlag{
					Name:  "hash",
					Usage: "Checksum hash value",
				},
				cli.StringFlag{
					Name:  "uid",
					Usage: "Owner uid",
				},
				cli.StringFlag{
					Name:  "gid",
					Usage: "Owner gid",
				},
				cli.Int64Flag{
					Name:  "size",
					Usage: "Size of file in bytes",
				},
				cli.UintFlag{
					Name:  "mode",
					Value: 0644,
					Usage: "File mode",
				},
				cli.StringFlag{
					Name:  "timefmt",
					Value: "2006-01-02 15:04:05.999999999 -0700",
					Usage: "Format for time stamp see golang time.Parse documentation",
				},
				cli.StringFlag{
					Name:  "mtime",
					Usage: "Modification time (default: current time)",
				},
				cli.StringFlag{
					Name:  "atime",
					Usage: "Last access time (default: set to mtime)",
				},
				cli.IntFlag{
					Name:  "stripe_count",
					Value: 1,
					Usage: "Set number of stripes in file",
				},
				cli.IntFlag{
					Name:  "stripe_size",
					Value: 1 << 20,
					Usage: "Set stripe size in bytes",
				},
			},
		},
		{
			Name:      "clone",
			Usage:     "Create a relased copy of an HSM-backed file.",
			ArgsUsage: "source_file target_file",
			Action:    hsmCloneAction,
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "stripe_count",
					Usage: "Override the number of stripes in the target copy.",
				},
				cli.IntFlag{
					Name:  "stripe_size",
					Usage: "Override stripe size (bytes) in target copy.",
				},
			},
		},
		{
			Name:      "restripe",
			Usage:     "Change stripe parameters of a released file.",
			ArgsUsage: "file",
			Action:    hsmRestripeAction,
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "stripe_count",
					Usage: "Override the number of stripes in the target copy.",
				},
				cli.IntFlag{
					Name:  "stripe_size",
					Usage: "Override stripe size (bytes) in target copy.",
				},
			},
		},
	}
	commands = append(commands, hsmCommands...)
}

func getFilePaths(c *cli.Context) ([]string, error) {
	var paths []string

	if c.Bool("null") {
		reader := bufio.NewReader(os.Stdin)
		path, err := reader.ReadBytes('\000')
		for err == nil {
			paths = append(paths, string(path[:len(path)-1]))
			path, err = reader.ReadBytes('\000')
		}
		if err != io.EOF {
			return nil, err
		}
	} else {
		paths = c.Args()
	}

	return paths, nil
}

func getPathStatus(c *cli.Context, filePath string) (string, error) {
	var buf bytes.Buffer

	s, err := hsm.GetFileStatus(filePath)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get HSM status for %s", filePath)
	}

	if !c.Bool("hide-path") {
		fmt.Fprintf(&buf, "%s ", filePath)
	}
	fmt.Fprintf(&buf, hsm.FileStatusString(s, !c.Bool("long")))

	if s.Exists() && c.Bool("action") {
		a, err := hsm.GetFileAction(filePath)
		if err != nil {
			return "", errors.Wrapf(err, "Failed to get current HSM action for %s", filePath)
		}
		if a.IsNone() {
			fmt.Fprintf(&buf, " -")
		} else {
			fmt.Fprintf(&buf, " [%s:%s]", a.Action(), a.State())
			if c.Bool("progress") && (a.IsArchive() || a.IsRestore()) {
				st, err := os.Stat(filePath)
				if err != nil {
					return "", errors.Wrapf(err, "Failed to stat() %s", filePath)
				}
				fmt.Fprintf(&buf, "(%s/%s)",
					humanize.IBytes(uint64(a.BytesCopied)),
					humanize.IBytes(uint64(st.Size())))
			}
		}
	} else {
		fmt.Fprintf(&buf, " -")
	}

	// TODO: Display xattrs, once we've standardized them?

	return buf.String(), nil
}

func hsmSetAction(c *cli.Context) error {
	logContext(c)

	paths, err := getFilePaths(c)
	if err != nil {
		return err
	}

	if len(paths) < 1 {
		return errors.New("HSM set request must be made with at least 1 path")
	}

	setFlags, err := hsm.GetStatusMask(c.StringSlice("flag"))
	if err != nil {
		return err
	}
	clearFlags, err := hsm.GetStatusMask(c.StringSlice("clear"))
	if err != nil {
		return err
	}
	archiveID := uint32(c.Int("id"))

	if setFlags == 0 && clearFlags == 0 && archiveID == 0 {
		return errors.New("HSM set request made with no flags to set or clear, and no new archive ID supplied")
	}

	// TODO: Parallelize this?
	for _, path := range paths {
		if err := hsm.SetFileStatus(path, setFlags, clearFlags, archiveID); err != nil {
			return err
		}
	}

	return nil
}

func hsmStatusAction(c *cli.Context) error {
	logContext(c)

	paths, err := getFilePaths(c)
	if err != nil {
		return err
	}

	if len(paths) < 1 {
		return errors.New("HSM status request must be made with at least 1 path")
	}

	for _, path := range paths {
		status, err := getPathStatus(c, path)
		if err != nil {
			return errors.Errorf("%s: %v", path, err)
		}
		fmt.Println(status)
	}

	return nil
}

type hsmRequestFn func(fs.RootDir, uint, []*lustre.Fid) error

func hsmRequestAction(requestFn func(fs.RootDir, uint, []*lustre.Fid) error) cli.ActionFunc {
	return func(c *cli.Context) error {
		logContext(c)

		paths, err := getFilePaths(c)
		if err != nil {
			return err
		}

		return submitHsmRequest(c.Command.Name, hsmRequestFn(requestFn), uint(c.Int("id")), paths...)
	}
}

func submitHsmRequest(actionName string, requestFn hsmRequestFn, archiveID uint, paths ...string) error {
	var fids []*lustre.Fid

	if len(paths) < 1 {
		return fmt.Errorf("HSM %s request must be made with at least 1 path", actionName)
	}

	fsRoot, err := fs.MountRoot(paths[0])
	if err != nil {
		return fmt.Errorf("Error getting fs root from %s: %s", paths[0], err)
	}

	// TODO: Occurs to me that it might be better to break up a large
	// batch into multiple batches, each serviced by its own goroutine.
	for _, path := range paths {
		absPath, err2 := filepath.Abs(path)
		if err2 != nil {
			return fmt.Errorf("Cannot resolve absolute path for %s: %s", path, err)
		}
		if !strings.HasPrefix(absPath, fsRoot.Path()) {
			return fmt.Errorf("All files in HSM request must be in the same filesystem (%s is not in %s)",
				path, fsRoot)
		}

		fid, err2 := fs.LookupFid(path)
		if err2 != nil {
			return fmt.Errorf("Cannot resolve Fid for %s: %s", path, err)
		}
		fids = append(fids, fid)
	}

	if requestFn != nil {
		err = requestFn(fsRoot, archiveID, fids)
	} else {
		err = fmt.Errorf("Unhandled HSM action: %s", actionName)
	}

	return err
}
