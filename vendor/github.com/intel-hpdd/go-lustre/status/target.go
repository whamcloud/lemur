// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package status

import (
	"bufio"
	"fmt"
	"os"
	"path"
)

type (
	// TargetIndex is the name of a target and its index.
	TargetIndex struct {
		Index int
		Name  string
	}
)

// LovTargets returns uuids and indices of the targts in an LOV.
// Path refers to a file or directory in a Lustre filesystem.
func LovTargets(p string) (result []TargetIndex, err error) {
	lov, err := LovName(p)
	if err != nil {
		return nil, err
	}

	return getTargetIndex("lov", lov)
}

// LmvTargets returns uuids and indices of the targts in an LmV.
// Path refers to a file or directory in a Lustre filesystem.
func LmvTargets(p string) (result []TargetIndex, err error) {
	lmv, err := LmvName(p)
	if err != nil {
		return nil, err
	}

	return getTargetIndex("lmv", lmv)
}

func getTargetIndex(targetType, targetName string) (result []TargetIndex, err error) {
	name := path.Join(procBase, targetType, targetName, "target_obd")
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var tgt TargetIndex
		fmt.Sscanf(scanner.Text(), "%d: %s", &tgt.Index, &tgt.Name)
		result = append(result, tgt)
	}
	return result, nil
}
