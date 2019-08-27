// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"reflect"
	"testing"

	"github.com/intel-hpdd/lemur/dmplugin"
	"github.com/intel-hpdd/lemur/internal/testhelpers"
)

func TestGcsLoadConfig(t *testing.T) {
	var cfg gcsConfig
	cfgFile, cleanup := testhelpers.TempCopy(t, "./test-fixtures/lhsm-plugin-gcs.test", 0600)
	defer cleanup()
	err := dmplugin.LoadConfig(cfgFile, &cfg)
	loaded := &cfg
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	expected := &gcsConfig{
		Archives: archiveSet{
			&archiveConfig{
				Name:   "2",
				ID:     2,
				Bucket: "hpdd-test-bucket",
				Prefix: "archive-test",
			},
		},
	}

	if !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("\nexpected: \n\n%s\ngot: \n\n%s\n\n", expected, loaded)
	}
}
