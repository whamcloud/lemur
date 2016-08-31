#!/bin/bash

EXCLUDES="golang.org/x github.com/mjmac/go-ps" # stuff that isn't really 3rd-party, or is test-only
SCANDIR=${SCANDIR:-$(mktemp -d)}

imports=$(go list -f '{{.Deps}}' ./... | \
	tr "[" " " | tr "]" " " | \
	xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' | \
	sort | uniq)

for import in $imports; do
	dest=$SCANDIR/$(dirname $import)
	if [[ $import == *yaml.v2* ]]; then
		dest=$SCANDIR/$import
	fi
	for excl in $EXCLUDES; do
		if [[ $import == *$excl* ]]; then
			echo "Skipping $import (matches $excl)."
			continue 2
		fi
	done

	mkdir -p $dest
	echo -n "Copying $import -> $dest... "
	cp -a $GOPATH/src/$import $dest
	echo Done.
done

echo "SCANDIR: $SCANDIR"
