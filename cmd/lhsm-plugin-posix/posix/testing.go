package posix

import "github.intel.com/hpdd/lustre/fs"

// TestClient implements the client.Client interface
type TestClient struct{}

// FsName returns a fake filesystem name
func (c *TestClient) FsName() string {
	return "test"
}

// Path returns a fake filesystem path
func (c *TestClient) Path() string {
	return "/fake/test/path"
}

// Root returns a fake fs.RootDir item
func (c *TestClient) Root() fs.RootDir {
	return fs.RootDir{}
}
