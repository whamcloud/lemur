package client

import "github.intel.com/hpdd/lustre/fs"

// TestClient implements the client.Client interface
type testClient struct {
	root string
}

// Test returns a test client.
func Test(root string) Client {
	return &testClient{root: root}
}

// FsName returns a fake filesystem name
func (c *testClient) FsName() string {
	return "test"
}

// Path returns a fake filesystem path
func (c *testClient) Path() string {
	return c.root
}

// Root returns a fake fs.RootDir item
func (c *testClient) Root() fs.RootDir {
	// Todo need a TestRootDir
	return fs.RootDir{path: root}
}
