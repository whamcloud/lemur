package main_test

import (
	"github.intel.com/hpdd/lustre/hsm"
	"github.intel.com/hpdd/lustre/system"
	"github.intel.com/hpdd/test/harness"
	"github.intel.com/hpdd/test/log"
	"github.intel.com/hpdd/test/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"syscall"
	"time"
)

// Adjust for slow systems
var hsmActionTimeout = 30 * time.Second

var copytoolMount = fmt.Sprintf("%s.ct", harness.ClientMount())
var hsmArchive string
var copytoolSession *gexec.Session

func doCopytoolSetup(backendType string) error {
	var err error
	hsmArchive, err = ioutil.TempDir(harness.TestRoot(), "archive")
	if err != nil {
		return err
	}

	if err = harness.DoClientMounts([]string{copytoolMount}, nil); err != nil {
		return err
	}

	posixArchive := fmt.Sprintf("%s:%s:1::%s:false", backendType, backendType, hsmArchive)

	command := exec.Command(harness.TestBinary("copytool"), "--disable-config", "--disable-mirror", "-logtostderr=true",
		"--archive", posixArchive, "--mnt", copytoolMount)
	copytoolSession, err = gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())

	time.Sleep(500 * time.Millisecond)
	return nil
}

func doCopytoolTeardown() error {
	copytoolSession.Interrupt()
	Eventually(copytoolSession).Should(gexec.Exit(0))

	if err := harness.Unmount(copytoolMount); err != nil {
		log.Error("Unmount failed: %s", err)
		return err
	}

	if err := os.RemoveAll(hsmArchive); err != nil {
		log.Error("Failed to remove %s: %s", hsmArchive, err)
		return err
	}

	return nil
}

func markFileForHsmAction(targetFile string, hsmAction string) error {
	var args []string
	switch hsmAction {
	case "archive":
		args = append(args, "hsm_archive", "--archive", "1", targetFile)
	case "restore":
		args = append(args, "hsm_restore", targetFile)
	case "release":
		args = append(args, "hsm_release", targetFile)
	default:
		panic(fmt.Sprintf("Unknown HSM action: %s", hsmAction))
	}

	command := exec.Command("lfs", args...)
	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Ω(err).ShouldNot(HaveOccurred())
	Eventually(session).Should(gexec.Exit(0))

	return nil
}

func getFileBlocks(filePath string) (int64, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	stat, ok := fi.Sys().(syscall.Stat_t)
	if !ok {
		panic("Stat did not provide a Stat_t")
	}

	return stat.Blocks, nil
}

func getFileMd5Sum(filePath string) ([]byte, error) {
	f, err := os.Open(filePath)
	defer f.Close()

	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Mode().IsDir() {
		return nil, errors.New("Can't md5sum a directory, dummy!")
	}
	md5 := md5.New()
	io.Copy(md5, f)
	return md5.Sum(nil), nil
}

var _ = Describe("When HSM is enabled,", func() {
	Describe("the POSIX Copytool", func() {
		names := []string{"foo", "bar", "baz"}
		testFiles := make([]string, 3)
		for i, n := range names {
			testFiles[i] = utils.TestFilePath(n)
		}

		BeforeEach(func() {
			Expect(CoordinatorStateIs("enabled")()).To(BeTrue())
			Expect(doCopytoolSetup("posix")).To(Succeed())
			for i, n := range names {
				testFiles[i] = utils.CreateTestFile(n)
			}
		})
		AfterEach(func() {
			Expect(doCopytoolTeardown()).To(Succeed())
			for _, file := range testFiles {
				Expect(os.Remove(file)).To(Succeed())
			}
		})

		// FIXME: Not really happy about the fact that these tests
		// rely on knowing way too much about the backend
		// implementation.
		Describe("responds to an archive request", func() {
			It("by copying the file into the archive.", func() {
				Ω(markFileForHsmAction(testFiles[0], "archive")).Should(Succeed())
				Eventually(func() bool {
					uuid, err := system.Lgetxattr(testFiles[0], "user.hsm_guid")
					if err != nil {
						// This is an expected error.
						Ω(err.Error()).Should(MatchRegexp(".*no data available$"))
						return false
					}

					uuidStr := string(uuid)
					archiveFile := path.Join(hsmArchive,
						"objects",
						fmt.Sprintf("%s", uuidStr[0:2]),
						fmt.Sprintf("%s", uuidStr[2:4]),
						uuidStr)

					_, err = os.Stat(archiveFile)
					log.Debug("Looking for %s: %s", archiveFile, err)
					return err == nil
				}, hsmActionTimeout, 0.5).Should(BeTrue())
			})
		})

		Describe("responds to restore requests", func() {
			var origMd5Sum []byte
			var err error
			testFile := testFiles[1]

			BeforeEach(func() {
				origMd5Sum, err = getFileMd5Sum(testFile)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(markFileForHsmAction(testFile, "archive")).Should(Succeed())
				Eventually(func() bool {
					state, err := hsm.State(testFile)
					Ω(err).ShouldNot(HaveOccurred())
					log.Debug("%s: %s", testFile, state.String())
					return state.Archived()
				}, hsmActionTimeout, 0.5).Should(BeTrue())
				Ω(markFileForHsmAction(testFile, "release")).Should(Succeed())
				Eventually(func() bool {
					state, err := hsm.State(testFile)
					Ω(err).ShouldNot(HaveOccurred())
					log.Debug("%s: %s", testFile, state.String())
					return state.Released()
				}, hsmActionTimeout, 0.5).Should(BeTrue())
			})

			It("by explicitly restoring the file contents from the archive.", func() {
				Ω(markFileForHsmAction(testFile, "restore")).Should(Succeed())

				Eventually(func() bool {
					state, err := hsm.State(testFile)
					Ω(err).ShouldNot(HaveOccurred())
					log.Debug("%s: %s", testFile, state.String())
					return state.Released()
				}, hsmActionTimeout, 0.5).Should(BeFalse())
				newMd5Sum, err := getFileMd5Sum(testFile)
				Ω(err).ShouldNot(HaveOccurred())
				Expect(bytes.Equal(newMd5Sum, origMd5Sum)).To(BeTrue())
			})

			It("by implicitly restoring the file contents from the archive.", func() {
				newMd5Sum, err := getFileMd5Sum(testFile)
				Ω(err).ShouldNot(HaveOccurred())
				Expect(bytes.Equal(newMd5Sum, origMd5Sum)).To(BeTrue())
			})

		})
	})
})
