package main_test

import (
	"github.intel.com/hpdd/test/harness"
	"github.intel.com/hpdd/test/log"
	"github.intel.com/hpdd/test/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"testing"
)

func CoordinatorStateIs(expected string) func() bool {
	return func() bool {
		state, err := harness.HsmCoordinatorState()
		log.Debug("Looking for CDT state %s, got %s (err: %v)", expected, state, err)
		return state == expected
	}

}

func TestCopytool(t *testing.T) {
	BeforeSuite(func() {
		log.AddDebugLogger(&log.ClosingGinkgoWriter{GinkgoWriter})
		CopytoolCLI, err := gexec.Build("github.intel.com/hpdd/lustrecli/cmd/copytool")
		Ω(err).ShouldNot(HaveOccurred())
		harness.SetTestBinary("copytool", CopytoolCLI)

		if err := harness.Setup(); err != nil {
			panic(err)
		}
		state, err := harness.HsmCoordinatorState()
		Expect(err).ToNot(HaveOccurred())
		if state != "stopped" {
			harness.ToggleHsmCoordinatorState("shutdown")
			Eventually(CoordinatorStateIs("stopped"), hsmActionTimeout, 1).Should(BeTrue())
		}

		harness.ToggleHsmCoordinatorState("purge")
		Expect(harness.ToggleHsmCoordinatorState("enabled")).To(Succeed())
		Eventually(CoordinatorStateIs("enabled"), hsmActionTimeout, 1).Should(BeTrue())
	})

	AfterSuite(func() {
		gexec.CleanupBuildArtifacts()
		if err := harness.Teardown(); err != nil {
			panic(err)
		}
		harness.ToggleHsmCoordinatorState("shutdown")
		Eventually(CoordinatorStateIs("stopped"), hsmActionTimeout, 1).Should(BeTrue())
		harness.ToggleHsmCoordinatorState("purge")
	})

	RegisterFailHandler(Fail)
	utils.RunSpecs(t, "Copytool Suite")
}
