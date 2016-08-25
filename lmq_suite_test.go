package lmq_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLmq(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lmq Suite")
}

var _ = BeforeSuite(func() {
	err := os.Mkdir("./queue_data", 0755)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	err := os.RemoveAll("./queue_data")
	Expect(err).NotTo(HaveOccurred())
})
