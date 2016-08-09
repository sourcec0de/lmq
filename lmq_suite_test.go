package lmq_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLmq(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lmq Suite")
}
