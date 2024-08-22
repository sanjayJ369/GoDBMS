package util

import "log"

func Assert(val bool) {
	log.Fatalf("expected value to be true")
}
