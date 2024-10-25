package util

import (
	"fmt"
	"log"

	"github.com/google/uuid"
)

func NewTempFileLoc() string {
	id, err := uuid.NewUUID()
	if err != nil {
		log.Fatalf("getting new uuid: %s", err)
	}

	return fmt.Sprintf("/tmp/%d", id.ID())
}
