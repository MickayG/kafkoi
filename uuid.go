package kafkoi

import (
	"github.com/nu7hatch/gouuid"
)

func createUUID() string {
	uuid4, err := uuid.NewV4()
	if err != nil {
		panic("Unable to generate consumer-group UUID")
	}

	return uuid4.String()
}
