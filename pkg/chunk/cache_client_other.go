//go:build !linux

package chunk

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

func newRemoteCacheManager(dataSock, ctrlSock string, reg prometheus.Registerer) (CacheManager, error) {
	return nil, fmt.Errorf("shared cache server is only supported on Linux")
}
