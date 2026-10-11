package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"
)

const healthcheckTimeout = 2 * time.Second

// runHealthcheck is used as the container HEALTHCHECK command: it reports the
// process ready only when /readyz answers 2xx. Kept in the binary because the
// production image (distroless) ships no shell or curl.
func runHealthcheck() int {
	target := "http://127.0.0.1" + addressPort(configuredAddress()) + "/readyz"
	client := &http.Client{Timeout: healthcheckTimeout}

	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
		return 1
	}

	response, err := client.Do(request)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %s returned %d\n", target, response.StatusCode)
		return 1
	}
	return 0
}
