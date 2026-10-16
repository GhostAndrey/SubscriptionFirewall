// Package version carries build metadata injected via -ldflags "-X" at build
// time.
package version

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)
