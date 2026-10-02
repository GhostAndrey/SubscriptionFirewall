// Package version carries build metadata injected via -ldflags "-X" at build time.
package version

var (
	// Version is the semantic version or git tag; "dev" for local builds.
	Version = "dev"
	// Commit is the source revision the binary was built from.
	Commit = "unknown"
	// BuildDate is the RFC3339 build timestamp, if provided.
	BuildDate = "unknown"
)
