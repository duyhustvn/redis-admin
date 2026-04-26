// Package sentinel provides Redis Sentinel connection and topology primitives.
package sentinel

import "errors"

// Typed errors for expected sentinel failure modes.
var (
	// ErrNoMaster is returned when no master can be resolved from Sentinel.
	ErrNoMaster = errors.New("no master found")

	// ErrQuorumNotMet is returned when fewer than quorum sentinels are reachable.
	ErrQuorumNotMet = errors.New("sentinel quorum not met")

	// ErrNodeUnreachable is returned when a Redis or Sentinel node cannot be connected to.
	ErrNodeUnreachable = errors.New("node unreachable")

	// ErrFlapping is returned when a node oscillates between up and down rapidly.
	ErrFlapping = errors.New("node is flapping — check CNI/network stability")
)
