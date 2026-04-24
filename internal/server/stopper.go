package server

import "github.com/duchm1606/ducklingdb/internal/util/stop"

// Stopper is re-exported from util/stop so callers in this package
// don't need to import two packages.
type Stopper = stop.Stopper

func NewStopper() *Stopper {
	return stop.NewStopper()
}
