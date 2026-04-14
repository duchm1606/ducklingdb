package hlc

import "fmt"

// Timestamp is a hybrid logical clock timestamp: a wall-clock component in
// nanoseconds plus a logical counter that breaks ties when two events share
// the same wall time.
//
// Comparison is lexicographic: first by WallTime, then by Logical. This
// produces a total order that respects physical time while staying causal
// under clock skew via the Logical component.
type Timestamp struct {
	WallTime int64 // nanoseconds since the Unix epoch
	Logical  int32 // tie-breaker for events sharing a WallTime
}

// IsEmpty returns true when the timestamp is the zero value.
func (t Timestamp) IsEmpty() bool {
	return t.WallTime == 0 && t.Logical == 0
}

// Less reports whether t strictly precedes other.
func (t Timestamp) Less(other Timestamp) bool {
	if t.WallTime != other.WallTime {
		return t.WallTime < other.WallTime
	}
	return t.Logical < other.Logical
}

// LessEq reports whether t is at or before other.
func (t Timestamp) LessEq(other Timestamp) bool {
	return t == other || t.Less(other)
}

// Equal reports whether two timestamps are identical.
func (t Timestamp) Equal(other Timestamp) bool {
	return t == other
}

// Next returns the smallest timestamp strictly greater than t.
// Used when a read timestamp must be pushed past a conflicting write.
func (t Timestamp) Next() Timestamp {
	if t.Logical == (1<<31)-1 {
		return Timestamp{WallTime: t.WallTime + 1, Logical: 0}
	}
	return Timestamp{WallTime: t.WallTime, Logical: t.Logical + 1}
}

// Prev returns the largest timestamp strictly less than t.
// Undefined behavior when called on the zero value.
func (t Timestamp) Prev() Timestamp {
	if t.Logical == 0 {
		return Timestamp{WallTime: t.WallTime - 1, Logical: (1 << 31) - 1}
	}
	return Timestamp{WallTime: t.WallTime, Logical: t.Logical - 1}
}

// String renders the timestamp in a "wall.logical" form convenient for logs.
func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%010d", t.WallTime, t.Logical)
}
