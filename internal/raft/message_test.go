package raft

import "testing"

func TestIsEmptyHardState(t *testing.T) {
	if !IsEmptyHardState(HardState{}) {
		t.Fatal("zero HardState should be empty")
	}
	if IsEmptyHardState(HardState{Term: 1}) {
		t.Fatal("non-zero HardState should not be empty")
	}
}
