package raft

import (
	"os"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
)

func newTestEngine(t *testing.T) *lsm.LSMEngine {
	t.Helper()
	dir, err := os.MkdirTemp("", "raft-storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	eng, err := lsm.OpenLSM(lsm.LSMOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	return eng
}

func TestLSMLogStorageEmpty(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))
	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if first != 1 || last != 0 {
		t.Fatalf("empty storage: want first=1 last=0, got first=%d last=%d", first, last)
	}
	hs, _ := s.InitialState()
	if !IsEmptyHardState(hs) {
		t.Fatal("empty storage should have empty HardState")
	}
}

func TestLSMLogStorageAppendAndEntries(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))
	entries := []Entry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 2, Index: 3, Data: []byte("c")},
	}
	if err := s.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	last, _ := s.LastIndex()
	if last != 3 {
		t.Fatalf("want LastIndex=3, got %d", last)
	}
	got, err := s.Entries(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || string(got[1].Data) != "b" {
		t.Fatalf("unexpected entries: %v", got)
	}
}

func TestLSMLogStorageHardState(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))
	hs := HardState{Term: 3, VotedFor: 2, Commit: 5}
	if err := s.SaveHardState(hs); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if loaded != hs {
		t.Fatalf("want %+v, got %+v", hs, loaded)
	}
}

func TestLSMLogStorageTerm(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))
	s.AppendEntries([]Entry{{Term: 2, Index: 1}, {Term: 3, Index: 2}})
	term, err := s.Term(2)
	if err != nil || term != 3 {
		t.Fatalf("want term=3, got term=%d err=%v", term, err)
	}
}
