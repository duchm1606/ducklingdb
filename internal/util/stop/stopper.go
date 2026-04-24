package stop

import "sync"

type Stopper struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	stopped chan struct{}
	once    sync.Once
}

func NewStopper() *Stopper {
	return &Stopper{
		stopped: make(chan struct{}),
	}
}

func (s *Stopper) RunWorker(f func()) {
	s.mu.Lock()
	select {
	case <-s.stopped:
		s.mu.Unlock()
		return
	default:
	}
	s.wg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.wg.Done()
		f()
	}()
}

func (s *Stopper) ShouldStop() <-chan struct{} {
	return s.stopped
}

func (s *Stopper) Stop() {
	s.once.Do(func() {
		close(s.stopped)
	})
	s.wg.Wait()
}
