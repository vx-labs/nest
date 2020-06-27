package stream

import (
	"context"
	"io"
	"time"

	"github.com/vx-labs/nest/commitlog"
)

type eofBehaviour int

const (
	// EOFBehaviourPoll will make the session poll for new records after an EOF error is received
	EOFBehaviourPoll eofBehaviour = 1 << iota
	// EOFBehaviourExit wil make the session exit when EOF is received
	EOFBehaviourExit eofBehaviour = 1 << iota
)

// ConsumerOpts describes stream session preferences
type ConsumerOpts struct {
	MaxBatchSize   int
	FromOffset     int64
	EOFBehaviour   eofBehaviour
	OffsetProvider OffsetIterator
}

type Batch struct {
	FirstOffset uint64
	Records     [][]byte
}
type poller struct {
	maxBatchSize int
	minBatchSize int
	current      Batch
	ch           chan Batch
	err          error
}

type Poller interface {
	Ready() <-chan Batch
	Error() error
}

func newPoller(ctx context.Context, r io.ReadSeeker, opts ConsumerOpts) Poller {
	var offset int64

	offset, _ = r.Seek(opts.FromOffset, io.SeekStart)
	if opts.OffsetProvider != nil {
		opts.OffsetProvider.AdvanceTo(uint64(opts.FromOffset))
	}
	s := &poller{
		ch:           make(chan Batch),
		maxBatchSize: opts.MaxBatchSize,
		minBatchSize: 10,
		current: Batch{
			FirstOffset: uint64(offset),
			Records:     [][]byte{},
		},
	}
	go s.run(ctx, r, opts)
	return s
}

func (s *poller) Error() error {
	return s.err
}
func (s *poller) Ready() <-chan Batch {
	return s.ch
}
func (s *poller) waitFlush(ctx context.Context) error {
	if len(s.current.Records) == 0 {
		return nil
	}
	select {
	case s.ch <- s.current:
		s.current = Batch{
			FirstOffset: s.current.FirstOffset + uint64(len(s.current.Records)),
			Records:     [][]byte{},
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
func (s *poller) tryFlush(ctx context.Context) error {
	if len(s.current.Records) == 0 {
		return nil
	}
	select {
	case s.ch <- s.current:
		s.current = Batch{
			FirstOffset: s.current.FirstOffset + uint64(len(s.current.Records)),
			Records:     [][]byte{},
		}
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}
func (s *poller) run(ctx context.Context, r io.ReadSeeker, opts ConsumerOpts) {
	decoder := commitlog.NewDecoder(r)
	defer close(s.ch)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if opts.OffsetProvider != nil {
			next, err := opts.OffsetProvider.Next()
			if err != nil {
				if err == io.EOF {
					if err := s.waitFlush(ctx); err != nil {
						s.err = err
					}
					if opts.EOFBehaviour == EOFBehaviourExit {
						return
					}
					select {
					case <-ticker.C:
						continue
					case <-ctx.Done():
						return
					}
				} else {
					s.err = err
					return
				}
			}
			_, err = r.Seek(int64(next), io.SeekStart)
			if err != nil {
				s.err = err
				return
			}
		}
		entry, err := decoder.Decode()
		if err == nil {
			s.current.Records = append(s.current.Records, entry.Payload())
		}
		if len(s.current.Records) >= s.maxBatchSize {
			if err := s.waitFlush(ctx); err != nil {
				s.err = err
				return
			}
			continue
		} else if len(s.current.Records) > s.minBatchSize || entry == nil {
			if err := s.tryFlush(ctx); err != nil {
				s.err = err
				return
			}
		}
		if err != nil {
			if err == io.EOF {
				if err := s.waitFlush(ctx); err != nil {
					s.err = err
				}
				if opts.EOFBehaviour == EOFBehaviourExit {
					return
				}
				select {
				case <-ticker.C:
					continue
				case <-ctx.Done():
					return
				}
			} else {
				s.err = err
			}
			return
		}
	}
}
