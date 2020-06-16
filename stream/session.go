package stream

import (
	"context"
	"io"
	"log"
	"time"
)

type eofBehaviour int

const (
	// EOFBehaviourPoll will make the session poll for new records after an EOF error is received
	EOFBehaviourPoll eofBehaviour = 1 << iota
	// EOFBehaviourExit wil make the session exit when EOF is received
	EOFBehaviourExit eofBehaviour = 1 << iota
)

// ConsumerOptions describes stream session preferences
type ConsumerOptions struct {
	MaxBatchSize int
	FromOffset   int64
	EOFBehaviour eofBehaviour
}

type Batch struct {
	FirstOffset uint64
	Records     [][]byte
}
type session struct {
	maxBatchSize int
	minBatchSize int
	current      Batch
	ch           chan Batch
}

type Session interface {
	Ready() <-chan Batch
}

func NewSession(ctx context.Context, log io.ReadSeeker, opts ConsumerOptions) Session {
	offset, _ := log.Seek(opts.FromOffset, io.SeekStart)
	s := &session{
		ch:           make(chan Batch),
		maxBatchSize: opts.MaxBatchSize,
		minBatchSize: 10,
		current: Batch{
			FirstOffset: uint64(offset),
			Records:     [][]byte{},
		},
	}
	go s.run(ctx, log, opts)
	return s
}

func (s *session) Ready() <-chan Batch {
	return s.ch
}
func (s *session) run(ctx context.Context, r io.Reader, opts ConsumerOptions) {
	defer close(s.ch)
	buf := make([]byte, 20*1000*1000)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		n, err := r.Read(buf)
		if n > 0 {
			newRecord := make([]byte, n)
			copy(newRecord, buf[:n])
			s.current.Records = append(s.current.Records, newRecord)
		}
		if len(s.current.Records) >= s.maxBatchSize {
			select {
			case s.ch <- s.current:
				s.current = Batch{
					FirstOffset: s.current.FirstOffset + uint64(len(s.current.Records)),
					Records:     [][]byte{},
				}
			case <-ctx.Done():
				return
			}
		} else if len(s.current.Records) > s.minBatchSize || len(s.current.Records) > 0 && n == 0 {
			select {
			case s.ch <- s.current:
				s.current = Batch{
					FirstOffset: s.current.FirstOffset + uint64(len(s.current.Records)),
					Records:     [][]byte{},
				}
			case <-ctx.Done():
				return
			default:
				continue
			}
		}
		if err != nil {
			if err == io.EOF {
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
				log.Print(err)
			}
			return
		}

	}
}
