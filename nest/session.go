package nest

import (
	"context"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/vx-labs/nest/nest/api"
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
	Records     []*api.Record
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
		minBatchSize: 0,
		current: Batch{
			FirstOffset: uint64(offset),
			Records:     []*api.Record{},
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
			record := &api.Record{}
			err = proto.Unmarshal(buf[:n], record)
			if err != nil {
				continue
			}
			s.current.Records = append(s.current.Records, record)
		}
		if n == 0 && len(s.current.Records) == 0 {
			if opts.EOFBehaviour == EOFBehaviourExit {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		} else {
			if len(s.current.Records) >= s.maxBatchSize {
				select {
				case s.ch <- s.current:
					s.current = Batch{
						FirstOffset: s.current.FirstOffset + uint64(len(s.current.Records)),
					}
				case <-ctx.Done():
					return
				}
			} else if len(s.current.Records) > s.minBatchSize {
				select {
				case s.ch <- s.current:
					s.current = Batch{
						FirstOffset: s.current.FirstOffset + uint64(len(s.current.Records)),
					}
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
		}
	}
}
