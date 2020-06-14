package nest

import (
	"context"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/vx-labs/nest/nest/api"
)

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
	offset, _ := log.Seek(0, io.SeekCurrent)
	s := &session{
		ch:           make(chan Batch),
		maxBatchSize: opts.MaxBatchSize,
		minBatchSize: 0,
		current: Batch{
			FirstOffset: uint64(offset),
			Records:     []*api.Record{},
		},
	}
	go s.run(ctx, log)
	return s
}

func (s *session) Ready() <-chan Batch {
	return s.ch
}
func (s *session) run(ctx context.Context, r io.Reader) {
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
			} else if len(s.current.Records) >= s.minBatchSize {
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
