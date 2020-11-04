package nest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/tysontate/gommap"
	"github.com/vx-labs/commitlog"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Recorder interface {
	io.Closer
	SetCurrentStateOffset(v uint64)
	CurrentStateOffset() uint64
	Snapshot() ([]byte, error)
	LookupTimestamp(ts uint64) uint64
	Consume(f func(r io.ReadSeeker) error) error
	Dump(w io.Writer, fromOffset uint64) error
	Append(stateOffset uint64, timestamps []uint64, payloads [][]byte) error
	Offset() uint64
	Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error
	Load(source io.Reader) error
	GetStatistics() commitlog.Statistics
	Latest() uint64
	Truncate() error
	SyncToIndex(stateIndex uint64) error
}

type DumpRecord struct {
	Offset  uint64 `json:"offset"`
	Payload []byte `json:"payload"`
}

// recorder integrates a commitlog with raft state
type recorder struct {
	mtx           sync.RWMutex
	id            uint64
	stream        string
	shard         uint64
	datadir       string
	stateOffset   gommap.MMap
	stateOffsetFd *os.File
	log           commitlog.CommitLog
}

func NewRecorder(id uint64, stream string, shard uint64, datadir string, logger *zap.Logger) (Recorder, error) {
	logger = logger.With(zap.String("recorder_stream", stream), zap.Uint64("recorder_shard", shard))
	logger.Debug("opening commit log")
	start := time.Now()
	log, err := commitlog.Open(path.Join(datadir, stream, fmt.Sprintf("%d", shard)), 250)
	if err != nil {
		return nil, err
	}
	logger.Debug("commit log opened", zap.Duration("elapsed_time", time.Since(start)))
	statePath := path.Join(datadir, fmt.Sprintf("%s-%d.state", stream, shard))
	var fd *os.File

	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		fd, err = os.OpenFile(statePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0650)
		if err != nil {
			return nil, err
		}
		err = fd.Truncate(8)
		if err != nil {
			fd.Close()
			os.Remove(statePath)
			return nil, err
		}
	} else {
		fd, err = os.OpenFile(statePath, os.O_RDWR, 0650)
		if err != nil {
			return nil, err
		}
	}
	mmapedData, err := gommap.Map(fd.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	s := &recorder{
		id:            id,
		stream:        stream,
		shard:         shard,
		datadir:       datadir,
		log:           log,
		stateOffset:   mmapedData,
		stateOffsetFd: fd,
	}
	logger.Debug("loaded log", zap.Uint64("current_log_offset", s.log.Offset()))
	return s, nil
}

func (s *recorder) GetStatistics() commitlog.Statistics {
	return s.log.GetStatistics()
}
func (s *recorder) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.stateOffset.UnsafeUnmap()
	s.stateOffsetFd.Close()
	return s.log.Close()
}

func (s *recorder) LookupTimestamp(ts uint64) uint64 {
	return s.log.LookupTimestamp(ts)
}
func (s *recorder) SyncToIndex(stateIndex uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.setCurrentStateOffset(stateIndex)
	return s.stateOffset.Sync(gommap.MS_SYNC)
}
func (s *recorder) Truncate() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.setCurrentStateOffset(0)
	s.log.Delete()
	log, err := commitlog.Open(path.Join(s.datadir, s.stream, fmt.Sprintf("%d", s.shard)), 250)
	if err != nil {
		return err
	}
	s.log = log
	return nil
}
func (s *recorder) Latest() uint64 {
	return s.log.Latest()
}
func (s *recorder) SetCurrentStateOffset(v uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.setCurrentStateOffset(v)
}
func (s *recorder) setCurrentStateOffset(v uint64) {
	binary.BigEndian.PutUint64(s.stateOffset, v)
}

func (s *recorder) CurrentStateOffset() uint64 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.currentStateOffset()
}
func (s *recorder) currentStateOffset() uint64 {
	return binary.BigEndian.Uint64(s.stateOffset)
}

func (s *recorder) Snapshot() ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return json.Marshal(Snapshot{
		Remote:         s.id,
		StateOffset:    s.currentStateOffset(),
		MessagesOffset: s.log.Offset(),
	})
}

func (s *recorder) Append(stateOffset uint64, timestamps []uint64, payloads [][]byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for idx := range payloads {
		_, err := s.log.WriteEntry(timestamps[idx], payloads[idx])
		if err != nil {
			return err
		}
	}
	s.setCurrentStateOffset(stateOffset)
	return nil
}

func (s *recorder) Consume(f func(r io.ReadSeeker) error) error {
	r := s.log.Reader()
	return f(r)
}
func (s *recorder) Load(source io.Reader) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	_, err := s.load(source, 0)
	return err
}
func (s *recorder) load(file io.Reader, lastOffset uint64) (int, error) {
	decoder := commitlog.NewDecoder(file)
	count := 0
	for {
		entry, err := decoder.Decode()
		if err != nil {
			if err == io.EOF {
				return count, nil
			}
			return count, err
		}
		if lastOffset > 0 && lastOffset <= entry.Offset() {
			return count, nil
		}
		if s.log.Offset() <= entry.Offset() {
			_, err := s.log.WriteEntry(entry.Timestamp(), entry.Payload())
			if err != nil {
				return count, err
			}
			count++
		}
	}
}

func (s *recorder) Offset() uint64 {
	return s.log.Offset()
}
func (s *recorder) restoreFromFile(ctx context.Context, snapshotDescription Snapshot, file io.ReadSeeker) error {
	L(ctx).Info("loading snapshot", zap.Uint64("remote_node", snapshotDescription.Remote), zap.Uint64("current_log_offset", s.log.Offset()), zap.Uint64("snapshot_log_offset", snapshotDescription.MessagesOffset))

	file.Seek(0, io.SeekStart)
	count, err := s.load(file, snapshotDescription.MessagesOffset)
	if err != nil {
		return err
	}
	s.setCurrentStateOffset(snapshotDescription.StateOffset)

	L(ctx).Info("loaded snapshot", zap.Int("loaded_entries", count), zap.Uint64("current_log_offset", s.log.Offset()))
	return nil
}

func (s *recorder) Dump(sink io.Writer, fromOffset uint64) error {
	r := s.log.Reader()
	_, err := r.Seek(int64(fromOffset), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = io.Copy(sink, r)
	return err
}

func (s *recorder) Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	L(ctx).Debug("restoring snapshot")

	snapshotDescription := Snapshot{}
	err := json.Unmarshal(snapshot, &snapshotDescription)
	if err != nil {
		L(ctx).Debug("failed to decode state snapshot", zap.Error(err))
	} else {
		if s.id == snapshotDescription.Remote {
			L(ctx).Info("refusing to load snapshot from ourselves", zap.Uint64("remote_node", snapshotDescription.Remote), zap.Uint64("current_log_offset", s.log.Offset()), zap.Uint64("snapshot_log_offset", snapshotDescription.MessagesOffset))
			return nil
		}
		file, err := ioutil.TempFile("", "sst-incoming.*.nest")
		if err != nil {
			L(ctx).Fatal("failed to create tmp file to receive snapshot", zap.Error(err))
		}
		defer os.Remove(file.Name())
		defer file.Close()
		err = caller(snapshotDescription.Remote, func(c *grpc.ClientConn) error {
			stream, err := api.NewStreamsClient(c).SST(ctx, &api.SSTRequest{
				Stream:     s.stream,
				Shard:      s.shard,
				ToOffset:   snapshotDescription.MessagesOffset,
				FromOffset: s.log.Offset(),
			})
			if err != nil {
				return err
			}

			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_, err = file.Write(chunk.Chunk)
				if err != nil {
					return err
				}
			}
			return file.Sync()
		})
		if err != nil {
			L(ctx).Fatal("failed to receive snapshot", zap.Error(err))
		}
		return s.restoreFromFile(ctx, snapshotDescription, file)
	}
	return nil
}
