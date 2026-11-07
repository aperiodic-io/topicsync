package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/aperiodic-io/topicsync/internal/retire"
	"github.com/aperiodic-io/topicsync/spec"
)

// fakeBroker is an in-process broker (franz-go's kfake) with one topic of 3 partitions holding a fresh record each.
func fakeBroker(t *testing.T) (*kfake.Cluster, *Cluster) {
	t.Helper()
	fc, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "busy"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fc.Close)
	p, err := kgo.NewClient(kgo.SeedBrokers(fc.ListenAddrs()...), kgo.DefaultProduceTopic("busy"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for part := range int32(3) {
		if err := p.ProduceSync(context.Background(), &kgo.Record{Partition: part, Value: []byte("x"), Timestamp: time.Now()}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	c, err := New(Config{Brokers: fc.ListenAddrs(), RequestTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return fc, c
}

// failListOffsets makes every ListOffsets answer carry a per-partition error (a storage error, an unavailable leader).
func failListOffsets(fc *kfake.Cluster, code int16) {
	fc.ControlKey(int16(kmsg.ListOffsets), func(r kmsg.Request) (kmsg.Response, error, bool) {
		fc.KeepControl() // every ListOffsets, not only the first
		req := r.(*kmsg.ListOffsetsRequest)
		resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewListOffsetsResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewListOffsetsResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = code
				sp.Offset, sp.Timestamp = -1, -1
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
}

// Regression (review finding 1): offsets that come back with per-partition errors were skipped, so a topic written
// seconds before read as empty and retire deleted it. Any such error is an error now, and retire refuses.
func TestOffsetErrorsNeverReadAsAnEmptyTopic(t *testing.T) {
	// NOT_LEADER persists past the client's own retries here (an election that does not settle)
	for _, code := range []int16{kerr.KafkaStorageError.Code, kerr.UnknownServerError.Code, kerr.NotLeaderForPartition.Code} {
		fc, c := fakeBroker(t)
		failListOffsets(fc, code)
		ctx := context.Background()
		if _, has, err := c.NewestRecordTime(ctx, "busy"); err == nil {
			t.Errorf("code %d: NewestRecordTime returned has=%v without error", code, has)
		}
		if _, err := c.EndOffsets(ctx, "busy"); err == nil {
			t.Errorf("code %d: EndOffsets returned without error", code)
		}
		d, err := spec.File{APIVersion: spec.APIVersion, Cluster: "c", Topics: []spec.Topic{{Name: "other", Partitions: 1, ReplicationFactor: 1}}}.Resolve()
		if err != nil {
			t.Fatal(err)
		}
		rep, err := retire.Retire(ctx, c, d, "busy", retire.Options{Confirm: "busy", MinIdle: time.Hour})
		if err == nil || rep.Deleted {
			t.Fatalf("code %d: retire with unreadable offsets: deleted %v, err %v", code, rep.Deleted, err)
		}
	}
}

func TestNewestRecordTimeReadsEveryPartition(t *testing.T) {
	_, c := fakeBroker(t)
	got, has, err := c.NewestRecordTime(context.Background(), "busy")
	if err != nil || !has || time.Since(got) > time.Minute {
		t.Fatalf("newest %v has %v err %v", got, has, err)
	}
	ends, err := c.EndOffsets(context.Background(), "busy")
	if err != nil || len(ends) != 3 || ends[0] != 1 {
		t.Fatalf("ends %v err %v", ends, err)
	}
}

// A partition that cannot be read to its end within the timeout is an error, never a partial answer (finding 2).
func TestAPartitionThatCannotBeReadIsAnError(t *testing.T) {
	fc, c := fakeBroker(t)
	fc.ControlKey(int16(kmsg.Fetch), func(r kmsg.Request) (kmsg.Response, error, bool) {
		fc.KeepControl()
		req := r.(*kmsg.FetchRequest)
		resp := req.ResponseKind().(*kmsg.FetchResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewFetchResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.LeaderNotAvailable.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	_, _, err := c.NewestRecordTime(context.Background(), "busy")
	if err == nil {
		t.Fatal("unreadable partitions gave an answer")
	}
	t.Logf("as expected: %v", err)
}
