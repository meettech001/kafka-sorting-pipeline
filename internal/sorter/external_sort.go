package sorter

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/segmentio/kafka-go"

	"github.com/example/kafka-sorting-pipeline/internal/data"
)

// SortKey identifies which field to sort by.
type SortKey int

const (
	SortByID SortKey = iota
	SortByName
	SortByContinent
)

func (k SortKey) String() string {
	switch k {
	case SortByID:
		return "id"
	case SortByName:
		return "name"
	case SortByContinent:
		return "continent"
	default:
		return "unknown"
	}
}

// ExternalSortAndProduce performs an external sort over srcFile by key and writes the
// sorted result directly to Kafka via the provided writer.
func ExternalSortAndProduce(
	ctx context.Context,
	srcFile string,
	tmpDir string,
	chunkSize int,
	key SortKey,
	writer *kafka.Writer,
	outputFile string,
) error {
	// Phase 1: split into sorted chunks.
	chunkFiles, err := createSortedChunks(srcFile, tmpDir, chunkSize, key)
	if err != nil {
		return fmt.Errorf("create sorted chunks: %w", err)
	}

	// Phase 2: k-way merge chunks and stream into Kafka.
	if err := mergeChunksAndProduce(ctx, chunkFiles, key, writer); err != nil {
		return fmt.Errorf("merge chunks: %w", err)
	}

	return nil
}

// createSortedChunks reads the source file, groups records into chunks of at most
// chunkSize records in memory, sorts each chunk, and writes it as a temporary file.
func createSortedChunks(srcFile, tmpDir string, chunkSize int, key SortKey) ([]string, error) {
	f, err := os.Open(srcFile)
	if err != nil {
		return nil, fmt.Errorf("open src: %w", err)
	}
	defer f.Close()

	if chunkSize <= 0 {
		chunkSize = 1_000_000
	}

	reader := bufio.NewReaderSize(f, 1<<20)
	var records []data.Record
	records = make([]data.Record, 0, chunkSize)

	chunkFiles := make([]string, 0, 16)
	chunkIndex := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read line: %w", err)
		}
		if len(line) > 0 {
			rec, perr := data.ParseCSV(line)
			if perr != nil {
				return nil, fmt.Errorf("parse csv: %w", perr)
			}
			records = append(records, rec)
		}

		if len(records) >= chunkSize || (err == io.EOF && len(records) > 0) {
			// Sort in-memory chunk.
			sort.Slice(records, func(i, j int) bool {
				switch key {
				case SortByID:
					return records[i].ID < records[j].ID
				case SortByName:
					if records[i].Name == records[j].Name {
						return records[i].ID < records[j].ID
					}
					return records[i].Name < records[j].Name
				case SortByContinent:
					if records[i].Continent == records[j].Continent {
						return records[i].ID < records[j].ID
					}
					return records[i].Continent < records[j].Continent
				default:
					return false
				}
			})

			chunkPath := filepath.Join(tmpDir, fmt.Sprintf("chunk-%06d.csv", chunkIndex))
			if err := writeChunk(chunkPath, records); err != nil {
				return nil, fmt.Errorf("write chunk: %w", err)
			}
			chunkFiles = append(chunkFiles, chunkPath)
			chunkIndex++
			records = records[:0]
		}

		if err == io.EOF {
			break
		}
	}

	return chunkFiles, nil
}

func writeChunk(path string, records []data.Record) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create chunk file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	for i := range records {
		if _, err := w.WriteString(records[i].ToCSV()); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
		if err := w.WriteByte('\n'); err != nil {
			return fmt.Errorf("write newline: %w", err)
		}
	}
	return w.Flush()
}

// mergeChunksAndProduce performs a k-way merge across chunkFiles and writes
// the merged (globally sorted) sequence into Kafka.
func mergeChunksAndProduce(ctx context.Context, chunkFiles []string, key SortKey, writer *kafka.Writer) error {
	type chunkReader struct {
		f   *os.File
		buf *bufio.Reader
	}

	readers := make([]*chunkReader, len(chunkFiles))
	for i, path := range chunkFiles {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open chunk %s: %w", path, err)
		}
		readers[i] = &chunkReader{
			f:   f,
			buf: bufio.NewReaderSize(f, 1<<20),
		}
		defer f.Close()
	}

	// Min-heap by current record.
	h := &recordHeap{
		key: key,
	}
	heap.Init(h)

	// Prime heap with first record from each chunk.
	for idx, r := range readers {
		line, err := r.buf.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("prime read chunk: %w", err)
		}
		if len(line) == 0 {
			continue
		}
		rec, perr := data.ParseCSV(line)
		if perr != nil {
			return fmt.Errorf("parse primed csv: %w", perr)
		}
		heap.Push(h, &heapItem{
			rec:   rec,
			index: idx,
		})
	}

	const batchSize = 10_000
	msgs := make([]kafka.Message, 0, batchSize)

	for h.Len() > 0 {
		item := heap.Pop(h).(*heapItem)
		msgs = append(msgs, kafka.Message{
			Value: []byte(item.rec.ToCSV()),
		})

		if len(msgs) >= batchSize {
			if err := writer.WriteMessages(ctx, msgs...); err != nil {
				return fmt.Errorf("write batch: %w", err)
			}
			msgs = msgs[:0]
		}

		// Refill from the same chunk.
		r := readers[item.index]
		line, err := r.buf.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("read from chunk: %w", err)
		}
		if len(line) > 0 {
			rec, perr := data.ParseCSV(line)
			if perr != nil {
				return fmt.Errorf("parse csv: %w", perr)
			}
			heap.Push(h, &heapItem{
				rec:   rec,
				index: item.index,
			})
		}
	}

	if len(msgs) > 0 {
		if err := writer.WriteMessages(ctx, msgs...); err != nil {
			return fmt.Errorf("write last batch: %w", err)
		}
	}

	return nil
}

type heapItem struct {
	rec   data.Record
	index int // which chunk
}

type recordHeap struct {
	items []*heapItem
	key   SortKey
}

func (h recordHeap) Len() int { return len(h.items) }

func (h recordHeap) Less(i, j int) bool {
	a := h.items[i].rec
	b := h.items[j].rec
	switch h.key {
	case SortByID:
		return a.ID < b.ID
	case SortByName:
		if a.Name == b.Name {
			return a.ID < b.ID
		}
		return a.Name < b.Name
	case SortByContinent:
		if a.Continent == b.Continent {
			return a.ID < b.ID
		}
		return a.Continent < b.Continent
	default:
		return false
	}
}

func (h recordHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *recordHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*heapItem))
}

func (h *recordHeap) Pop() interface{} {
	n := len(h.items)
	it := h.items[n-1]
	h.items = h.items[:n-1]
	return it
}
