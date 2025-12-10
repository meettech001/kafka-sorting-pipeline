package data

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Record represents one CSV row.
type Record struct {
	ID        int32
	Name      string
	Address   string
	Continent string
}

var continents = []string{
	"North America",
	"Asia",
	"South America",
	"Europe",
	"Africa",
	"Australia",
}

// ToCSV encodes the record into a single CSV line without trailing newline.
func (r Record) ToCSV() string {
	// We guarantee that name and address never contain commas, so a simple join is enough.
	return fmt.Sprintf("%d,%s,%s,%s", r.ID, r.Name, r.Address, r.Continent)
}

// ParseCSV parses a CSV line into a Record.
func ParseCSV(line string) (Record, error) {
	parts := strings.Split(strings.TrimSpace(line), ",")
	if len(parts) != 4 {
		return Record{}, fmt.Errorf("invalid field count: %d", len(parts))
	}
	id, err := parseInt32(parts[0])
	if err != nil {
		return Record{}, fmt.Errorf("parse id: %w", err)
	}
	return Record{
		ID:        id,
		Name:      parts[1],
		Address:   parts[2],
		Continent: parts[3],
	}, nil
}

func parseInt32(s string) (int32, error) {
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

// Generator creates random records following the requirements.
type Generator struct {
	rnd *rand.Rand
}

func NewGenerator() *Generator {
	src := rand.NewSource(time.Now().UnixNano())
	return &Generator{rnd: rand.New(src)}
}

// RandomRecord returns a random Record that matches the schema constraints.
func (g *Generator) RandomRecord() Record {
	return Record{
		ID:        g.randomID(),
		Name:      g.randomAlphaString(10, 15),
		Address:   g.randomAddress(15, 20),
		Continent: g.randomContinent(),
	}
}

func (g *Generator) randomID() int32 {
	return int32(g.rnd.Int31())
}

func (g *Generator) randomAlphaString(minLen, maxLen int) string {
	n := minLen + g.rnd.Intn(maxLen-minLen+1)
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		// a-z only
		b[i] = byte('a' + g.rnd.Intn(26))
	}
	return string(b)
}

func (g *Generator) randomAddress(minLen, maxLen int) string {
	n := minLen + g.rnd.Intn(maxLen-minLen+1)
	chars := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = chars[g.rnd.Intn(len(chars))]
		// Ensure we never pick comma to keep CSV simple
		if b[i] == ',' {
			b[i] = ' '
		}
	}
	return string(b)
}

func (g *Generator) randomContinent() string {
	return continents[g.rnd.Intn(len(continents))]
}
