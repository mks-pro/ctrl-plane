// Package migrations embeds the SQL migration files so the one-shot runner (and
// the integration tests) can apply them without relying on the working
// directory or on a migration framework.
package migrations

import (
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

// Migration is one versioned up/down pair.
type Migration struct {
	Version string // "0001"
	Name    string // "init"
	Up      string
	Down    string
}

// All returns migrations sorted by version. Every up file must have a down file.
func All() ([]Migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	byVersion := map[string]*Migration{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		// <version>_<name>.<up|down>.sql — e.g. 0001_init.up.sql
		parts := strings.Split(name, ".")
		if len(parts) != 3 {
			return nil, fmt.Errorf("migration %q: expected <version>_<name>.<up|down>.sql", name)
		}
		version, mname, ok := strings.Cut(parts[0], "_")
		if !ok || len(version) != 4 || mname == "" {
			return nil, fmt.Errorf("migration %q: version must be four digits followed by _name", name)
		}
		body, err := files.ReadFile(name)
		if err != nil {
			return nil, err
		}
		m, ok := byVersion[version]
		if !ok {
			m = &Migration{Version: version, Name: mname}
			byVersion[version] = m
		}
		switch parts[1] {
		case "up":
			m.Up = string(body)
		case "down":
			m.Down = string(body)
		default:
			return nil, fmt.Errorf("migration %q: unknown direction %q", name, parts[1])
		}
	}
	out := make([]Migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.Up == "" || m.Down == "" {
			return nil, fmt.Errorf("migration %s: both up and down files are required", m.Version)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}
