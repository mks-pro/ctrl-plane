package migrations_test

import (
	"strings"
	"testing"

	"github.com/your-org/ctrlplane/migrations"
)

func TestAllParsesEmbeddedMigrations(t *testing.T) {
	all, err := migrations.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("migrations = %d, want 1", len(all))
	}
	m := all[0]
	if m.Version != "0001" || m.Name != "init" {
		t.Fatalf("migration identity = %s_%s", m.Version, m.Name)
	}
	if !strings.Contains(m.Up, "CREATE TABLE tenants") || !strings.Contains(m.Down, "DROP TABLE") {
		t.Fatalf("unexpected migration bodies: up=%d bytes down=%d bytes", len(m.Up), len(m.Down))
	}
}
