package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/your-org/ctrlplane/internal/domain"
)

func TestValidateSlug(t *testing.T) {
	valid := []string{"abc", "acme-prod", "a1b", "a" + strings.Repeat("b", 26) + "c"}
	for _, in := range valid {
		got, err := domain.ValidateSlug("  " + in + "  ")
		if err != nil {
			t.Fatalf("ValidateSlug(%q) unexpected error: %v", in, err)
		}
		if got != in {
			t.Fatalf("ValidateSlug(%q) = %q, want trimmed input", in, got)
		}
	}

	invalid := map[string]string{
		"":                                  "empty",
		"ab":                                "too short",
		"a" + strings.Repeat("b", 27) + "c": "too long",
		"-abc":                              "leading hyphen",
		"abc-":                              "trailing hyphen",
		"Abc":                               "uppercase",
		"a_b":                               "underscore",
		"1abc":                              "leading digit",
		"abc def":                           "space",
		"caf\u00e9":                         "non-ascii",
	}
	for in, why := range invalid {
		if _, err := domain.ValidateSlug(in); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("ValidateSlug(%q) (%s) error = %v, want ErrInvalid", in, why, err)
		}
	}
}

func TestValidateName(t *testing.T) {
	got, err := domain.ValidateName("  Acme Production  ")
	if err != nil || got != "Acme Production" {
		t.Fatalf("ValidateName trim = %q, %v", got, err)
	}
	if _, err := domain.ValidateName("   "); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("blank name: got %v, want ErrInvalid", err)
	}
	if _, err := domain.ValidateName(strings.Repeat("a", 64)); err != nil {
		t.Fatalf("64 chars should be valid: %v", err)
	}
	if _, err := domain.ValidateName(strings.Repeat("a", 65)); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("65 chars: got %v, want ErrInvalid", err)
	}
	multi := strings.Repeat("\u00e9", 64)
	if utf8.RuneCountInString(multi) != 64 {
		t.Fatal("test setup: rune count")
	}
	if _, err := domain.ValidateName(multi); err != nil {
		t.Fatalf("64 multi-byte runes should be valid: %v", err)
	}
	if _, err := domain.ValidateName("bad\x00name"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("control char: got %v, want ErrInvalid", err)
	}
}

func TestValidateVersionAndLimit(t *testing.T) {
	if err := domain.ValidateVersion(0); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("version 0: %v", err)
	}
	if err := domain.ValidateVersion(1); err != nil {
		t.Fatalf("version 1: %v", err)
	}

	cases := []struct {
		in   string
		want int
		err  bool
	}{
		{"", 20, false},
		{"1", 1, false},
		{"100", 100, false},
		{"101", 0, true},
		{"0", 0, true},
		{"-1", 0, true},
		{"abc", 0, true},
		{"1.5", 0, true},
	}
	for _, c := range cases {
		got, err := domain.ParseLimit(c.in)
		if c.err {
			if !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("ParseLimit(%q) error = %v, want ErrInvalid", c.in, err)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Fatalf("ParseLimit(%q) = %d, %v; want %d", c.in, got, err, c.want)
		}
	}
}

func TestFormatTime(t *testing.T) {
	ts := time.Date(2026, 10, 24, 12, 0, 0, 123456789, time.UTC)
	if got, want := domain.FormatTime(ts), "2026-10-24T12:00:00.123Z"; got != want {
		t.Fatalf("FormatTime = %q, want %q", got, want)
	}
	offset := time.FixedZone("CEST", 2*60*60)
	local := time.Date(2026, 10, 24, 12, 0, 0, 123456789, offset)
	if got, want := domain.FormatTime(local), "2026-10-24T10:00:00.123Z"; got != want {
		t.Fatalf("FormatTime with offset = %q, want %q", got, want)
	}
}
