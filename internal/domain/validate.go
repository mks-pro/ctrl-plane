package domain

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// slugPattern is Kubernetes-namespace compatible. It also enforces the 3–28
// character length: 1 + (1..26) + 1.
var slugPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

// ValidateSlug trims and validates a slug. It deliberately does not lowercase:
// silently accepting "Acme" would make the stored value differ from the input.
func ValidateSlug(raw string) (string, error) {
	slug := strings.TrimSpace(raw)
	if slug == "" {
		return "", Invalid("slug", "is required")
	}
	if !slugPattern.MatchString(slug) {
		return "", Invalid("slug", "must be 3-28 characters, start with a letter, end with a letter or digit, and contain only lowercase letters, digits and hyphens")
	}
	return slug, nil
}

// ValidateName trims and validates a tenant name: 1-64 characters, no control
// characters. Length is counted in runes, so non-ASCII names are not penalised.
func ValidateName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", Invalid("name", "is required")
	}
	if utf8.RuneCountInString(name) > 64 {
		return "", Invalid("name", "must be at most 64 characters")
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", Invalid("name", "must not contain control characters")
		}
	}
	return name, nil
}

// ValidateVersion validates the optimistic-locking token supplied by a client.
func ValidateVersion(v int) error {
	if v < 1 {
		return Invalid("version", "must be at least 1")
	}
	return nil
}

// DefaultLimit is the page size when the client does not ask for one.
const DefaultLimit = 20

// MaxLimit is the largest page the API will serve.
const MaxLimit = 100

// ParseLimit parses the `limit` query parameter. An empty value means default.
func ParseLimit(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return DefaultLimit, nil
	}
	var n int
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, Invalid("limit", "must be an integer between 1 and 100")
		}
		n = n*10 + int(r-'0')
		if n > MaxLimit {
			return 0, Invalid("limit", "must be an integer between 1 and 100")
		}
	}
	if n < 1 {
		return 0, Invalid("limit", "must be an integer between 1 and 100")
	}
	return n, nil
}
