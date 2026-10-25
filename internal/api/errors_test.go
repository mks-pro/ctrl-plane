package api_test

import "errors"

var errDial = errors.New("dial tcp 127.0.0.1:5432: connect: connection refused")
