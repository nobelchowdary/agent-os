package api

import _ "embed"

// openAPISpec is the checked-in wire contract. Contract tests derive route,
// authorization, request-validation and error-shape checks from this document.
//
//go:embed openapi.json
var openAPISpec []byte
