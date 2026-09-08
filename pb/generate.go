// Package durablepb contains the protobuf-generated wire types for journal.log entries.
//
// To regenerate journal.pb.go after editing journal.proto:
//
//	go generate ./pb/
//
// Prerequisites (one-time setup):
//
//	brew install protobuf
//	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//
// Commit both journal.proto and journal.pb.go in the same commit.
// Never edit journal.pb.go by hand.
package durablepb

//go:generate protoc --go_out=. --go_opt=paths=source_relative journal.proto
