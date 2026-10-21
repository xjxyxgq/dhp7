package main

import (
	"math/rand"
	"reflect"
	"testing"
)

const testUUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

func TestGTIDSetDifference(t *testing.T) {
	wanted, err := parseGTIDSet(testUUID + ":1-3:5-9:12," + testUUID + ":10-11")
	if err != nil {
		t.Fatal(err)
	}
	current, err := parseGTIDSet(testUUID + ":2:6-7:10:13")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := wanted.subtract(current).String(), testUUID+":1:3:5:8-9:11-12"; got != want {
		t.Fatalf("missing = %q, want %q", got, want)
	}
	if got, want := current.subtract(wanted).String(), testUUID+":13"; got != want {
		t.Fatalf("extra = %q, want %q", got, want)
	}
	if got, want := wanted.String(), testUUID+":1-3:5-12"; got != want {
		t.Fatalf("normalized = %q, want %q", got, want)
	}
}

func TestGTIDSetDifferenceAgainstIndividualSequences(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	for trial := 0; trial < 200; trial++ {
		wanted, current := make(gtidSet), make(gtidSet)
		wantMissing := make(map[uint64]bool)
		for sequence := uint64(1); sequence <= 30; sequence++ {
			inWanted, inCurrent := random.Intn(2) == 0, random.Intn(2) == 0
			if inWanted {
				wanted[testUUID] = append(wanted[testUUID], interval{sequence, sequence})
			}
			if inCurrent {
				current[testUUID] = append(current[testUUID], interval{sequence, sequence})
			}
			if inWanted && !inCurrent {
				wantMissing[sequence] = true
			}
		}
		wanted.normalize()
		current.normalize()
		gotMissing := make(map[uint64]bool)
		for _, part := range wanted.subtract(current)[testUUID] {
			for sequence := part.first; sequence <= part.last; sequence++ {
				gotMissing[sequence] = true
			}
		}
		if !reflect.DeepEqual(gotMissing, wantMissing) {
			t.Fatalf("trial %d: got %v, want %v", trial, gotMissing, wantMissing)
		}
	}
}

func TestInvalidGTIDSets(t *testing.T) {
	for _, raw := range []string{
		"not-a-uuid:1", testUUID + ":0", testUUID + ":3-2",
		testUUID + ":1--2", testUUID + ":9223372036854775808", testUUID + ":1,",
	} {
		if _, err := parseGTIDSet(raw); err == nil {
			t.Errorf("accepted invalid GTID set %q", raw)
		}
	}
}

func TestAddressKey(t *testing.T) {
	for raw, want := range map[string]string{
		"192.168.1.10:3306": "192.168.1.10_3306",
		"[::1]:3306":        "::1_3306",
	} {
		addr, err := parseAddress(raw)
		if err != nil {
			t.Fatal(err)
		}
		if addr.key() != want {
			t.Errorf("%s key = %q, want %q", raw, addr.key(), want)
		}
	}
}
