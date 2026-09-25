// Copyright (c) 0xCarbon
// SPDX-License-Identifier: MPL-2.0

//go:build !race

package mori

// raceDetector reports whether the test binary runs under the race
// detector, for tests that are costly there and exercise no concurrency.
const raceDetector = false
