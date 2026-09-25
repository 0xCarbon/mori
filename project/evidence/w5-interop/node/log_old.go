//go:build moriold

package main

import (
	"io"

	"github.com/0xCarbon/mori"
)

func quiet(c *mori.Config) { c.LogOutput = io.Discard }
