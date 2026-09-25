//go:build !moriold

package main

import (
	"log/slog"

	"github.com/0xCarbon/mori"
)

func quiet(c *mori.Config) { c.Logger = slog.New(slog.DiscardHandler) }
