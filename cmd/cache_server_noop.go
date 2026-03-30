//go:build windows
// +build windows

package cmd

import "github.com/urfave/cli/v2"

func cmdCacheServer() *cli.Command {
	return &cli.Command{
		Name:   "cache-server",
		Hidden: true,
		Usage:  "Not supported on Windows",
		Action: func(c *cli.Context) error {
			return nil
		},
	}
}
