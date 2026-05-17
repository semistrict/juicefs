package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

func cmdAccessProfile() *cli.Command {
	return &cli.Command{
		Name:     "access-profile",
		Category: "TOOL",
		Usage:    "Control per-file access profile recording and loading",
		Description: `
Access profiles record first-touch read/write block access order for a file
and store those runs in a separate profile file. A later load operation reads
that profile file and warms the target file's blocks.

This command talks to the mounted JuiceFS client through its internal .control
RPC. The control state is per mount process; the profile file is the persistent
artifact.

PROFILE names the profile file to record into or load from. If PROFILE is
relative, JuiceFS resolves it relative to FILE's parent directory. The same
FILE may have record and load active at the same time, and both operations may
use the same PROFILE; in that case JuiceFS loads stable previous runs while
recording a new run.

Examples:
  # Start recording FILE accesses into a profile next to FILE.
  juicefs access-profile record /mnt/jfs/snapshot.mem snapshot.mem.profile

  # Stop recording and commit the run.
  juicefs access-profile stop-record /mnt/jfs/snapshot.mem

  # Warm FILE from an existing profile.
  juicefs access-profile load /mnt/jfs/snapshot.mem snapshot.mem.profile

  # Cancel warmup.
  juicefs access-profile stop-load /mnt/jfs/snapshot.mem

  # Show current access profile state.
  juicefs access-profile status /mnt/jfs/snapshot.mem`,
		Subcommands: []*cli.Command{
			{
				Name:      "record",
				Usage:     "Start recording an access profile for a file",
				ArgsUsage: "FILE PROFILE",
				Description: `
Start recording successful first-touch read/write block accesses for FILE.
When recording is stopped, JuiceFS appends one run to PROFILE and keeps only
the default number of retained runs. Relative PROFILE values are resolved
relative to FILE's parent directory.`,
				Action: func(c *cli.Context) error {
					if err := requireArgs(c, 2, 2); err != nil {
						return err
					}
					return runAccessProfile(c.Args().Get(0), vfs.AccessProfileRequest{Op: vfs.AccessProfileRecordStart, Profile: c.Args().Get(1)})
				},
			},
			{
				Name:      "stop-record",
				Usage:     "Stop recording and commit the current access profile run",
				ArgsUsage: "FILE",
				Description: `
Stop the active recorder for FILE. If a recorder is active, JuiceFS flushes
the buffered run to the profile file selected by the matching record command.`,
				Action: func(c *cli.Context) error {
					if err := requireArgs(c, 1, 1); err != nil {
						return err
					}
					return runAccessProfile(c.Args().Get(0), vfs.AccessProfileRequest{Op: vfs.AccessProfileRecordStop})
				},
			},
			{
				Name:      "load",
				Usage:     "Start loading an access profile to warm a file",
				ArgsUsage: "FILE PROFILE",
				Description: `
Start warming FILE from PROFILE. Loading uses the recorded runs breadth-first
and skips duplicate blocks during a single load. Relative PROFILE values are
resolved relative to FILE's parent directory.`,
				Action: func(c *cli.Context) error {
					if err := requireArgs(c, 2, 2); err != nil {
						return err
					}
					return runAccessProfile(c.Args().Get(0), vfs.AccessProfileRequest{Op: vfs.AccessProfileLoadStart, Profile: c.Args().Get(1)})
				},
			},
			{
				Name:      "stop-load",
				Usage:     "Cancel an in-flight access profile warmup",
				ArgsUsage: "FILE",
				Description: `
Cancel any active profile warmup for FILE. Blocks already warmed remain in the
JuiceFS cache.`,
				Action: func(c *cli.Context) error {
					if err := requireArgs(c, 1, 1); err != nil {
						return err
					}
					return runAccessProfile(c.Args().Get(0), vfs.AccessProfileRequest{Op: vfs.AccessProfileLoadStop})
				},
			},
			{
				Name:      string(vfs.AccessProfileStatusOp),
				Usage:     "Show access profile state for a file",
				ArgsUsage: "FILE",
				Description: `
Return JSON describing whether FILE is currently recording or loading in this
mount process.`,
				Action: func(c *cli.Context) error {
					if err := requireArgs(c, 1, 1); err != nil {
						return err
					}
					return runAccessProfile(c.Args().Get(0), vfs.AccessProfileRequest{Op: vfs.AccessProfileStatusOp})
				},
			},
		},
	}
}

func runAccessProfile(path string, req vfs.AccessProfileRequest) error {
	resp, err := accessProfileRPC(path, req)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(data))
	if !resp.OK {
		return errors.New(string(resp.Status))
	}
	return nil
}

func accessProfileRPC(path string, req vfs.AccessProfileRequest) (*vfs.AccessProfileResponse, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	inode, err := utils.GetFileInode(absPath)
	if err != nil {
		return nil, fmt.Errorf("lookup inode for %q: %w", path, err)
	}
	if inode < uint64(meta.RootInode) {
		return nil, fmt.Errorf("inode number shouldn't be less than %d", meta.RootInode)
	}
	req.Inode = meta.Ino(inode)
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	cf, err := openController(absPath)
	if err != nil {
		return nil, fmt.Errorf("open controller: %w", err)
	}
	defer cf.Close()

	wb := utils.NewBuffer(uint32(8 + len(data)))
	wb.Put32(meta.AccessProfile)
	wb.Put32(uint32(len(data)))
	wb.Put(data)
	if _, err = cf.Write(wb.Bytes()); err != nil {
		return nil, fmt.Errorf("write message: %w", err)
	}
	respData, errno := readProgress(cf, nil)
	if errno == syscall.EINVAL {
		return nil, fmt.Errorf("access-profile is not supported, please upgrade and mount again")
	}
	if errno != 0 {
		return nil, fmt.Errorf("access-profile: %s", errno)
	}
	var resp vfs.AccessProfileResponse
	if err = json.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func requireArgs(c *cli.Context, min, max int) error {
	if c.NArg() < min {
		return fmt.Errorf("%s requires at least %d argument(s)", c.Command.FullName(), min)
	}
	if max > 0 && c.NArg() > max {
		return fmt.Errorf("%s accepts at most %d argument(s)", c.Command.FullName(), max)
	}
	return nil
}
