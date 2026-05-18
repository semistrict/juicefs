package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

func cmdSharedExtentSources() *cli.Command {
	return &cli.Command{
		Name:      "shared-extent-sources",
		Action:    sharedExtentSources,
		Category:  "INSPECTOR",
		Usage:     "Locate source files for shared extent spans",
		ArgsUsage: "PATH/INODE [PATH/INODE...]",
		Description: `
Given an ordered clone chain, locate the earliest file that shares each extent
span with the last file. The arguments must be ordered from oldest/most-general
to newest/most-specific:

  FILE_0 FILE_1 ... FILE_N

The expected relationship is that FILE_1 was cloned from FILE_0, FILE_2 was
cloned from FILE_1, and so on, with a few local modifications in later files.
FILE_N is the target file whose byte range is being queried. Earlier files are
candidate sources for shared extents.

Results are printed as formatted JSON. In each span, sourceIndex refers to the
argument position. sourceIndex=-1 means the target has a hole for that span.
sourceIndex=N means the target has unique local data for that span. Any smaller
non-negative sourceIndex means the target span is shared with that earlier file.

Examples:
  juicefs shared-extent-sources --offset 0 --length 32768 base clone target
  juicefs shared-extent-sources --inode --offset 0 --length 32768 1001 1002 1003`,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "inode",
				Aliases: []string{"i"},
				Usage:   "treat arguments as inode numbers; current directory must be inside JuiceFS",
			},
			&cli.Uint64Flag{
				Name:  "offset",
				Usage: "range start offset in bytes",
			},
			&cli.Uint64Flag{
				Name:     "length",
				Usage:    "range length in bytes",
				Required: true,
			},
		},
	}
}

func sharedExtentSources(ctx *cli.Context) error {
	setup0(ctx, 1, 0)
	if ctx.Args().Len() < 2 {
		return fmt.Errorf("at least two files are required")
	}
	length := ctx.Uint64("length")
	if length == 0 {
		return fmt.Errorf("--length must be greater than 0")
	}
	var controllerDir string
	files := make([]vfs.Ino, ctx.Args().Len())
	for i := 0; i < ctx.Args().Len(); i++ {
		arg := ctx.Args().Get(i)
		if ctx.Bool("inode") {
			ino, err := strconv.ParseUint(arg, 10, 64)
			if err != nil {
				return fmt.Errorf("parse inode %q: %w", arg, err)
			}
			files[i] = vfs.Ino(ino)
			continue
		}
		abs, err := filepath.Abs(arg)
		if err != nil {
			return fmt.Errorf("abs of %q: %w", arg, err)
		}
		ino, err := utils.GetFileInode(abs)
		if err != nil {
			return fmt.Errorf("lookup inode for %q: %w", arg, err)
		}
		files[i] = vfs.Ino(ino)
		if i == 0 {
			controllerDir = abs
		}
	}
	if controllerDir == "" {
		var err error
		controllerDir, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	controller, err := openController(controllerDir)
	if err != nil {
		return fmt.Errorf("open control file for %q: %w", controllerDir, err)
	}
	defer controller.Close()

	payload := encodeSharedExtentSourcesControlRequest(files, []vfs.SharedExtentSourceRange{{
		Off: ctx.Uint64("offset"),
		Len: length,
	}})
	msg := utils.NewBuffer(uint32(8 + len(payload)))
	msg.Put32(meta.SharedExtentSources)
	msg.Put32(uint32(len(payload)))
	msg.Put(payload)
	if _, err = controller.Write(msg.Bytes()); err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	data, errno := readProgress(controller, func(uint64, uint64) {})
	if errno != 0 {
		return errno
	}
	var resp vfs.SharedExtentSourcesResponse
	if err = json.Unmarshal(data, &resp); err != nil {
		return err
	}
	formatted, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(formatted))
	return nil
}

func encodeSharedExtentSourcesControlRequest(files []vfs.Ino, ranges []vfs.SharedExtentSourceRange) []byte {
	w := utils.NewBuffer(uint32(4 + len(files)*8 + 4 + len(ranges)*16))
	w.Put32(uint32(len(files)))
	for _, ino := range files {
		w.Put64(uint64(ino))
	}
	w.Put32(uint32(len(ranges)))
	for _, rg := range ranges {
		w.Put64(rg.Off)
		w.Put64(rg.Len)
	}
	return w.Bytes()
}
