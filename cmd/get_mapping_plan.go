package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/pkg/xattr"
	"github.com/urfave/cli/v2"
)

type mappingPlanResponse struct {
	Target  string              `json:"target"`
	Offset  uint64              `json:"offset"`
	Len     uint64              `json:"len"`
	Sources []mappingPlanSource `json:"sources"`
	Spans   []mappingPlanSpan   `json:"spans"`
}

type mappingPlanSource struct {
	Ino  uint64 `json:"ino"`
	Path string `json:"path"`
}

type mappingPlanSpan struct {
	Off     uint64             `json:"off"`
	Len     uint64             `json:"len"`
	Backing mappingPlanBacking `json:"backing"`
}

type mappingPlanBacking struct {
	Kind        string `json:"kind"`
	Path        string `json:"path,omitempty"`
	FileOffset  uint64 `json:"file_offset"`
	SourceIndex int    `json:"source_index"`
	SourceIno   uint64 `json:"source_ino"`
}

func cmdGetMappingPlan() *cli.Command {
	return &cli.Command{
		Name:      "get-mapping-plan",
		Action:    getMappingPlan,
		Category:  "INSPECTOR",
		Usage:     "Build a patchwork mmap plan for a cloned file",
		ArgsUsage: "PATH",
		Flags: []cli.Flag{
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

func getMappingPlan(ctx *cli.Context) error {
	setup0(ctx, 1, 1)
	length := ctx.Uint64("length")
	if length == 0 {
		return fmt.Errorf("--length must be greater than 0")
	}
	target, err := filepath.Abs(ctx.Args().First())
	if err != nil {
		return fmt.Errorf("abs target: %w", err)
	}
	targetIno, err := utils.GetFileInode(target)
	if err != nil {
		return fmt.Errorf("lookup inode for %q: %w", target, err)
	}
	mountpoint, err := findJuiceFSMountpoint(target)
	if err != nil {
		return err
	}
	chain, err := cloneChainForMappingPlan(mountpoint, target, vfs.Ino(targetIno))
	if err != nil {
		return err
	}

	resp, err := sharedExtentSourcesForMappingPlan(target, chain, ctx.Uint64("offset"), length)
	if err != nil {
		return err
	}
	targetIndex := len(chain) - 1
	plan := mappingPlanResponse{
		Target:  target,
		Offset:  ctx.Uint64("offset"),
		Len:     length,
		Sources: make([]mappingPlanSource, len(chain)),
	}
	for i, source := range chain {
		plan.Sources[i] = mappingPlanSource{
			Ino:  uint64(source.ino),
			Path: source.path,
		}
	}
	for _, span := range resp.Spans {
		out := mappingPlanSpan{Off: span.Off, Len: span.Len}
		if span.SourceIndex < 0 {
			out.Backing = mappingPlanBacking{Kind: "zero"}
		} else {
			if span.SourceIndex >= len(chain) {
				return fmt.Errorf("source index %d outside %d sources", span.SourceIndex, len(chain))
			}
			fileOffset := span.SourceOff
			if span.SourceIndex == targetIndex && fileOffset == 0 {
				fileOffset = span.Off
			}
			out.Backing = mappingPlanBacking{
				Kind:        "file",
				Path:        chain[span.SourceIndex].path,
				FileOffset:  fileOffset,
				SourceIndex: span.SourceIndex,
				SourceIno:   uint64(span.SourceIno),
			}
		}
		plan.Spans = append(plan.Spans, out)
	}
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

type mappingPlanChainEntry struct {
	ino  vfs.Ino
	path string
}

func cloneChainForMappingPlan(mountpoint, target string, targetIno vfs.Ino) ([]mappingPlanChainEntry, error) {
	var reverse []mappingPlanChainEntry
	seen := make(map[vfs.Ino]bool)
	path := target
	ino := targetIno
	for {
		if seen[ino] {
			return nil, fmt.Errorf("cycle in clone source chain at inode %d", ino)
		}
		seen[ino] = true
		reverse = append(reverse, mappingPlanChainEntry{ino: ino, path: path})
		value, err := xattr.Get(path, meta.CloneSourceXattr)
		if err != nil {
			if errors.Is(err, xattr.ENOATTR) {
				break
			}
			return nil, fmt.Errorf("get %s on %q: %w", meta.CloneSourceXattr, path, err)
		}
		srcIno, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s on %q: %w", meta.CloneSourceXattr, path, err)
		}
		path, err = resolveMappingPlanInodePath(mountpoint, vfs.Ino(srcIno))
		if err != nil {
			return nil, err
		}
		ino = vfs.Ino(srcIno)
	}
	chain := make([]mappingPlanChainEntry, len(reverse))
	for i := range reverse {
		chain[i] = reverse[len(reverse)-1-i]
	}
	return chain, nil
}

func resolveMappingPlanInodePath(mountpoint string, ino vfs.Ino) (string, error) {
	resp, err := infoForMappingPlan(mountpoint, ino)
	if err != nil {
		return "", err
	}
	if len(resp.Paths) == 0 {
		return "", fmt.Errorf("inode %d has no path", ino)
	}
	if resp.Paths[0] == "/" {
		return mountpoint, nil
	}
	return filepath.Join(mountpoint, resp.Paths[0]), nil
}

func infoForMappingPlan(controllerDir string, ino vfs.Ino) (*vfs.InfoResponse, error) {
	controller, err := openController(controllerDir)
	if err != nil {
		return nil, fmt.Errorf("open control file for %q: %w", controllerDir, err)
	}
	defer controller.Close()
	wb := utils.NewBuffer(8 + 11)
	wb.Put32(meta.InfoV2)
	wb.Put32(11)
	wb.Put64(uint64(ino))
	wb.Put8(0)
	wb.Put8(0)
	wb.Put8(0)
	if _, err = controller.Write(wb.Bytes()); err != nil {
		return nil, fmt.Errorf("write info request: %w", err)
	}
	data, errno := readProgress(controller, func(uint64, uint64) {})
	if errno != 0 {
		return nil, errno
	}
	var resp vfs.InfoResponse
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Failed {
		return nil, fmt.Errorf("%s", resp.Reason)
	}
	return &resp, nil
}

func sharedExtentSourcesForMappingPlan(controllerDir string, chain []mappingPlanChainEntry, off, length uint64) (*vfs.SharedExtentSourcesResponse, error) {
	files := make([]vfs.Ino, len(chain))
	for i, entry := range chain {
		files[i] = entry.ino
	}
	controller, err := openController(controllerDir)
	if err != nil {
		return nil, fmt.Errorf("open control file for %q: %w", controllerDir, err)
	}
	defer controller.Close()
	payload := encodeSharedExtentSourcesControlRequest(files, []vfs.SharedExtentSourceRange{{
		Off: off,
		Len: length,
	}})
	msg := utils.NewBuffer(uint32(8 + len(payload)))
	msg.Put32(meta.SharedExtentSources)
	msg.Put32(uint32(len(payload)))
	msg.Put(payload)
	if _, err = controller.Write(msg.Bytes()); err != nil {
		return nil, fmt.Errorf("write shared extent request: %w", err)
	}
	data, errno := readProgress(controller, func(uint64, uint64) {})
	if errno != 0 {
		return nil, errno
	}
	var resp vfs.SharedExtentSourcesResponse
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func findJuiceFSMountpoint(path string) (string, error) {
	cur := path
	if fi, err := os.Stat(cur); err == nil && !fi.IsDir() {
		cur = filepath.Dir(cur)
	}
	for {
		ino, err := utils.GetFileInode(cur)
		if err != nil {
			return "", fmt.Errorf("lookup inode for %q: %w", cur, err)
		}
		if ino == uint64(meta.RootInode) {
			return cur, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", fmt.Errorf("%q is not inside a mounted JuiceFS tree", path)
		}
		cur = parent
	}
}
