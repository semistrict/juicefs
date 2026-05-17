package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestAccessProfileCommandHelpDocumentsRPCAndSubcommands(t *testing.T) {
	var out bytes.Buffer
	app := &cli.App{
		Name:     "juicefs",
		Writer:   &out,
		Commands: []*cli.Command{cmdAccessProfile()},
	}
	if err := app.Run([]string{"juicefs", "access-profile", "--help"}); err != nil {
		t.Fatal(err)
	}
	help := out.String()
	for _, want := range []string{
		"internal .control",
		"record",
		"stop-record",
		"load",
		"stop-load",
		"status",
		"If PROFILE is",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("help does not contain %q:\n%s", want, help)
		}
	}
}
