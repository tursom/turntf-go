package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tursom/turntf-go/demo"
)

func main() {
	var file string
	var timeout time.Duration

	flag.StringVar(&file, "f", "", "path to scenario yaml")
	flag.DurationVar(&timeout, "timeout", 0, "optional overall scenario timeout")
	flag.Parse()

	if file == "" {
		fmt.Fprintln(os.Stderr, "missing required -f <scenario.yaml>")
		os.Exit(2)
	}

	scenario, err := demo.LoadFile(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load scenario: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if err := demo.RunScenario(ctx, scenario, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "run scenario: %v\n", err)
		os.Exit(1)
	}
}
