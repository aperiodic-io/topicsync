// Command topicsync keeps a Kafka cluster's topics as a desired-state file says. See the README.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/aperiodic-io/topicsync/internal/cli"
)

func main() {
	// SIGTERM (a Job's deadline, a drain) ends the run cleanly: actions in flight finish, the rest fail, and the
	// cluster is never left with a half-made topic (each Kafka request is atomic per topic).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], cli.DefaultEnv())
	stop()
	os.Exit(code)
}
