package main

import (
	"flag"
	"fmt"
	"os"

	"kafkastat/internal/ui"

	tea "github.com/charmbracelet/bubbletea"
)

var (
	Version   = "0.1.0"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "kafkastat version: %s (commit: %s, built: %s)\n\nUsage:\n", Version, GitCommit, BuildTime)
		flag.PrintDefaults()
	}
	showVersion := flag.Bool("v", false, "Print version and exit")
	broker := flag.String("broker", "localhost:9092", "Kafka broker address (host:port)")
	refresh := flag.Int("refresh", 5, "Refresh interval in seconds")

	flag.Parse()

	if *showVersion {
		fmt.Printf("kafkastat version: %s\ncommit:     %s\nbuilt:      %s\n", Version, GitCommit, BuildTime)
		os.Exit(0)
	}

	m := ui.NewModel(*broker, *refresh)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
