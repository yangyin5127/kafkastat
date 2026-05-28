package main

import (
	"flag"
	"fmt"
	"os"

	"kafkastat/internal/ui"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	broker := flag.String("broker", "localhost:9092", "Kafka broker address (host:port)")
	refresh := flag.Int("refresh", 5, "Refresh interval in seconds")
	flag.Parse()

	m := ui.NewModel(*broker, *refresh)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
