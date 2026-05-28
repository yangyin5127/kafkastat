package ui

import (
	"fmt"
	"time"

	"kafkastat/internal/kafka"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ── Messages ─────────────────────────────────────────────────────────────────

type connectMsg struct {
	client *kafka.Client
	err    error
}

type dataMsg struct {
	info *kafka.ClusterInfo
	err  error
}

type tickMsg time.Time

// ── Model ─────────────────────────────────────────────────────────────────────

// Model is the root bubbletea model.
type Model struct {
	// config
	broker     string
	refreshSec int

	// state
	client      *kafka.Client
	clusterInfo *kafka.ClusterInfo
	err         error
	connecting  bool

	// navigation
	selectedTopic int
	topicOffset   int // scroll offset for topics list

	// resizable groups panel
	groupsInner   int  // inner height of the groups border box
	groupsSized   bool // whether groupsInner has been auto-scaled to terminal size

	// terminal size
	width  int
	height int

	spinner spinner.Model
}

// NewModel creates a model ready to be passed to tea.NewProgram.
func NewModel(broker string, refreshSec int) Model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	return Model{
		broker:      broker,
		refreshSec:  refreshSec,
		connecting:  true,
		groupsInner: 5,
		spinner:     s,
	}
}

// ── bubbletea interface ───────────────────────────────────────────────────────

func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		connectCmd(m.broker),
	)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		// On first resize, set groupsInner to ~35% of usable vertical space.
		if !m.groupsSized {
			avail := m.height - 8 // see budget equation in View()
			if avail > 0 {
				m.groupsInner = avail * 35 / 100
				if m.groupsInner < 3 {
					m.groupsInner = 3
				}
			}
			m.groupsSized = true
		}
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case connectMsg:
		m.connecting = false
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.client = msg.client
		return m, tea.Batch(
			fetchCmd(m.client),
			tickCmd(m.refreshSec),
		)

	case dataMsg:
		if msg.err != nil {
			m.err = msg.err
		} else {
			m.err = nil
			m.clusterInfo = msg.info
			// Clamp selection
			if m.clusterInfo != nil && m.selectedTopic >= len(m.clusterInfo.Topics) {
				m.selectedTopic = max(0, len(m.clusterInfo.Topics)-1)
			}
		}
		return m, nil

	case tickMsg:
		if m.client != nil {
			return m, tea.Batch(
				fetchCmd(m.client),
				tickCmd(m.refreshSec),
			)
		}
		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		if m.client != nil {
			m.client.Close()
		}
		return m, tea.Quit

	case "up", "k":
		if m.selectedTopic > 0 {
			m.selectedTopic--
			if m.selectedTopic < m.topicOffset {
				m.topicOffset = m.selectedTopic
			}
		}

	case "down", "j":
		if m.clusterInfo != nil && m.selectedTopic < len(m.clusterInfo.Topics)-1 {
			m.selectedTopic++
		}

	case "r":
		if m.client != nil {
			return m, fetchCmd(m.client)
		}

	case "+", "=", "]":
		m.groupsInner = clamp(m.groupsInner+1, 3, m.height-12)

	case "-", "[":
		m.groupsInner = clamp(m.groupsInner-1, 3, m.height-12)
	}
	return m, nil
}

// ── View ─────────────────────────────────────────────────────────────────────

func (m Model) View() string {
	if m.width == 0 {
		return ""
	}

	header := m.renderHeader()

	// Loading / error screens
	if m.connecting {
		body := fmt.Sprintf("\n  %s Connecting to %s…\n", m.spinner.View(), m.broker)
		return lipgloss.JoinVertical(lipgloss.Left, header, body)
	}
	if m.err != nil && m.clusterInfo == nil {
		errTxt := errStyle.Render(fmt.Sprintf(
			"\n  ✗  %v\n\n  Press r to retry or q to quit.", m.err,
		))
		return lipgloss.JoinVertical(lipgloss.Left, header, errTxt)
	}

	// lipgloss .Height(n) sets INNER height; outer = n+2 (top+bottom border).
	// Budget: 3 + (middleH+2) + (groupsInner+2) + 1 = m.height
	//         → middleH = m.height - m.groupsInner - 8
	middleH := m.height - m.groupsInner - 8
	if middleH < 4 {
		middleH = 4
	}

	const topicsW = 28
	// Left outer = topicsW+2, right outer = partitionsW+2; together must = m.width
	partitionsW := m.width - (topicsW + 2) - 2 // inner width of right panel
	if partitionsW < 10 {
		partitionsW = 10
	}

	topicsPanel := m.renderTopicsPanel(topicsW, middleH)
	partitionsPanel := m.renderPartitionsPanel(partitionsW, middleH)

	middle := lipgloss.JoinHorizontal(lipgloss.Top, topicsPanel, partitionsPanel)
	groups := m.renderGroupsPanel(m.width-2, m.groupsInner, false)
	footer := m.renderFooter()

	return lipgloss.JoinVertical(lipgloss.Left, header, middle, groups, footer)
}

// ── Commands ──────────────────────────────────────────────────────────────────

func connectCmd(broker string) tea.Cmd {
	return func() tea.Msg {
		client, err := kafka.NewClient([]string{broker})
		return connectMsg{client: client, err: err}
	}
}

func fetchCmd(client *kafka.Client) tea.Cmd {
	return func() tea.Msg {
		info, err := client.FetchClusterInfo()
		return dataMsg{info: info, err: err}
	}
}

func tickCmd(secs int) tea.Cmd {
	return tea.Tick(time.Duration(secs)*time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
