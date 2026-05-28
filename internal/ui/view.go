package ui

import (
	"fmt"
	"strings"
	"time"

	"kafkastat/internal/kafka"

	"github.com/charmbracelet/lipgloss"
)

// ── Styles ────────────────────────────────────────────────────────────────────

var (
	// Colours
	colorGreen  = lipgloss.Color("82")
	colorRed    = lipgloss.Color("196")
	colorYellow = lipgloss.Color("214")
	colorBlue   = lipgloss.Color("39")
	colorPurple = lipgloss.Color("135")
	colorGray   = lipgloss.Color("244")
	colorWhite  = lipgloss.Color("255")
	colorDim    = lipgloss.Color("238")

	headerBorderColor = lipgloss.Color("57")
	panelBorderColor  = lipgloss.Color("238")

	// Shared styles
	errStyle = lipgloss.NewStyle().Foreground(colorRed)

	selectedItemStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("0")).
				Background(colorGreen).
				Bold(true)

	normalItemStyle = lipgloss.NewStyle().Foreground(colorWhite)

	dimStyle    = lipgloss.NewStyle().Foreground(colorGray)
	greenStyle  = lipgloss.NewStyle().Foreground(colorGreen)
	redStyle    = lipgloss.NewStyle().Foreground(colorRed)
	yellowStyle = lipgloss.NewStyle().Foreground(colorYellow)

	panelBorder = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(panelBorderColor)
)

// ── Header ────────────────────────────────────────────────────────────────────

func (m Model) renderHeader() string {
	var dot string
	if m.client != nil && m.err == nil {
		dot = greenStyle.Render("●")
	} else {
		dot = redStyle.Render("●")
	}

	title := fmt.Sprintf("%s Kafka Monitor  cluster: %s",
		dot,
		lipgloss.NewStyle().Foreground(colorBlue).Bold(true).Render(m.broker),
	)

	var right string
	if m.clusterInfo != nil {
		right = dimStyle.Render(fmt.Sprintf("brokers: %d  refreshed: %s  [r]efresh  [q]uit",
			m.clusterInfo.BrokerCount,
			m.clusterInfo.FetchedAt.Format(time.TimeOnly),
		))
	} else {
		right = dimStyle.Render("[q]uit")
	}

	inner := m.width - 4 // 2 border + 2 padding
	if inner < 10 {
		inner = 10
	}
	gap := inner - lipgloss.Width(title) - lipgloss.Width(right)
	if gap < 1 {
		gap = 1
	}
	content := title + strings.Repeat(" ", gap) + right

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(headerBorderColor).
		Width(m.width-2).
		Padding(0, 1).
		Render(content)
}

// ── Topics panel (left) ───────────────────────────────────────────────────────

func (m Model) renderTopicsPanel(width, height int) string {
	title := lipgloss.NewStyle().Bold(true).Foreground(colorPurple).Render("Topics")
	lines := []string{title, strings.Repeat("─", width-4)}

	innerH := height - 4 // title + border + divider
	if innerH < 1 {
		innerH = 1
	}

	var topics []kafka.TopicInfo
	if m.clusterInfo != nil {
		topics = m.clusterInfo.Topics
	}

	// Adjust scroll offset so selected is always visible
	if m.selectedTopic >= m.topicOffset+innerH {
		m.topicOffset = m.selectedTopic - innerH + 1
	}
	if m.topicOffset < 0 {
		m.topicOffset = 0
	}

	for i := m.topicOffset; i < len(topics) && i < m.topicOffset+innerH; i++ {
		t := topics[i]
		label := fmt.Sprintf("%-*s", width-6, truncate(t.Name, width-6))
		var prefix string
		if i == m.selectedTopic {
			prefix = "► "
			lines = append(lines, selectedItemStyle.Render(prefix+label))
		} else {
			prefix = "  "
			lines = append(lines, normalItemStyle.Render(prefix+label))
		}
	}

	// Pad remaining rows
	for len(lines)-2 < innerH {
		lines = append(lines, "")
	}

	content := strings.Join(lines, "\n")
	return panelBorder.Width(width).Height(height).Render(content)
}

// ── Partitions panel (right) ──────────────────────────────────────────────────

// cell returns a fixed-width, left-aligned cell that is ANSI-aware.
func cell(w int, s string) string {
	return lipgloss.NewStyle().Width(w).MaxWidth(w).Inline(true).Render(s)
}

func (m Model) renderPartitionsPanel(width, height int) string {
	var topicName string
	var partitions []kafka.PartitionInfo
	var consumerLag map[int32]int64
	var topicInfo kafka.TopicInfo

	if m.clusterInfo != nil && len(m.clusterInfo.Topics) > 0 {
		topicInfo = m.clusterInfo.Topics[m.selectedTopic]
		topicName = topicInfo.Name
		partitions = topicInfo.Partitions
		consumerLag = m.clusterInfo.ConsumerLag[topicName]
	}

	// Compute column widths from actual content (header width or max data width + gap).
	const colGap = 2
	cPart := len("Part") + colGap
	cNewest := len("Newest") + colGap
	cOldest := len("Oldest") + colGap
	cLag := len("Consumer lag") + colGap
	cLeader := len("Leader") + colGap
	cISR := len("ISR") + colGap
	cRate := len("Rate/s") + colGap
	for _, p := range partitions {
		if w := len(fmt.Sprintf("P%d", p.ID)) + colGap; w > cPart {
			cPart = w
		}
		if w := len(fmt.Sprintf("%d", p.Newest)) + colGap; w > cNewest {
			cNewest = w
		}
		if w := len(fmt.Sprintf("%d", p.Oldest)) + colGap; w > cOldest {
			cOldest = w
		}
		if w := len(fmt.Sprintf("%d", consumerLag[p.ID])) + colGap; w > cLag {
			cLag = w
		}
	}
	// Distribute remaining panel width as extra padding (capped at +8 per column).
	if totalMin := cPart + cNewest + cOldest + cLag + cLeader + cISR + cRate; totalMin < width-2 {
		extra := (width - 2 - totalMin) / 7
		if extra > 8 {
			extra = 8
		}
		cPart += extra
		cNewest += extra
		cOldest += extra
		cLag += extra
		cLeader += extra
		cISR += extra
		cRate += extra
	}

	// Title line: "Partitions  topic: xxx" left, topic metadata right.
	header := lipgloss.NewStyle().Bold(true).Foreground(colorPurple).Render("Partitions")
	sub := dimStyle.Render(fmt.Sprintf("topic: %s", topicName))
	titleLeft := header + "  " + sub
	var metaRight string
	if topicName != "" {
		metaRight = dimStyle.Render(fmt.Sprintf(
			"partitions:%d  replicas:%d  retention:%s",
			len(partitions), topicInfo.ReplicationFactor, fmtRetention(topicInfo.RetentionMs),
		))
	}
	gap := width - lipgloss.Width(titleLeft) - lipgloss.Width(metaRight) - 4
	if gap < 1 {
		gap = 1
	}
	title := titleLeft + strings.Repeat(" ", gap) + metaRight
	lines := []string{title, strings.Repeat("─", width-4)}

	colHeader := cell(cPart, "Part") +
		cell(cNewest, "Newest") +
		cell(cOldest, "Oldest") +
		cell(cLag, "Consumer lag") +
		cell(cLeader, "Leader") +
		cell(cISR, "ISR") +
		cell(cRate, "Rate/s")
	lines = append(lines, dimStyle.Render(colHeader))

	innerH := height - 5
	if innerH < 1 {
		innerH = 1
	}

	for i, p := range partitions {
		if i >= innerH {
			break
		}
		lag := consumerLag[p.ID]
		leaderStr := dimStyle.Render(fmt.Sprintf("%d", p.Leader))
		if p.Leader < 0 {
			leaderStr = dimStyle.Render("—")
		}
		row := cell(cPart, fmt.Sprintf("P%d", p.ID)) +
			cell(cNewest, fmt.Sprintf("%d", p.Newest)) +
			cell(cOldest, fmt.Sprintf("%d", p.Oldest)) +
			cell(cLag, lagColored(lag)) +
			cell(cLeader, leaderStr) +
			cell(cISR, isrColored(p.ISRCount, p.ReplicaCount)) +
			cell(cRate, fmtRate(p.Rate))
		lines = append(lines, row)
	}

	// Pad
	for len(lines)-2 < height-2 {
		lines = append(lines, "")
	}

	content := strings.Join(lines, "\n")
	return panelBorder.Width(width).Height(height).Render(content)
}

// ── Consumer Groups panel (bottom) ────────────────────────────────────────────

// renderGroupsPanel renders consumer groups that consume from the selected topic.
// active=true when the mouse hovers on or is dragging the top border (divider).
func (m Model) renderGroupsPanel(width, innerH int, active bool) string {
	var selectedTopic string
	if m.clusterInfo != nil && len(m.clusterInfo.Topics) > 0 {
		selectedTopic = m.clusterInfo.Topics[m.selectedTopic].Name
	}

	titleLabel := lipgloss.NewStyle().Bold(true).Foreground(colorPurple).Render("Consumer Groups")
	if selectedTopic != "" {
		titleLabel += dimStyle.Render(fmt.Sprintf("  (topic: %s)", selectedTopic))
	}
	// Drag-handle hint: brighter when hovered/dragging
	var resizeHint string
	if active {
		resizeHint = lipgloss.NewStyle().Foreground(colorYellow).Bold(true).Render("↕ drag")
	} else {
		resizeHint = dimStyle.Render("[ ] resize")
	}
	titleLine := titleLabel + strings.Repeat(" ", max(1, width-lipgloss.Width(titleLabel)-lipgloss.Width(resizeHint)-4)) + resizeHint
	lines := []string{titleLine}

	// Rows available after the title row
	available := innerH - 1
	if available < 1 {
		available = 1
	}

	// Filter groups to those consuming from the selected topic
	var relevant []kafka.ConsumerGroupInfo
	if m.clusterInfo != nil {
		for _, g := range m.clusterInfo.Groups {
			if _, ok := g.TopicLag[selectedTopic]; ok {
				relevant = append(relevant, g)
			}
		}
	}

	// Compute column widths from actual content (header width or max data width + gap).
	const colGap = 2
	cLagW := len("Topic Lag") + colGap     // 11
	cHealthW := 12                         // "✗ critical" = 10 visual + 2
	cRateW := len("Rate/s") + colGap       // 8
	cMembW := len("Members") + colGap      // 9
	cStateW := len("Rebalancing") + colGap // 13
	cGroupW := len("Group") + colGap       // 7
	for _, g := range relevant {
		if w := len(g.Name) + colGap; w > cGroupW {
			cGroupW = w
		}
		if w := len(fmt.Sprintf("%d", g.TopicLag[selectedTopic])) + colGap; w > cLagW {
			cLagW = w
		}
	}
	// Distribute remaining panel width as extra padding (capped at +8 per column).
	if totalMin := 2 + cGroupW + cLagW + cHealthW + cRateW + cMembW + cStateW; totalMin < width-4 {
		extra := (width - 4 - totalMin) / 6
		if extra > 8 {
			extra = 8
		}
		cGroupW += extra
		cLagW += extra
		cHealthW += extra
		cRateW += extra
		cMembW += extra
		cStateW += extra
	}

	if len(relevant) == 0 {
		lines = append(lines, dimStyle.Render("  No consumer groups for this topic."))
	} else {
		colHeader := "  " + dimStyle.Render(
			cell(cGroupW, "Group")+
				cell(cLagW, "Topic Lag")+
				cell(cHealthW, "Health")+
				cell(cRateW, "Rate/s")+
				cell(cMembW, "Members")+
				cell(cStateW, "State"),
		)
		lines = append(lines, colHeader)
		maxGroups := available - 1
		for i, g := range relevant {
			if i >= maxGroups {
				break
			}
			topicLag := g.TopicLag[selectedTopic]
			row := "  " + cell(cGroupW, truncate(g.Name, cGroupW)) +
				cell(cLagW, lagColored(topicLag)) +
				cell(cHealthW, healthStatus(topicLag)) +
				cell(cRateW, fmtRate(g.TopicRate[selectedTopic])) +
				cell(cMembW, fmt.Sprintf("%d", g.MemberCount)) +
				cell(cStateW, stateColored(g.State))
			lines = append(lines, row)
		}
	}

	// Pad so lipgloss fills the box to exactly innerH rows
	for len(lines) < innerH {
		lines = append(lines, "")
	}

	content := strings.Join(lines, "\n")
	borderColor := panelBorderColor
	if active {
		borderColor = colorYellow
	}
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Width(width).Height(innerH).
		Render(content)
}

// ── Footer ────────────────────────────────────────────────────────────────────

func (m Model) renderFooter() string {
	keys := dimStyle.Render("↑/k  ↓/j  navigate    [  ]  resize groups    r  refresh    q  quit")
	var fetchInfo string
	if m.clusterInfo != nil {
		fetchInfo = dimStyle.Render(fmt.Sprintf("topics: %d  groups: %d",
			len(m.clusterInfo.Topics), len(m.clusterInfo.Groups)))
	}
	gap := m.width - lipgloss.Width(keys) - lipgloss.Width(fetchInfo) - 2
	if gap < 1 {
		gap = 1
	}
	return " " + keys + strings.Repeat(" ", gap) + fetchInfo
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// fmtRetention formats a retention.ms value into a human-readable string.
// -1 = infinite (Kafka default meaning); 0 = unknown.
func fmtRetention(ms int64) string {
	switch {
	case ms == 0:
		return dimStyle.Render("—")
	case ms < 0:
		return dimStyle.Render("∞")
	}
	hours := ms / 3_600_000
	if hours == 0 {
		return fmt.Sprintf("%dm", ms/60_000)
	}
	if hours < 24 {
		return fmt.Sprintf("%dh", hours)
	}
	days := hours / 24
	h := hours % 24
	if h == 0 {
		return fmt.Sprintf("%dd", days)
	}
	return fmt.Sprintf("%dd%dh", days, h)
}

// isrColored renders the ISR/replica ratio with colour coding.
func isrColored(isr, replicas int) string {
	if replicas == 0 {
		return dimStyle.Render("—")
	}
	s := fmt.Sprintf("%d/%d", isr, replicas)
	if isr == replicas {
		return greenStyle.Render(s)
	}
	if isr*2 >= replicas { // majority in sync
		return yellowStyle.Render(s)
	}
	return redStyle.Render(s)
}

// stateColored renders a consumer group state with colour coding.
func stateColored(state string) string {
	switch state {
	case "Stable":
		return greenStyle.Render("Stable")
	case "Empty":
		return dimStyle.Render("Empty")
	case "PreparingRebalance", "CompletingRebalance":
		return yellowStyle.Render("Rebalancing")
	case "Dead":
		return redStyle.Render("Dead")
	default:
		if state == "" {
			return dimStyle.Render("—")
		}
		return dimStyle.Render(state)
	}
}

// lagColored returns the lag as a coloured string.
func lagColored(lag int64) string {
	s := fmt.Sprintf("%d", lag)
	switch {
	case lag == 0:
		return greenStyle.Render(s)
	case lag < 100:
		return greenStyle.Render(s)
	case lag < 1000:
		return yellowStyle.Render(s)
	default:
		return redStyle.Render(s)
	}
}

// healthStatus returns a coloured health indicator based on lag.
func healthStatus(lag int64) string {
	switch {
	case lag == 0:
		return greenStyle.Render("✓ healthy")
	case lag < 500:
		return yellowStyle.Render("↑ lagging")
	default:
		return redStyle.Render("✗ critical")
	}
}

// fmtRate formats a message/sec rate.
func fmtRate(rate float64) string {
	if rate <= 0 {
		return dimStyle.Render("—")
	}
	if rate >= 1000 {
		return fmt.Sprintf("%.1fk/s", rate/1000)
	}
	return fmt.Sprintf("%.0f/s", rate)
}

// truncate shortens s to at most n runes.
func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	if n <= 1 {
		return string(runes[:n])
	}
	return string(runes[:n-1]) + "…"
}
