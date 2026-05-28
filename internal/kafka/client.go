package kafka

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// PartitionInfo holds offset and throughput data for a single partition.
type PartitionInfo struct {
	ID           int32
	Newest       int64
	Oldest       int64
	Rate         float64 // messages produced per second
	Leader       int32   // leader broker ID (-1 = unknown)
	ReplicaCount int     // total replica count
	ISRCount     int     // in-sync replica count
}

// TopicInfo aggregates partition data for a topic.
type TopicInfo struct {
	Name              string
	Partitions        []PartitionInfo
	ReplicationFactor int
	RetentionMs       int64 // -1 = infinite; 0 = unknown
}

// ConsumerGroupInfo holds lag and throughput data for a consumer group.
type ConsumerGroupInfo struct {
	Name        string
	TotalLag    int64
	// Per-topic lag: topic name -> total lag for that topic
	TopicLag    map[string]int64
	// Per-topic consume rate: topic name -> msgs consumed per second
	TopicRate   map[string]float64
	MemberCount int    // number of active consumers in the group
	State       string // "Stable", "Empty", "PreparingRebalance", etc.
}

// TopicPartitionLag represents consumer lag for a single partition.
type TopicPartitionLag struct {
	Topic     string
	Partition int32
	Lag       int64
}

// ClusterInfo is the full snapshot returned on each refresh.
type ClusterInfo struct {
	BrokerCount int
	Topics      []TopicInfo
	Groups      []ConsumerGroupInfo
	// ConsumerLag is indexed [topic][partition] -> lag (across all groups, summed)
	ConsumerLag map[string]map[int32]int64
	FetchedAt   time.Time
}

// Client wraps a persistent sarama client + admin connection.
type Client struct {
	brokers      []string
	saramaClient sarama.Client
	admin        sarama.ClusterAdmin
	mu           sync.Mutex

	// For produce rate calculation
	prevOffsets map[string]map[int32]int64
	prevFetchAt time.Time

	// For consumer rate calculation: group -> topic -> partition -> committed offset
	prevGroupOffsets map[string]map[string]map[int32]int64
}

// NewClient dials the brokers and returns a ready Client.
func NewClient(brokers []string) (*Client, error) {
	cfg := saramaConfig()

	sc, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(sc)
	if err != nil {
		sc.Close()
		return nil, fmt.Errorf("admin: %w", err)
	}

	return &Client{
		brokers:          brokers,
		saramaClient:     sc,
		admin:            admin,
		prevOffsets:      make(map[string]map[int32]int64),
		prevGroupOffsets: make(map[string]map[string]map[int32]int64),
	}, nil
}

// Close releases all resources.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.admin != nil {
		c.admin.Close()
	}
	if c.saramaClient != nil {
		c.saramaClient.Close()
	}
}

// FetchClusterInfo refreshes metadata and returns a full cluster snapshot.
func (c *Client) FetchClusterInfo() (*ClusterInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Refresh broker metadata
	if err := c.saramaClient.RefreshMetadata(); err != nil {
		return nil, fmt.Errorf("refresh metadata: %w", err)
	}

	info := &ClusterInfo{
		BrokerCount: len(c.saramaClient.Brokers()),
		ConsumerLag: make(map[string]map[int32]int64),
		FetchedAt:   now,
	}

	// ── Topics ───────────────────────────────────────────────────────────
	topicMap, err := c.admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	var topicNames []string
	for name := range topicMap {
		if len(name) > 0 && name[0] != '_' { // skip internal topics
			topicNames = append(topicNames, name)
		}
	}
	sort.Strings(topicNames)

	elapsed := now.Sub(c.prevFetchAt).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	currentOffsets := make(map[string]map[int32]int64)

	for _, topicName := range topicNames {
		detail := topicMap[topicName]
		currentOffsets[topicName] = make(map[int32]int64)
		info.ConsumerLag[topicName] = make(map[int32]int64)

		var parts []PartitionInfo
		for i := int32(0); i < detail.NumPartitions; i++ {
			newest, _ := c.saramaClient.GetOffset(topicName, i, sarama.OffsetNewest)
			oldest, _ := c.saramaClient.GetOffset(topicName, i, sarama.OffsetOldest)
			if newest < 0 {
				newest = 0
			}
			if oldest < 0 {
				oldest = 0
			}
			currentOffsets[topicName][i] = newest

			var rate float64
			if !c.prevFetchAt.IsZero() {
				if prevTopic, ok := c.prevOffsets[topicName]; ok {
					if prevOffset, ok := prevTopic[i]; ok {
						delta := newest - prevOffset
						if delta > 0 {
							rate = float64(delta) / elapsed
						}
					}
				}
			}

			// Partition metadata is cached after RefreshMetadata, no extra network calls.
			var leaderID int32 = -1
			var replicaCount, isrCount int
			if leader, err := c.saramaClient.Leader(topicName, i); err == nil {
				leaderID = leader.ID()
			}
			if replicas, err := c.saramaClient.Replicas(topicName, i); err == nil {
				replicaCount = len(replicas)
			}
			if isr, err := c.saramaClient.InSyncReplicas(topicName, i); err == nil {
				isrCount = len(isr)
			}

			parts = append(parts, PartitionInfo{
				ID:           i,
				Newest:       newest,
				Oldest:       oldest,
				Rate:         rate,
				Leader:       leaderID,
				ReplicaCount: replicaCount,
				ISRCount:     isrCount,
			})
		}

		// Fetch retention config (single lightweight admin call per topic).
		var retentionMs int64
		if entries, err := c.admin.DescribeConfig(sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        topicName,
			ConfigNames: []string{"retention.ms"},
		}); err == nil {
			for _, e := range entries {
				if e.Name == "retention.ms" {
					retentionMs, _ = strconv.ParseInt(e.Value, 10, 64)
				}
			}
		}

		info.Topics = append(info.Topics, TopicInfo{
			Name:              topicName,
			Partitions:        parts,
			ReplicationFactor: int(detail.ReplicationFactor),
			RetentionMs:       retentionMs,
		})
	}

	// Save offsets for next rate calculation
	c.prevOffsets = currentOffsets
	c.prevFetchAt = now

	// ── Consumer Groups ──────────────────────────────────────────────────
	groupMap, err := c.admin.ListConsumerGroups()
	if err != nil {
		// Non-fatal: return topic data without group info
		return info, nil
	}

	var groupNames []string
	for name, protoType := range groupMap {
		if protoType == "consumer" {
			groupNames = append(groupNames, name)
		}
	}
	sort.Strings(groupNames)

	// Batch-fetch group descriptions (member count + state) in a single request.
	groupDescMap := make(map[string]*sarama.GroupDescription)
	if len(groupNames) > 0 {
		if descs, err := c.admin.DescribeConsumerGroups(groupNames); err == nil {
			for _, d := range descs {
				groupDescMap[d.GroupId] = d
			}
		}
	}

	// Build a helper: topicName -> index in info.Topics
	topicIdx := make(map[string]int, len(info.Topics))
	for i, t := range info.Topics {
		topicIdx[t.Name] = i
	}

	currentGroupOffsets := make(map[string]map[string]map[int32]int64)

	for _, groupName := range groupNames {
		committed, err := c.admin.ListConsumerGroupOffsets(groupName, nil)
		if err != nil {
			continue
		}

		currentGroupOffsets[groupName] = make(map[string]map[int32]int64)

		var totalLag int64
		topicLag := make(map[string]int64)
		topicRate := make(map[string]float64)

		for topicName, partBlocks := range committed.Blocks {
			idx, exists := topicIdx[topicName]
			if !exists {
				continue
			}
			topic := info.Topics[idx]

			currentGroupOffsets[groupName][topicName] = make(map[int32]int64)

			// Build a quick lookup: partition ID -> newest offset
			newestByPart := make(map[int32]int64, len(topic.Partitions))
			for _, p := range topic.Partitions {
				newestByPart[p.ID] = p.Newest
			}

			for partID, block := range partBlocks {
				if block.Err != sarama.ErrNoError || block.Offset < 0 {
					continue
				}
				newest, ok := newestByPart[partID]
				if !ok {
					continue
				}
				lag := newest - block.Offset
				if lag < 0 {
					lag = 0
				}
				totalLag += lag
				topicLag[topicName] += lag
				info.ConsumerLag[topicName][partID] += lag

				currentGroupOffsets[groupName][topicName][partID] = block.Offset

				// Consume rate: delta in committed offset / elapsed
				if !c.prevFetchAt.IsZero() {
					if prevGroup, ok := c.prevGroupOffsets[groupName]; ok {
						if prevTopic, ok := prevGroup[topicName]; ok {
							if prevOff, ok := prevTopic[partID]; ok {
								delta := block.Offset - prevOff
								if delta > 0 {
									topicRate[topicName] += float64(delta) / elapsed
								}
							}
						}
					}
				}
			}
		}

		if totalLag > 0 || len(topicLag) > 0 {
			var memberCount int
			var state string
			if desc, ok := groupDescMap[groupName]; ok {
				memberCount = len(desc.Members)
				state = desc.State
			}
			info.Groups = append(info.Groups, ConsumerGroupInfo{
				Name:        groupName,
				TotalLag:    totalLag,
				TopicLag:    topicLag,
				TopicRate:   topicRate,
				MemberCount: memberCount,
				State:       state,
			})
		}
	}

	c.prevGroupOffsets = currentGroupOffsets

	return info, nil
}

func saramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0
	cfg.Net.DialTimeout = 10 * time.Second
	cfg.Net.ReadTimeout = 10 * time.Second
	cfg.Net.WriteTimeout = 10 * time.Second
	cfg.Metadata.RefreshFrequency = 60 * time.Second
	cfg.Metadata.Full = true
	return cfg
}
