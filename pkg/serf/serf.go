/*
 * Copyright 2025 Alibaba Cloud, Inc. or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package serf

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/cachefs/pkg/consistent"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/eryugey/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
)

var logger = utils.GetLogger("cachefs")

const (
	// serf event queue depth
	EventQueueDepth = 16384

	// serf gossip interval
	GossipInterval = 200 * time.Millisecond

	// serf member event coalesce period (GossipInterval * 2)
	CoalescePeriod = GossipInterval * 2

	// serf event worker count
	EventWorkerCount = 8

	// number of virtual node, the larger the longer time required to re-colculate
	// hashring, 100 seems like a good trade-off
	VirtualNodeCount = 100

	// serf join worker count and task channel buffer size
	JoinWorkerCount = 4
)

const (
	// Serf roles
	SerfRoleInit   string = "initnode"
	SerfRoleSeeder string = "seeder"

	eventTypeUser         string = "user"
	eventTypeMemberJoin   string = "member-join"
	eventTypeMemberLeave  string = "member-leave"
	eventTypeMemberFailed string = "member-failed"

	// key of serf member tags
	keySerfRole  string = "role"
	keySerfGroup string = "group"
	keyRdmaGroup string = "rdmaGroup"
	keyCachePort string = "cachePort"

	// time interval to update consistent hash ring
	ringUpdateInterval time.Duration = 5 * time.Minute
)

type SerfService interface {
	// BroadCast `payload` info with serf user event `name` to serf cluster
	BroadCast(name string, payload []byte) error

	// Register new serf service user, users should be registered before `Start()`
	RegisterUser(user *SerfUser) error

	// Get member list of nodes in serf cluster, return cached members if raw is false
	GetMembers(raw bool) []string

	// Get number of nodes in serf cluster
	GetMemberCount() int

	// Check if given serf member exists in serf cluster
	MemberExists(memAddr string) bool

	// Return Group of this serf service
	Group() string

	// Return local address of this serf service
	LocalAddr() string

	// Check if given key hit local node of consistent hash ring
	HashHit(key string) bool

	// Return hash owner of given key, return `hashKey` and `memAddr`, where hashKey is the
	// owner's ring key (node name if useNodeName, else memAddr), memAddr is the owner's cache
	// address (host:cachePort)
	GetHashOwner(key string) (hashKey string, memAddr string, err error)

	// Start this serf service
	Start() error

	// Stop this serf service
	Stop()
}

type SerfMember struct {
	Name      string
	Addr      string
	Role      string
	RdmaGroup string
}

type SerfUser struct {
	// Name of this serf user
	Name string

	// Role in culster, only support one role for now, optional
	Role string

	// User event names used by this serf user
	UserEvents []string

	// Channel to receive MemberJoin members, optional
	MemberJoinCh chan []SerfMember

	// Channel to receive MemberLeave/Fail members, optional
	MemberLeaveCh chan []SerfMember

	// Channel to receive serf UserEvent
	UserEventCh chan serf.UserEvent

	// Customized UserEvent coalescer, optional
	UserEventCoalescer serf.Coalescer

	// Customized EventBuffer filter, optional. If set, only filtered events
	// will be included in the LocalState during Push-Pull synchronization.
	Filter serf.EventBufferFilterer

	// Channel to indicate serf service is ready
	ReadyCh chan struct{}

	// Channel to indicate serf joined existing network, optional
	JoinedCh chan struct{}
}

func (su *SerfUser) WaitReady() {
	<-su.ReadyCh
}

type serfService struct {
	config      *SerfConfig
	serfCluster *serf.Serf

	// users of this serf service, a `SerfUser` has only one `Name`
	userMu sync.RWMutex
	users  map[string]*SerfUser

	// name of serf user events, a `SerfUser`may have multiple `UserEvents`,
	// and each event name maps to the same `SerfUser`
	eventMu sync.RWMutex
	events  map[string]*SerfUser

	// serf members cache, indexed by member addr (ip:port) -> node name
	memberMu    sync.Mutex
	members     map[string]string
	hashring    *consistent.ConsistentHashRing
	useNodeName bool   // use serf node name as key in hash ring
	localKey    string // cached hash ring key for this node
	// reverse map from hash ring key to member cache addr, only maintained when
	// useNodeName is true (otherwise the ring key already is the cache addr).
	hashKeyToAddr map[string]string

	serfEventCh   chan serf.Event
	eventWorkerCh chan serf.Event
	eventChFull   chan struct{}

	joinTaskCh chan *joinTask

	stopCh chan struct{}
}

type joinResult struct {
	addr  string // target address, used for summary log
	nodes int    // number of joined nodes
	err   error
}

type joinTask struct {
	ip       string
	wg       *sync.WaitGroup   // optional: for blocking wait until task completes
	resultCh chan<- joinResult // optional: for collecting join results
}

func NewSerfService(conf *SerfConfig, reg prometheus.Registerer) SerfService {
	if conf.LoggerName != "" {
		logger = utils.GetLogger(conf.LoggerName)
	}

	// First check CFS_SERF_GROUP env for serf group, then check SERVICE_NAME,
	// which is used by PAI-EAS
	if conf.Group == "" {
		if os.Getenv("CFS_SERF_GROUP") != "" {
			conf.Group = os.Getenv("CFS_SERF_GROUP")
		} else if os.Getenv("SERVICE_NAME") != "" {
			conf.Group = os.Getenv("SERVICE_NAME")
		}
	}
	ss := &serfService{
		config:        conf,
		useNodeName:   conf.NodeName != "", // node name as hash key if configured explicitly
		users:         make(map[string]*SerfUser),
		events:        make(map[string]*SerfUser),
		members:       make(map[string]string),
		hashKeyToAddr: make(map[string]string),
		eventWorkerCh: make(chan serf.Event, EventQueueDepth),
		eventChFull:   make(chan struct{}, EventWorkerCount),
		joinTaskCh:    make(chan *joinTask, JoinWorkerCount),
		stopCh:        make(chan struct{}),
	}
	ss.InitMetrics(reg)
	return ss
}

func (s *serfService) InitMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "peer_count",
		Help: "Number of peer nodes in serf cluster",
	},
		func() float64 {
			return float64(s.GetMemberCount())
		}))
}

// Notify all serf users that serf service has been started.
func (s *serfService) notifyStarted() {
	s.userMu.RLock()
	defer s.userMu.RUnlock()
	for _, user := range s.users {
		if user.ReadyCh != nil {
			close(user.ReadyCh)
		}
	}
}

// Notify all serf users that serf network has been constructed.
func (s *serfService) notifySerfJoin() {
	s.userMu.RLock()
	defer s.userMu.RUnlock()
	for _, user := range s.users {
		if user.JoinedCh == nil {
			continue
		}
		select {
		case user.JoinedCh <- struct{}{}:
		default:
			logger.Warnf("serf: serf joined channel of user \"%s\" is full", user.Name)
		}
	}
}

func (s *serfService) Start() error {
	var err error

	s.userMu.RLock()
	for _, user := range s.users {
		if user.Role == "" {
			continue
		}
		if s.config.Role == "" {
			s.config.Role = user.Role
		} else {
			logger.Warnf("Ignore serf role \"%s\" set from serf user %s", user.Role, user.Name)
		}
	}
	s.userMu.RUnlock()

	// Generate serf Config from our local SerfConfig and save serfEventCh,
	// as it might be changed after serf.Create()
	eventCh := make(chan serf.Event, EventQueueDepth)
	conf := setupSerfConfig(s.config, s, s, eventCh)
	s.serfEventCh = eventCh

	// Create creates a new Serf instance, starting all the background tasks
	// to maintain cluster membership information.
	//
	// After calling this function, the configuration should no longer be used
	// or modified by the caller.
	s.serfCluster, err = serf.Create(conf)
	if err != nil {
		return fmt.Errorf("Create serf cluster: %s", err)
	}

	if s.config.HashRing {
		// Cache the local hash key before starting any event goroutines.
		s.localKey = s.memberHashKey(s.serfCluster.LocalMember().Name, s.config.CacheAddr)
		logger.Infof("serf: local hash key is %s", s.localKey)

		node := s.config.NodeCount
		logger.Infof("Creating consistent hash ring with %d virtual nodes", node)
		s.hashring = consistent.NewHashRing(node)
		s.hashring.AddToHashring(s.localKey)
		if s.useNodeName {
			// Seed the local node so its ring key resolves even if the self
			// member-join event is delayed or missing. Safe to write without
			// memberMu here: done before any event goroutine starts.
			s.hashKeyToAddr[s.localKey] = s.config.CacheAddr
		}
		go s.hashRingPeriodicUpdates()
	}

	// Start the Serf event handler goroutine early to avoid missing any join events
	// that occur after this point.
	go s.serveSerfEvents()

	// Start join workers early so that serfJoin() can submit tasks to them.
	logger.Debugf("Starting %d serf join workers...", JoinWorkerCount)
	for i := 0; i < JoinWorkerCount; i++ {
		go s.serfJoinWorker(i)
	}

	joinAddr := s.config.JoinAddr
	if joinAddr != "" {
		start := time.Now()
		addrs := strings.Fields(strings.TrimSpace(joinAddr))
		nodes, err := s.serfJoin(addrs)
		if err != nil {
			logger.Infof("Serf join \"%s\" failed, last error: %s", joinAddr, err)
		} else {
			logger.Infof("Serf joined to existing cluster, contacted %d nodes in %s", nodes, time.Since(start))
			s.notifySerfJoin()
		}
	}

	// If serf memberlist command is provided, run and join existing members in background
	if s.config.JoinCmd != "" {
		go s.serfJoinCmd()
	}

	// Notify users serf service has been started
	s.notifyStarted()
	return nil
}

func (s *serfService) Stop() {
	logger.Info("Stop serf service")
	if s.stopCh != nil {
		close(s.stopCh)
		s.stopCh = nil
	}
	if s.serfCluster != nil {
		s.serfCluster.Leave()
		s.serfCluster.Shutdown()
		s.serfCluster = nil
	}
}

func (s *serfService) BroadCast(name string, payload []byte) error {
	if s.stopCh == nil {
		return errors.New("serf service has been stopped")
	}
	if s.serfCluster == nil {
		return errors.New("serf service not started yet")
	}

	s.eventMu.RLock()
	_, ok := s.events[name]
	s.eventMu.RUnlock()
	if !ok {
		return fmt.Errorf("Serf user event \"%s\" not registered", name)
	}

	// TODO: check payload size and split if it's greater than serf.UserEventSizeLimit
	str := string(payload)
	logger.Debugf("serf: broadcast %s - %s to cluster, payload size %d", name, str, len(payload))
	return s.serfCluster.UserEvent(name, payload, true)
}

func (s *serfService) RegisterUser(user *SerfUser) (err error) {
	if user == nil {
		return nil
	}
	if user.Name == "" {
		return errors.New("serf user has empty name")
	}
	if len(user.UserEvents) == 0 {
		return errors.New("no serf user events")
	}

	s.userMu.Lock()
	_, ok := s.users[user.Name]
	if ok {
		s.userMu.Unlock()
		logger.Warnf("serf: Serf User \"%s\" has already been registered", user.Name)
		return fmt.Errorf("serf user \"%s\" already registered", user.Name)
	}
	s.users[user.Name] = user
	s.userMu.Unlock()

	s.eventMu.Lock()
	defer func() {
		s.eventMu.Unlock()
		if err == nil {
			return
		}

		// delete just added user on error
		s.userMu.Lock()
		delete(s.users, user.Name)
		s.userMu.Unlock()
	}()
	for _, event := range user.UserEvents {
		_, ok := s.events[event]
		if ok {
			logger.Warnf("serf: event %s from Serf User %s has already been registered", event, user.Name)
			return fmt.Errorf("serf event %s already registered", event)
		}
	}
	for _, event := range user.UserEvents {
		s.events[event] = user
	}
	return nil
}

func (s *serfService) Group() string {
	return s.config.Group
}

func (s *serfService) LocalAddr() string {
	return s.config.CacheAddr
}

func (s *serfService) HashHit(key string) bool {
	if s.stopCh == nil {
		return false
	}

	// Take it as hash miss if not started, or there's no hashring
	if s.serfCluster == nil {
		return false
	}

	if !s.config.HashRing || s.hashring == nil {
		return false
	}
	addr, err := s.hashring.Get(key)
	if err != nil {
		logger.Warnf("Get \"%s\" from hash ring: %s", key, err)
	} else {
		if addr == s.localKey {
			return true
		}
	}
	return false
}

func (s *serfService) GetHashOwner(key string) (string, string, error) {
	if s.stopCh == nil || s.serfCluster == nil {
		return "", "", fmt.Errorf("serf service not started")
	}
	if !s.config.HashRing || s.hashring == nil {
		return "", "", fmt.Errorf("hash ring not enabled")
	}
	hashKey, err := s.hashring.Get(key)
	if err != nil {
		return "", "", err
	}
	// When not using node name, the ring key already is the cache addr.
	if !s.useNodeName {
		return hashKey, hashKey, nil
	}
	s.memberMu.Lock()
	memAddr, ok := s.hashKeyToAddr[hashKey]
	s.memberMu.Unlock()
	if !ok {
		return hashKey, "", fmt.Errorf("hash owner %q not resolved to cache addr", hashKey)
	}
	return hashKey, memAddr, nil
}

// get member list
func (s *serfService) GetMembers(raw bool) []string {
	if s.stopCh == nil {
		return nil
	}
	if s.serfCluster == nil {
		logger.Warn("Serf GetMembers: service not started yet.")
		return nil
	}
	var memAddrs []string
	if !raw {
		// return members in our member list
		s.memberMu.Lock()
		defer s.memberMu.Unlock()
		memAddrs = make([]string, 0, len(s.members))
		for k := range s.members {
			memAddrs = append(memAddrs, k)
		}
	} else {
		// return members in underlying serf cluster
		rawMembers := s.serfCluster.Members()
		memAddrs = make([]string, 0, len(rawMembers))
		for _, member := range rawMembers {
			if member.Status == serf.StatusAlive {
				memAddr := formatMemberAddr(&member)
				memAddrs = append(memAddrs, memAddr)
			}
		}
	}
	return memAddrs
}

// get the number of members
func (s *serfService) GetMemberCount() int {
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	return len(s.members)
}

// check given member addr exists in member cache
func (s *serfService) MemberExists(memAddr string) bool {
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	_, ok := s.members[memAddr]
	return ok
}

func (s *serfService) serfEventWorker() {
	for {
		select {
		case <-s.stopCh:
			return
		case <-s.eventChFull:
			// Skip half of old events
			toSkip := EventQueueDepth / 2 / EventWorkerCount
			for i := 0; i < toSkip; i++ {
				<-s.eventWorkerCh
			}
		case e := <-s.eventWorkerCh:
			s.handleSerfEvent(e)
		}
	}
}

func (s *serfService) serveSerfEvents() {
	for i := 0; i < EventWorkerCount; i++ {
		go s.serfEventWorker()
	}
	for {
		select {
		case <-s.stopCh:
			logger.Info("serf: Stop receving user events from cluster")
			return
		case e := <-s.serfEventCh:
			select {
			case s.eventWorkerCh <- e:
			default:
				for i := 0; i < EventWorkerCount; i++ {
					s.eventChFull <- struct{}{}
				}
				logger.Warn("Serf event channel full, dropping half old events")
			}
		}
	}
}

// serfJoinOne joins a single serf address. This is the actual function that
// performs the join work. The address may or may not include a port; if no port
// is specified, the default serf bind port from config is appended.
func (s *serfService) serfJoinOne(addr string) (int, error) {
	var serfAddr string
	if strings.Contains(addr, ":") {
		serfAddr = addr
	} else {
		serfAddr = fmt.Sprintf("%s:%d", addr, s.config.BindPort)
	}
	return s.serfCluster.Join([]string{serfAddr}, false)
}

// Join joins an existing Serf cluster by submitting join tasks to the shared
// task channel and waiting for all results concurrently. Returns the total number
// of nodes successfully contacted. The returned error will be non-nil only in
// the case that no nodes could be contacted.
func (s *serfService) serfJoin(addrs []string) (int, error) {
	if len(addrs) == 0 {
		return 0, nil
	}
	resultCh := make(chan joinResult, len(addrs))
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		task := &joinTask{
			ip:       addr,
			resultCh: resultCh,
			wg:       &wg,
		}
		select {
		case <-s.stopCh:
			wg.Done()
			continue
		case s.joinTaskCh <- task:
		}
	}
	wg.Wait()
	close(resultCh)

	var totalNodes int
	var errs []error
	var groupMismatchAddrs []string
	for result := range resultCh {
		totalNodes += result.nodes
		if result.err != nil {
			if isGroupMismatchError(result.err) {
				groupMismatchAddrs = append(groupMismatchAddrs, result.addr)
			} else {
				errs = append(errs, result.err)
			}
		}
	}

	if len(groupMismatchAddrs) > 0 {
		logger.Infof("Serf join: %d members rejected due to group mismatch: %s",
			len(groupMismatchAddrs), strings.Join(groupMismatchAddrs, ", "))
	}

	// As long as at least one address joined successfully, return the total
	// number of nodes contacted without error.
	if totalNodes > 0 {
		return totalNodes, nil
	}
	if len(errs) > 0 {
		return 0, errs[0]
	}
	if len(groupMismatchAddrs) > 0 {
		// totalNodes==0 and errs is empty, so every failure was a group mismatch.
		// This is a permanent configuration error, not a transient failure.
		return 0, fmt.Errorf("%w: all %d addresses rejected",
			ErrGroupMismatch, len(groupMismatchAddrs))
	}
	return 0, nil
}

func (s *serfService) serfJoinWorker(id int) {
	for {
		select {
		case <-s.stopCh:
			logger.Debugf("Serf join worker %d shutting down due to service stop.", id)
			return
		case task, ok := <-s.joinTaskCh:
			if !ok {
				logger.Debugf("Serf join worker %d shutting down: task channel closed", id)
				return
			}

			startJoin := time.Now()
			nodes, err := s.serfJoinOne(task.ip)
			joinDuration := time.Since(startJoin)

			if err != nil {
				if !isGroupMismatchError(err) {
					logger.Warnf("Serf join %s in join worker %d error: %s", task.ip, id, err)
				}
			} else {
				logger.Debugf("Serf join worker %d joined %s contacted %d nodes in %s",
					id, task.ip, nodes, joinDuration)
			}
			if task.resultCh != nil {
				task.resultCh <- joinResult{nodes: nodes, err: err, addr: task.ip}
			}
			if task.wg != nil {
				task.wg.Done()
			}
		}
	}
}

func (s *serfService) serfJoinCmd() {
	// For the first phase 6 attempts, keep a 5 second interval.
	// Subsequently, try once per minute, up to a total of `secondPhaseAttempts` additional attempts.
	const (
		firstPhaseAttempts = 6
		firstInterval      = 5 * time.Second
		secondInterval     = 1 * time.Minute
	)

	secondPhaseAttempts := s.config.MaxRetry
	maxAttempts := firstPhaseAttempts + secondPhaseAttempts
	logger.Debugf(
		"SerfCmd attempts %v for %d times and %v for %d times, total attempts %d times",
		firstInterval, firstPhaseAttempts,
		secondInterval, secondPhaseAttempts,
		maxAttempts,
	)

	retry := uint32(0)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	noOutput := true
	script := s.config.JoinCmd
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		if retry >= maxAttempts {
			logger.Infof("Stop retrying serf memberlist command after %d attempts", retry)
			if noOutput {
				logger.Warnf("Serf memberlist command got no output after %d attempts", retry)
			}
			break
		}
		if retry < firstPhaseAttempts && retry > 0 {
			time.Sleep(firstInterval)
		} else if retry >= firstPhaseAttempts {
			time.Sleep(secondInterval)
		}
		retry++

		startExec := time.Now()
		out, err := exec.Command(script).CombinedOutput()
		ipList := strings.TrimSpace(string(out))
		if err != nil {
			logger.Warnf("Run serf memberlist command %s, error: \"%s\", output: \"%s\"", script, err, ipList)
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
				break
			}
			continue
		}
		if len(ipList) == 0 {
			logger.Warnf("Serf memberlist command %s got no output, try again", script)
			continue
		}
		noOutput = false

		logger.Infof("Serf memberlist command %s run in %s, output: \"%s\"", script, time.Since(startExec), ipList)
		ips := strings.Fields(ipList)
		rng.Shuffle(len(ips), func(i, j int) { ips[i], ips[j] = ips[j], ips[i] })

		nodes, err := s.serfJoin(ips)
		if nodes > 1 {
			s.notifySerfJoin()
			logger.Infof("Serf joined cluster with %d members from memberlist command in %s",
				nodes, time.Since(startExec))
			return
		} else {
			logger.Info("Serf cluster only contains myself, try again")
		}
	}
}

func (s *serfService) handleSerfUserEvent(e serf.Event) {
	userEvent, ok := e.(serf.UserEvent)
	if !ok {
		logger.Warnf("serf: event %s is not a user event", e)
		return
	}
	eventName := userEvent.Name
	payload := userEvent.Payload
	if payload == nil {
		logger.Warnf("serf: user event \"%s\" has no payload, skip", eventName)
		return
	}

	s.eventMu.RLock()
	user, ok := s.events[eventName]
	s.eventMu.RUnlock()
	if !ok {
		logger.Warnf("serf: user event \"%s\" is not registered", eventName)
		return
	}

	if user.UserEventCh == nil {
		return
	}
	select {
	case user.UserEventCh <- userEvent:
	default:
		logger.Warnf("serf: user event channel \"%s\" is full", eventName)
	}
}

func (s *serfService) handleSerfMemberJoinEvent(e serf.Event) {
	memberEvent, ok := e.(serf.MemberEvent)
	if !ok {
		logger.Warnf("serf: event %s is not a member event", e)
		return
	}
	start := time.Now()
	members := make([]SerfMember, 0, len(memberEvent.Members))
	for _, member := range memberEvent.Members {
		memAddr := formatMemberAddr(&member)
		if !s.inSameSerfGroup(&member) {
			group, same := isSerfGroup(s.config.Group, &member)
			if !same {
				logger.Infof("Ignore member join event from %s with a different group: \"%s\"", memAddr, group)
			}
			continue
		}
		s.addMember(member.Name, memAddr)
		role := getMemberRole(&member)
		members = append(members, SerfMember{
			Name: member.Name, Addr: memAddr, Role: role, RdmaGroup: member.Tags[keyRdmaGroup],
		})
	}
	if len(members) > 0 {
		logger.Infof("Handled %d new member(s) join event in %s, %d members in cluster",
			len(members), time.Since(start), s.GetMemberCount())
	}

	// Notify all serf users about new members
	s.userMu.RLock()
	defer s.userMu.RUnlock()
	for name, user := range s.users {
		if user.MemberJoinCh == nil {
			continue
		}
		select {
		case user.MemberJoinCh <- members:
		default:
			logger.Warnf("serf: member join channel \"%s\" is full", name)
		}
	}
}

func (s *serfService) handleSerfMemberLeaveEvent(e serf.Event) {
	memberEvent, ok := e.(serf.MemberEvent)
	if !ok {
		logger.Warnf("serf: event %s is not a member event", e)
		return
	}

	start := time.Now()
	members := make([]SerfMember, 0, len(memberEvent.Members))
	for _, member := range memberEvent.Members {
		memAddr := formatMemberAddr(&member)

		// Check to avoid removing local node from the hash ring.
		if memAddr == s.config.CacheAddr {
			continue
		}

		// Ignore members from other groups.
		if !s.inSameSerfGroup(&member) {
			continue
		}
		s.delMember(member.Name, memAddr)
		role := getMemberRole(&member)
		members = append(members, SerfMember{Name: member.Name, Addr: memAddr, Role: role})
	}
	if len(members) == 0 {
		return
	}

	logger.Infof("Handle %d %s members in %s, %d members in cluster",
		len(members), e, time.Since(start), s.GetMemberCount())

	// Notify all serf users about leaving members
	s.userMu.RLock()
	defer s.userMu.RUnlock()
	for name, user := range s.users {
		if user.MemberLeaveCh == nil {
			continue
		}
		select {
		case user.MemberLeaveCh <- members:
		default:
			logger.Warnf("serf: member leave channel \"%s\" is full", name)
		}
	}
}

func (s *serfService) handleSerfEvent(e serf.Event) {
	logger.Tracef("serf raw event %v", e)

	// Only handle the given type of event
	eventType := e.EventType().String()
	if !isValidEventType(eventType) {
		logger.Debugf("Received serf event %q, skip", eventType)
		return
	}

	if eventType == eventTypeUser {
		s.handleSerfUserEvent(e)
	} else if eventType == eventTypeMemberJoin {
		s.handleSerfMemberJoinEvent(e)
	} else if eventType == eventTypeMemberLeave {
		s.handleSerfMemberLeaveEvent(e)
	} else if eventType == eventTypeMemberFailed {
		s.handleSerfMemberLeaveEvent(e)
	} else {
		logger.Warnf("serf: event type %s not supported yet", eventType)
	}
	return
}

func (s *serfService) hashRingPeriodicUpdates() {
	ticker := time.NewTicker(ringUpdateInterval)
	for {
		select {
		case <-s.stopCh:
			ticker.Stop()
			return
		case <-ticker.C:
			s.updateHashRing()
		}
	}
}

// updates the hash ring based on the current Serf cluster members.
func (s *serfService) updateHashRing() {
	s.memberMu.Lock()
	keys := make([]string, 0, len(s.members))
	for addr, name := range s.members {
		keys = append(keys, s.memberHashKey(name, addr))
	}
	s.hashring.Set(keys)
	s.memberMu.Unlock()
	logger.Debugf("serf: set %v to hash ring", keys)
	logger.Infof("Updated hash ring with %d member(s)", len(keys))
}

// add new member and update hashring if needed
func (s *serfService) addMember(name, memAddr string) {
	logger.Infof("Add new node %s(%s)", name, memAddr)
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	s.members[memAddr] = name
	if s.config.HashRing {
		hashKey := s.memberHashKey(name, memAddr)
		if s.hashring.AddToHashring(hashKey) {
			logger.Debugf("serf: added %s(%s) to hash ring with key %q", name, memAddr, hashKey)
		} else {
			logger.Debugf("serf: node %s(%s) already in hash ring", name, memAddr)
		}
		if s.useNodeName {
			s.hashKeyToAddr[hashKey] = memAddr
		}
	}
}

// remove member and update hashring if needed
func (s *serfService) delMember(name, memAddr string) {
	logger.Infof("Delete node %s(%s)", name, memAddr)
	s.memberMu.Lock()
	defer s.memberMu.Unlock()
	delete(s.members, memAddr)

	// Remove the member from the consistent hash ring.
	if s.config.HashRing {
		hashKey := s.memberHashKey(name, memAddr)
		if s.hashring.RemoveFromHashring(hashKey) {
			logger.Debugf("serf: removed %s(%s) from hash ring with key %q", name, memAddr, hashKey)
		} else {
			logger.Debugf("serf: %s(%s) not exists in hash ring", name, memAddr)
		}
		if s.useNodeName {
			delete(s.hashKeyToAddr, hashKey)
		}
	}
}

func getMemberRole(member *serf.Member) string {
	role, ok := member.Tags[keySerfRole]
	if !ok {
		return ""
	}
	return role
}

// check if the given member is in the same serf group as we are
func (s *serfService) inSameSerfGroup(member *serf.Member) bool {
	// init node doesn't belong to any group
	if isSerfInitNode(member) {
		return false
	}
	_, same := isSerfGroup(s.config.Group, member)
	return same
}

func isSerfInitNode(member *serf.Member) bool {
	return getMemberRole(member) == SerfRoleInit
}

// check if the given member is in the same group as specified group
func isSerfGroup(group string, member *serf.Member) (string, bool) {
	memGroup, _ := member.Tags[keySerfGroup]
	return memGroup, group == memGroup
}

func formatMemberAddr(member *serf.Member) string {
	return fmt.Sprintf("%s:%s", member.Addr.String(), member.Tags[keyCachePort])
}

// get the hash ring key for a member.
func (s *serfService) memberHashKey(name, addr string) string {
	if s.useNodeName {
		return name
	}
	return addr
}

func isValidEventType(typ string) bool {
	switch typ {
	case eventTypeUser, eventTypeMemberJoin, eventTypeMemberLeave, eventTypeMemberFailed:
		return true
	default:
		return false
	}
}
