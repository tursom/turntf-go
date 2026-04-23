package demo

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	turntf "github.com/tursom/turntf-go"
	"gopkg.in/yaml.v3"
)

const Version = "v1alpha1"

var (
	validActions = []string{
		"send_message",
		"send_packet",
		"ping",
		"create_user",
		"get_user",
		"update_user",
		"delete_user",
		"list_messages",
		"subscribe_channel",
		"unsubscribe_channel",
		"list_subscriptions",
		"block_user",
		"unblock_user",
		"list_blocked_users",
		"list_events",
		"operations_status",
		"metrics",
		"list_cluster_nodes",
		"list_node_logged_in_users",
	}
	validEvents = []string{
		"message_pushed",
		"packet_pushed",
		"error",
		"disconnect",
	}
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	var raw string
	if err := node.Decode(&raw); err != nil {
		return err
	}
	if strings.TrimSpace(raw) == "" {
		d.Duration = 0
		return nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", raw, err)
	}
	d.Duration = value
	return nil
}

type Scenario struct {
	Version     string                 `yaml:"version"`
	Name        string                 `yaml:"name"`
	Description string                 `yaml:"description"`
	Defaults    Defaults               `yaml:"defaults"`
	Vars        map[string]any         `yaml:"vars"`
	Nodes       map[string]NodeConfig  `yaml:"nodes"`
	Sessions    map[string]SessionSpec `yaml:"sessions"`
	Script      []Step                 `yaml:"script"`
}

type Defaults struct {
	Timeout         Duration `yaml:"timeout"`
	IdleTimeout     Duration `yaml:"idle_timeout"`
	AutoAckMessages *bool    `yaml:"auto_ack_messages"`
}

type NodeConfig struct {
	BaseURL string `yaml:"base_url"`
}

type SessionSpec struct {
	Node         string         `yaml:"node"`
	User         UserCredential `yaml:"user"`
	SeenMessages []CursorRef    `yaml:"seen_messages"`
}

type UserCredential struct {
	NodeID   int64         `yaml:"node_id"`
	UserID   int64         `yaml:"user_id"`
	Password PasswordValue `yaml:"password"`
}

type PasswordValue struct {
	turntf.PasswordInput
}

func (p *PasswordValue) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		Source string `yaml:"source"`
		Value  string `yaml:"value"`
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	switch raw.Source {
	case "plain":
		password, err := turntf.PlainPassword(raw.Value)
		if err != nil {
			return err
		}
		p.PasswordInput = password
		return nil
	case "hashed":
		password := turntf.HashedPassword(raw.Value)
		if err := password.Validate(); err != nil {
			return err
		}
		p.PasswordInput = password
		return nil
	default:
		return fmt.Errorf("password.source must be plain or hashed")
	}
}

type CursorRef struct {
	NodeID int64 `yaml:"node_id"`
	Seq    int64 `yaml:"seq"`
}

type Step struct {
	Step     string            `yaml:"step"`
	Name     string            `yaml:"name"`
	Session  string            `yaml:"session"`
	Action   string            `yaml:"action"`
	Request  map[string]any    `yaml:"request"`
	Expect   *Expectation      `yaml:"expect"`
	Event    string            `yaml:"event"`
	Match    map[string]any    `yaml:"match"`
	Save     map[string]string `yaml:"save"`
	Timeout  Duration          `yaml:"timeout"`
	Duration Duration          `yaml:"duration"`
	Branches []Branch          `yaml:"branches"`
}

type Branch struct {
	Name   string `yaml:"name"`
	Script []Step `yaml:"script"`
}

type Expectation struct {
	Login      map[string]any    `yaml:"login"`
	OK         map[string]any    `yaml:"ok"`
	Error      *ErrorExpectation `yaml:"error"`
	Disconnect string            `yaml:"disconnect"`
}

type ErrorExpectation struct {
	Code    string `yaml:"code"`
	Message string `yaml:"message"`
}

func Parse(data []byte) (*Scenario, error) {
	var scenario Scenario
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&scenario); err != nil {
		return nil, err
	}
	scenario.applyDefaults()
	if err := scenario.Validate(); err != nil {
		return nil, err
	}
	return &scenario, nil
}

func LoadFile(path string) (*Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

func (s *Scenario) Validate() error {
	if s.Version != Version {
		return fmt.Errorf("version must be %q", Version)
	}
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if len(s.Nodes) == 0 {
		return fmt.Errorf("nodes is required")
	}
	for name, node := range s.Nodes {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("nodes contains an empty key")
		}
		if strings.TrimSpace(node.BaseURL) == "" {
			return fmt.Errorf("nodes.%s.base_url is required", name)
		}
	}
	if len(s.Sessions) == 0 {
		return fmt.Errorf("sessions is required")
	}
	for name, session := range s.Sessions {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("sessions contains an empty key")
		}
		if _, ok := s.Nodes[session.Node]; !ok {
			return fmt.Errorf("sessions.%s.node references unknown node %q", name, session.Node)
		}
		if session.User.NodeID == 0 || session.User.UserID == 0 {
			return fmt.Errorf("sessions.%s.user requires node_id, user_id, password", name)
		}
		if err := session.User.Password.Validate(); err != nil {
			return fmt.Errorf("sessions.%s.user.password: %w", name, err)
		}
		for i, cursor := range session.SeenMessages {
			if cursor.NodeID == 0 || cursor.Seq == 0 {
				return fmt.Errorf("sessions.%s.seen_messages[%d] requires node_id and seq", name, i)
			}
		}
	}
	if len(s.Script) == 0 {
		return fmt.Errorf("script is required")
	}
	for i := range s.Script {
		if err := s.validateStep(s.Script[i], fmt.Sprintf("script[%d]", i), false); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scenario) validateStep(step Step, path string, inParallel bool) error {
	switch step.Step {
	case "connect":
		if err := s.requireSession(path, step.Session); err != nil {
			return err
		}
		if step.Expect == nil || len(step.Expect.Login) == 0 {
			return fmt.Errorf("%s.expect.login is required", path)
		}
	case "request":
		if err := s.requireSession(path, step.Session); err != nil {
			return err
		}
		if !slices.Contains(validActions, step.Action) {
			return fmt.Errorf("%s.action must be one of %v", path, validActions)
		}
		if step.Request == nil {
			return fmt.Errorf("%s.request is required", path)
		}
		if step.Expect == nil {
			return fmt.Errorf("%s.expect is required", path)
		}
		hasOK := len(step.Expect.OK) > 0
		hasError := step.Expect.Error != nil
		if hasOK == hasError {
			return fmt.Errorf("%s.expect must contain exactly one of ok or error", path)
		}
	case "expect_event":
		if err := s.requireSession(path, step.Session); err != nil {
			return err
		}
		if !slices.Contains(validEvents, step.Event) {
			return fmt.Errorf("%s.event must be one of %v", path, validEvents)
		}
		if len(step.Match) == 0 {
			return fmt.Errorf("%s.match is required", path)
		}
	case "sleep":
		if step.Duration.Duration <= 0 {
			return fmt.Errorf("%s.duration must be > 0", path)
		}
	case "close":
		if err := s.requireSession(path, step.Session); err != nil {
			return err
		}
	case "parallel":
		if len(step.Branches) < 2 {
			return fmt.Errorf("%s.branches must contain at least 2 branches", path)
		}
		seen := make(map[string]struct{}, len(step.Branches))
		for i, branch := range step.Branches {
			if strings.TrimSpace(branch.Name) == "" {
				return fmt.Errorf("%s.branches[%d].name is required", path, i)
			}
			if _, ok := seen[branch.Name]; ok {
				return fmt.Errorf("%s.branches contains duplicate name %q", path, branch.Name)
			}
			seen[branch.Name] = struct{}{}
			if len(branch.Script) == 0 {
				return fmt.Errorf("%s.branches[%d].script is required", path, i)
			}
			for j := range branch.Script {
				if err := s.validateStep(branch.Script[j], fmt.Sprintf("%s.branches[%d].script[%d]", path, i, j), true); err != nil {
					return err
				}
			}
		}
		if err := validateParallelBarriers(step, path); err != nil {
			return err
		}
	case "barrier":
		if !inParallel {
			return fmt.Errorf("%s.barrier can only be used inside parallel branches", path)
		}
		if strings.TrimSpace(step.Name) == "" {
			return fmt.Errorf("%s.name is required", path)
		}
	default:
		return fmt.Errorf("%s.step %q is unsupported", path, step.Step)
	}

	for name, expr := range step.Save {
		if !validVarName(name) {
			return fmt.Errorf("%s.save contains invalid variable name %q", path, name)
		}
		if strings.TrimSpace(expr) == "" {
			return fmt.Errorf("%s.save.%s is required", path, name)
		}
	}
	return nil
}

func validateParallelBarriers(step Step, path string) error {
	required := make(map[string]int)
	for _, branch := range step.Branches {
		seen := map[string]struct{}{}
		for _, branchStep := range branch.Script {
			if branchStep.Step != "barrier" {
				continue
			}
			if _, ok := seen[branchStep.Name]; ok {
				return fmt.Errorf("%s branch %q repeats barrier %q", path, branch.Name, branchStep.Name)
			}
			seen[branchStep.Name] = struct{}{}
			required[branchStep.Name]++
		}
	}
	for name, count := range required {
		if count != len(step.Branches) {
			return fmt.Errorf("%s barrier %q is not present in every branch", path, name)
		}
	}
	return nil
}

func (s *Scenario) requireSession(path, session string) error {
	if strings.TrimSpace(session) == "" {
		return fmt.Errorf("%s.session is required", path)
	}
	if _, ok := s.Sessions[session]; !ok {
		return fmt.Errorf("%s.session references unknown session %q", path, session)
	}
	return nil
}

func (s *Scenario) applyDefaults() {
	if s.Defaults.Timeout.Duration <= 0 {
		s.Defaults.Timeout.Duration = 5 * time.Second
	}
	if s.Defaults.IdleTimeout.Duration <= 0 {
		s.Defaults.IdleTimeout.Duration = 500 * time.Millisecond
	}
	if s.Defaults.AutoAckMessages == nil {
		value := true
		s.Defaults.AutoAckMessages = &value
	}
	if s.Vars == nil {
		s.Vars = map[string]any{}
	}
}

var varNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validVarName(name string) bool {
	return varNamePattern.MatchString(name)
}
