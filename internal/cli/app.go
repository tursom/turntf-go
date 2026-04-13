package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"

	turntf "github.com/tursom/turntf-go"
)

type CLI struct {
	BaseURL           string               `name:"base-url" help:"TurnTF base URL." env:"TURNTF_BASE_URL" required:""`
	NodeID            int64                `name:"node-id" help:"Client node ID for WS commands." env:"TURNTF_NODE_ID"`
	UserID            int64                `name:"user-id" help:"Client user ID for WS commands." env:"TURNTF_USER_ID"`
	Password          string               `help:"Client password for WS commands." env:"TURNTF_PASSWORD"`
	AdminNodeID       int64                `name:"admin-node-id" help:"Admin node ID for HTTP login or admin WS commands." env:"TURNTF_ADMIN_NODE_ID"`
	AdminUserID       int64                `name:"admin-user-id" help:"Admin user ID for HTTP login or admin WS commands." env:"TURNTF_ADMIN_USER_ID"`
	AdminPassword     string               `name:"admin-password" help:"Admin password for HTTP login or admin WS commands." env:"TURNTF_ADMIN_PASSWORD"`
	Timeout           time.Duration        `help:"Request timeout for non-listen commands." default:"10s"`
	PingInterval      time.Duration        `name:"ping-interval" help:"Application ping interval for WS clients." default:"30s"`
	InitialReconnect  time.Duration        `name:"initial-reconnect-delay" help:"Initial reconnect delay for WS clients." default:"1s"`
	MaxReconnect      time.Duration        `name:"max-reconnect-delay" help:"Maximum reconnect delay for WS clients." default:"30s"`
	JSON              bool                 `name:"json" help:"Emit structured JSON output when supported."`
	Listen            ListenCmd            `cmd:"" help:"Listen on a websocket client connection and print incoming events."`
	SendMessage       SendMessageCmd       `cmd:"" name:"send-message" help:"Send a persistent message over websocket."`
	SendPacket        SendPacketCmd        `cmd:"" name:"send-packet" help:"Send a transient packet over websocket."`
	Login             LoginCmd             `cmd:"" help:"Run admin HTTP login and print the bearer token."`
	CreateUser        CreateUserCmd        `cmd:"" name:"create-user" help:"Create a user with admin HTTP login."`
	CreateChannel     CreateChannelCmd     `cmd:"" name:"create-channel" help:"Create a channel with admin HTTP login."`
	Subscribe         SubscribeCmd         `cmd:"" help:"Create a channel subscription with admin HTTP login."`
	ListMessages      ListMessagesCmd      `cmd:"" name:"list-messages" help:"List historical messages through websocket RPC."`
	GetUser           GetUserCmd           `cmd:"" name:"get-user" help:"Get a user through websocket RPC."`
	UpdateUser        UpdateUserCmd        `cmd:"" name:"update-user" help:"Update a user through websocket RPC."`
	DeleteUser        DeleteUserCmd        `cmd:"" name:"delete-user" help:"Delete a user through websocket RPC."`
	ListSubscriptions ListSubscriptionsCmd `cmd:"" name:"list-subscriptions" help:"List subscriptions through websocket RPC."`
	ListEvents        ListEventsCmd        `cmd:"" name:"list-events" help:"List events through websocket RPC."`
	OpsStatus         OpsStatusCmd         `cmd:"" name:"ops-status" help:"Fetch operations status through websocket RPC."`
	Metrics           MetricsCmd           `cmd:"" help:"Fetch metrics through websocket RPC."`
	Demo              DemoCmd              `cmd:"" help:"Run a small end-to-end demo and then keep listening."`
}

type ListenCmd struct{}

type SendMessageCmd struct {
	TargetNodeID int64  `name:"target-node-id" help:"Target node ID." required:""`
	TargetUserID int64  `name:"target-user-id" help:"Target user ID." required:""`
	Sender       string `help:"Sender label." required:""`
	BodyInput
}

type SendPacketCmd struct {
	TargetNodeID int64  `name:"target-node-id" help:"Target node ID." required:""`
	TargetUserID int64  `name:"target-user-id" help:"Target user ID." required:""`
	Sender       string `help:"Sender label." required:""`
	Mode         string `help:"Transient delivery mode." enum:"best_effort,route_retry" default:"best_effort"`
	BodyInput
}

type LoginCmd struct{}

type CreateUserCmd struct {
	Username    string `help:"Username to create." required:""`
	NewPassword string `name:"new-password" help:"Password for the new user." required:""`
	Role        string `help:"Role for the new user." default:"user"`
	ProfileJSON string `name:"profile-json" help:"Profile JSON string."`
}

type CreateChannelCmd struct {
	Username    string `help:"Channel username to create." required:""`
	ProfileJSON string `name:"profile-json" help:"Profile JSON string."`
}

type SubscribeCmd struct {
	SubscriberNodeID int64 `name:"subscriber-node-id" help:"Subscriber node ID." required:""`
	SubscriberUserID int64 `name:"subscriber-user-id" help:"Subscriber user ID." required:""`
	ChannelNodeID    int64 `name:"channel-node-id" help:"Channel node ID." required:""`
	ChannelUserID    int64 `name:"channel-user-id" help:"Channel user ID." required:""`
}

type ListMessagesCmd struct {
	TargetNodeID int64 `name:"target-node-id" help:"Target node ID." required:""`
	TargetUserID int64 `name:"target-user-id" help:"Target user ID." required:""`
	Limit        int   `help:"Maximum number of messages to return." default:"20"`
}

type GetUserCmd struct {
	TargetNodeID int64 `name:"target-node-id" help:"Target node ID." required:""`
	TargetUserID int64 `name:"target-user-id" help:"Target user ID." required:""`
}

type UpdateUserCmd struct {
	TargetNodeID int64  `name:"target-node-id" help:"Target node ID." required:""`
	TargetUserID int64  `name:"target-user-id" help:"Target user ID." required:""`
	Username     string `help:"Updated username."`
	NewPassword  string `name:"new-password" help:"Updated password."`
	Role         string `help:"Updated role."`
	ProfileJSON  string `name:"profile-json" help:"Updated profile JSON string."`
}

type DeleteUserCmd struct {
	TargetNodeID int64 `name:"target-node-id" help:"Target node ID." required:""`
	TargetUserID int64 `name:"target-user-id" help:"Target user ID." required:""`
}

type ListSubscriptionsCmd struct {
	SubscriberNodeID int64 `name:"subscriber-node-id" help:"Subscriber node ID." required:""`
	SubscriberUserID int64 `name:"subscriber-user-id" help:"Subscriber user ID." required:""`
}

type ListEventsCmd struct {
	After int64 `help:"List events after this sequence or event ID." default:"0"`
	Limit int   `help:"Maximum number of events to return." default:"20"`
}

type OpsStatusCmd struct{}

type MetricsCmd struct{}

type DemoCmd struct {
	UserName      string `name:"user-name" help:"Demo user name." default:"demo-user"`
	UserPassword  string `name:"user-password" help:"Demo user password." default:"demo-password"`
	ChannelName   string `name:"channel-name" help:"Optional demo channel name. Leave empty to skip channel creation." default:"demo-channel"`
	MessageSender string `name:"message-sender" help:"Sender label for the demo message." default:"demo-cli"`
	MessageBody   string `name:"message-body" help:"Message body to send during demo." default:"hello from turntf-client demo"`
	SendPacket    bool   `name:"send-packet" help:"Also send a transient packet to the demo user."`
	PacketMode    string `name:"packet-mode" help:"Transient packet delivery mode." enum:"best_effort,route_retry" default:"best_effort"`
}

type BodyInput struct {
	Body     string `help:"UTF-8 body string." xor:"body_source"`
	BodyHex  string `name:"body-hex" help:"Hex encoded body bytes." xor:"body_source"`
	BodyFile string `name:"body-file" help:"Read body bytes from file." xor:"body_source"`
}

type app struct {
	stdout io.Writer
	stderr io.Writer
}

type emitter struct {
	out  io.Writer
	json bool
	mu   sync.Mutex
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cli := CLI{}
	parser, err := kong.New(
		&cli,
		kong.Name("turntf-client"),
		kong.Description("TurnTF test and demo CLI client."),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
	)
	if err != nil {
		return err
	}
	parseCtx, err := parser.Parse(args)
	if err != nil {
		return err
	}
	return (&app{stdout: stdout, stderr: stderr}).execute(ctx, parseCtx.Command(), &cli)
}

func (a *app) execute(ctx context.Context, command string, cli *CLI) error {
	switch command {
	case "listen":
		return a.runListen(ctx, cli)
	case "send-message":
		return a.runSendMessage(ctx, cli)
	case "send-packet":
		return a.runSendPacket(ctx, cli)
	case "login":
		return a.runLogin(ctx, cli)
	case "create-user":
		return a.runCreateUser(ctx, cli)
	case "create-channel":
		return a.runCreateChannel(ctx, cli)
	case "subscribe":
		return a.runSubscribe(ctx, cli)
	case "list-messages":
		return a.runListMessages(ctx, cli)
	case "get-user":
		return a.runGetUser(ctx, cli)
	case "update-user":
		return a.runUpdateUser(ctx, cli)
	case "delete-user":
		return a.runDeleteUser(ctx, cli)
	case "list-subscriptions":
		return a.runListSubscriptions(ctx, cli)
	case "list-events":
		return a.runListEvents(ctx, cli)
	case "ops-status":
		return a.runOpsStatus(ctx, cli)
	case "metrics":
		return a.runMetrics(ctx, cli)
	case "demo":
		return a.runDemo(ctx, cli)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func (a *app) runListen(ctx context.Context, cli *CLI) error {
	creds, err := cli.userCredentials()
	if err != nil {
		return err
	}
	out := &emitter{out: a.stdout, json: cli.JSON}
	handler := newEventHandler(out)
	client, err := cli.newWSClient(creds, handler)
	if err != nil {
		return err
	}
	defer client.Close()

	listenCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := client.Connect(listenCtx); err != nil {
		return err
	}

	select {
	case err := <-handler.disconnectCh:
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-listenCtx.Done():
		return nil
	}
}

func (a *app) runSendMessage(ctx context.Context, cli *CLI) error {
	creds, err := cli.userCredentials()
	if err != nil {
		return err
	}
	body, err := cli.SendMessage.BodyInput.bytes()
	if err != nil {
		return err
	}
	client, err := cli.newWSClient(creds, turntf.NopHandler{})
	if err != nil {
		return err
	}
	defer client.Close()

	opCtx, cancel := context.WithTimeout(ctx, cli.Timeout)
	defer cancel()
	if err := client.Connect(opCtx); err != nil {
		return err
	}

	msg, err := client.SendMessage(opCtx, turntf.SendMessageInput{
		Target: turntf.UserRef{NodeID: cli.SendMessage.TargetNodeID, UserID: cli.SendMessage.TargetUserID},
		Sender: cli.SendMessage.Sender,
		Body:   body,
	})
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, msg)
}

func (a *app) runSendPacket(ctx context.Context, cli *CLI) error {
	creds, err := cli.userCredentials()
	if err != nil {
		return err
	}
	body, err := cli.SendPacket.BodyInput.bytes()
	if err != nil {
		return err
	}
	client, err := cli.newWSClient(creds, turntf.NopHandler{})
	if err != nil {
		return err
	}
	defer client.Close()

	opCtx, cancel := context.WithTimeout(ctx, cli.Timeout)
	defer cancel()
	if err := client.Connect(opCtx); err != nil {
		return err
	}

	relay, err := client.SendPacket(opCtx, turntf.SendPacketInput{
		Target:       turntf.UserRef{NodeID: cli.SendPacket.TargetNodeID, UserID: cli.SendPacket.TargetUserID},
		Sender:       cli.SendPacket.Sender,
		Body:         body,
		DeliveryMode: turntf.DeliveryMode(cli.SendPacket.Mode),
	})
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, relay)
}

func (a *app) runLogin(ctx context.Context, cli *CLI) error {
	httpClient := turntf.NewHTTPClient(cli.BaseURL)
	creds, err := cli.adminCredentials()
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithTimeout(ctx, cli.Timeout)
	defer cancel()

	token, err := httpClient.Login(opCtx, creds.NodeID, creds.UserID, creds.Password)
	if err != nil {
		return err
	}
	if cli.JSON {
		return writeResult(a.stdout, true, map[string]string{"token": token})
	}
	_, err = fmt.Fprintln(a.stdout, token)
	return err
}

func (a *app) runCreateUser(ctx context.Context, cli *CLI) error {
	return a.createUser(ctx, cli, turntf.CreateUserRequest{
		Username:    cli.CreateUser.Username,
		Password:    cli.CreateUser.NewPassword,
		ProfileJSON: []byte(cli.CreateUser.ProfileJSON),
		Role:        cli.CreateUser.Role,
	})
}

func (a *app) runCreateChannel(ctx context.Context, cli *CLI) error {
	return a.createUser(ctx, cli, turntf.CreateUserRequest{
		Username:    cli.CreateChannel.Username,
		ProfileJSON: []byte(cli.CreateChannel.ProfileJSON),
		Role:        "channel",
	})
}

func (a *app) createUser(ctx context.Context, cli *CLI, req turntf.CreateUserRequest) error {
	httpClient := turntf.NewHTTPClient(cli.BaseURL)
	token, err := a.adminToken(ctx, cli)
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithTimeout(ctx, cli.Timeout)
	defer cancel()

	user, err := httpClient.CreateUser(opCtx, token, req)
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, user)
}

func (a *app) runSubscribe(ctx context.Context, cli *CLI) error {
	httpClient := turntf.NewHTTPClient(cli.BaseURL)
	token, err := a.adminToken(ctx, cli)
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithTimeout(ctx, cli.Timeout)
	defer cancel()
	subscriber := turntf.UserRef{NodeID: cli.Subscribe.SubscriberNodeID, UserID: cli.Subscribe.SubscriberUserID}
	channel := turntf.UserRef{NodeID: cli.Subscribe.ChannelNodeID, UserID: cli.Subscribe.ChannelUserID}
	if err := httpClient.CreateSubscription(opCtx, token, subscriber, channel); err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, map[string]any{
		"status":     "ok",
		"subscriber": subscriber,
		"channel":    channel,
	})
}

func (a *app) runListMessages(ctx context.Context, cli *CLI) error {
	client, opCtx, cancel, err := cli.adminWSClientWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Close()

	items, err := client.ListMessages(opCtx, "", turntf.UserRef{NodeID: cli.ListMessages.TargetNodeID, UserID: cli.ListMessages.TargetUserID}, cli.ListMessages.Limit)
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, items)
}

func (a *app) runGetUser(ctx context.Context, cli *CLI) error {
	client, opCtx, cancel, err := cli.adminWSClientWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Close()

	user, err := client.GetUser(opCtx, turntf.UserRef{NodeID: cli.GetUser.TargetNodeID, UserID: cli.GetUser.TargetUserID})
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, user)
}

func (a *app) runUpdateUser(ctx context.Context, cli *CLI) error {
	client, opCtx, cancel, err := cli.adminWSClientWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Close()

	req := turntf.UpdateUserRequest{}
	if cli.UpdateUser.Username != "" {
		req.Username = &cli.UpdateUser.Username
	}
	if cli.UpdateUser.NewPassword != "" {
		req.Password = &cli.UpdateUser.NewPassword
	}
	if cli.UpdateUser.Role != "" {
		req.Role = &cli.UpdateUser.Role
	}
	if cli.UpdateUser.ProfileJSON != "" {
		profile := []byte(cli.UpdateUser.ProfileJSON)
		req.ProfileJSON = &profile
	}
	user, err := client.UpdateUser(opCtx, turntf.UserRef{NodeID: cli.UpdateUser.TargetNodeID, UserID: cli.UpdateUser.TargetUserID}, req)
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, user)
}

func (a *app) runDeleteUser(ctx context.Context, cli *CLI) error {
	client, opCtx, cancel, err := cli.adminWSClientWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Close()

	result, err := client.DeleteUser(opCtx, turntf.UserRef{NodeID: cli.DeleteUser.TargetNodeID, UserID: cli.DeleteUser.TargetUserID})
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, result)
}

func (a *app) runListSubscriptions(ctx context.Context, cli *CLI) error {
	client, opCtx, cancel, err := cli.adminWSClientWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Close()

	items, err := client.ListSubscriptions(opCtx, turntf.UserRef{NodeID: cli.ListSubscriptions.SubscriberNodeID, UserID: cli.ListSubscriptions.SubscriberUserID})
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, items)
}

func (a *app) runListEvents(ctx context.Context, cli *CLI) error {
	client, opCtx, cancel, err := cli.adminWSClientWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Close()

	items, err := client.ListEvents(opCtx, cli.ListEvents.After, cli.ListEvents.Limit)
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, items)
}

func (a *app) runOpsStatus(ctx context.Context, cli *CLI) error {
	client, opCtx, cancel, err := cli.adminWSClientWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Close()

	status, err := client.OperationsStatus(opCtx)
	if err != nil {
		return err
	}
	return writeResult(a.stdout, cli.JSON, status)
}

func (a *app) runMetrics(ctx context.Context, cli *CLI) error {
	client, opCtx, cancel, err := cli.adminWSClientWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Close()

	text, err := client.Metrics(opCtx)
	if err != nil {
		return err
	}
	if cli.JSON {
		return writeResult(a.stdout, true, map[string]string{"text": text})
	}
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	_, err = io.WriteString(a.stdout, text)
	return err
}

func (a *app) runDemo(ctx context.Context, cli *CLI) error {
	adminToken, err := a.adminToken(ctx, cli)
	if err != nil {
		return err
	}
	httpClient := turntf.NewHTTPClient(cli.BaseURL)
	opCtx, cancel := context.WithTimeout(ctx, cli.Timeout)
	defer cancel()

	user, err := httpClient.CreateUser(opCtx, adminToken, turntf.CreateUserRequest{
		Username: cli.Demo.UserName,
		Password: cli.Demo.UserPassword,
		Role:     "user",
	})
	if err != nil {
		return fmt.Errorf("create demo user: %w", err)
	}

	var channel turntf.User
	if cli.Demo.ChannelName != "" {
		channel, err = httpClient.CreateChannel(opCtx, adminToken, turntf.CreateUserRequest{
			Username: cli.Demo.ChannelName,
			Role:     "channel",
		})
		if err != nil {
			return fmt.Errorf("create demo channel: %w", err)
		}
		if err := httpClient.CreateSubscription(opCtx, adminToken, turntf.UserRef{NodeID: user.NodeID, UserID: user.UserID}, turntf.UserRef{NodeID: channel.NodeID, UserID: channel.UserID}); err != nil {
			return fmt.Errorf("create demo subscription: %w", err)
		}
	}

	out := &emitter{out: a.stdout, json: cli.JSON}
	_ = out.emit("demo_user", user)
	if channel.UserID != 0 {
		_ = out.emit("demo_channel", channel)
	}

	listenHandler := newEventHandler(out)
	listener, err := cli.newWSClient(turntf.Credentials{
		NodeID:   user.NodeID,
		UserID:   user.UserID,
		Password: cli.Demo.UserPassword,
	}, listenHandler)
	if err != nil {
		return err
	}
	defer listener.Close()

	listenCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := listener.Connect(listenCtx); err != nil {
		return fmt.Errorf("connect demo listener: %w", err)
	}

	adminClient, err := cli.newWSClientMustAdmin(turntf.NopHandler{})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	adminWSCtx, adminCancel := context.WithTimeout(ctx, cli.Timeout)
	defer adminCancel()
	if err := adminClient.Connect(adminWSCtx); err != nil {
		return fmt.Errorf("connect admin websocket: %w", err)
	}

	message, err := adminClient.SendMessage(adminWSCtx, turntf.SendMessageInput{
		Target: turntf.UserRef{NodeID: user.NodeID, UserID: user.UserID},
		Sender: cli.Demo.MessageSender,
		Body:   []byte(cli.Demo.MessageBody),
	})
	if err != nil {
		return fmt.Errorf("send demo message: %w", err)
	}
	_ = out.emit("demo_send_message", message)

	if cli.Demo.SendPacket {
		packet, err := adminClient.SendPacket(adminWSCtx, turntf.SendPacketInput{
			Target:       turntf.UserRef{NodeID: user.NodeID, UserID: user.UserID},
			Sender:       cli.Demo.MessageSender,
			Body:         []byte(cli.Demo.MessageBody),
			DeliveryMode: turntf.DeliveryMode(cli.Demo.PacketMode),
		})
		if err != nil {
			return fmt.Errorf("send demo packet: %w", err)
		}
		_ = out.emit("demo_send_packet", packet)
	}

	select {
	case err := <-listenHandler.disconnectCh:
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-listenCtx.Done():
		return nil
	}
}

func (a *app) adminToken(ctx context.Context, cli *CLI) (string, error) {
	httpClient := turntf.NewHTTPClient(cli.BaseURL)
	creds, err := cli.adminCredentials()
	if err != nil {
		return "", err
	}
	opCtx, cancel := context.WithTimeout(ctx, cli.Timeout)
	defer cancel()
	return httpClient.Login(opCtx, creds.NodeID, creds.UserID, creds.Password)
}

func (cli *CLI) newWSClient(creds turntf.Credentials, handler turntf.Handler) (*turntf.Client, error) {
	return turntf.NewClient(turntf.Config{
		BaseURL:               cli.BaseURL,
		Credentials:           creds,
		CursorStore:           turntf.NewMemoryCursorStore(),
		Handler:               handler,
		InitialReconnectDelay: cli.InitialReconnect,
		MaxReconnectDelay:     cli.MaxReconnect,
		PingInterval:          cli.PingInterval,
		RequestTimeout:        cli.Timeout,
	})
}

func (cli *CLI) adminWSClientWithTimeout(ctx context.Context) (*turntf.Client, context.Context, context.CancelFunc, error) {
	creds, err := cli.adminCredentials()
	if err != nil {
		return nil, nil, nil, err
	}
	client, err := cli.newWSClient(creds, turntf.NopHandler{})
	if err != nil {
		return nil, nil, nil, err
	}
	opCtx, cancel := context.WithTimeout(ctx, cli.Timeout)
	if err := client.Connect(opCtx); err != nil {
		cancel()
		client.Close()
		return nil, nil, nil, err
	}
	return client, opCtx, cancel, nil
}

func (cli *CLI) newWSClientMustAdmin(handler turntf.Handler) (*turntf.Client, error) {
	creds, err := cli.adminCredentials()
	if err != nil {
		return nil, err
	}
	return cli.newWSClient(creds, handler)
}

func (cli *CLI) userCredentials() (turntf.Credentials, error) {
	if cli.NodeID == 0 || cli.UserID == 0 || cli.Password == "" {
		return turntf.Credentials{}, fmt.Errorf("client credentials are required: set --node-id, --user-id and --password or TURNTF_NODE_ID/TURNTF_USER_ID/TURNTF_PASSWORD")
	}
	return turntf.Credentials{NodeID: cli.NodeID, UserID: cli.UserID, Password: cli.Password}, nil
}

func (cli *CLI) adminCredentials() (turntf.Credentials, error) {
	if cli.AdminNodeID == 0 || cli.AdminUserID == 0 || cli.AdminPassword == "" {
		return turntf.Credentials{}, fmt.Errorf("admin credentials are required: set --admin-node-id, --admin-user-id and --admin-password or TURNTF_ADMIN_NODE_ID/TURNTF_ADMIN_USER_ID/TURNTF_ADMIN_PASSWORD")
	}
	return turntf.Credentials{NodeID: cli.AdminNodeID, UserID: cli.AdminUserID, Password: cli.AdminPassword}, nil
}

func (b BodyInput) bytes() ([]byte, error) {
	switch {
	case b.Body != "":
		return []byte(b.Body), nil
	case b.BodyHex != "":
		data, err := hex.DecodeString(b.BodyHex)
		if err != nil {
			return nil, fmt.Errorf("decode --body-hex: %w", err)
		}
		return data, nil
	case b.BodyFile != "":
		data, err := os.ReadFile(b.BodyFile)
		if err != nil {
			return nil, fmt.Errorf("read --body-file: %w", err)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("one of --body, --body-hex or --body-file is required")
	}
}

func writeResult(w io.Writer, asJSON bool, value any) error {
	if asJSON {
		return writeJSON(w, value)
	}
	return writeJSON(w, value)
}

func writeJSON(w io.Writer, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s\n", data)
	return err
}

func newEventHandler(out *emitter) *eventHandler {
	return &eventHandler{
		out:          out,
		disconnectCh: make(chan error, 1),
	}
}

type eventHandler struct {
	out          *emitter
	disconnectCh chan error
}

func (h *eventHandler) OnLogin(_ context.Context, info turntf.LoginInfo) {
	_ = h.out.emit("login", map[string]any{
		"user":             info.User,
		"protocol_version": info.ProtocolVersion,
	})
}

func (h *eventHandler) OnMessage(_ context.Context, msg turntf.Message) {
	_ = h.out.emit("message", msg)
}

func (h *eventHandler) OnPacket(_ context.Context, packet turntf.Packet) {
	_ = h.out.emit("packet", packet)
}

func (h *eventHandler) OnError(_ context.Context, err error) {
	_ = h.out.emit("error", map[string]string{"error": err.Error()})
}

func (h *eventHandler) OnDisconnect(_ context.Context, err error) {
	_ = h.out.emit("disconnect", map[string]string{"error": errorString(err)})
	select {
	case h.disconnectCh <- err:
	default:
	}
}

func (e *emitter) emit(kind string, payload any) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.json {
		return writeJSON(e.out, map[string]any{
			"time":    time.Now().Format(time.RFC3339),
			"type":    kind,
			"payload": payload,
		})
	}

	line := humanLine(kind, payload)
	_, err := fmt.Fprintln(e.out, line)
	return err
}

func humanLine(kind string, payload any) string {
	now := time.Now().Format(time.RFC3339)
	switch kind {
	case "login":
		if data, ok := payload.(map[string]any); ok {
			if user, ok := data["user"].(turntf.User); ok {
				return fmt.Sprintf("%s login user=%d:%d username=%s role=%s protocol=%v", now, user.NodeID, user.UserID, user.Username, user.Role, data["protocol_version"])
			}
		}
	case "message":
		if msg, ok := payload.(turntf.Message); ok {
			return fmt.Sprintf("%s message target=%d:%d cursor=%d:%d sender=%s body=%q", now, msg.UserNodeID, msg.UserID, msg.NodeID, msg.Seq, msg.Sender, string(msg.Body))
		}
	case "packet":
		if packet, ok := payload.(turntf.Packet); ok {
			return fmt.Sprintf("%s packet packet_id=%d recipient=%d:%d sender=%s mode=%s body=%q", now, packet.PacketID, packet.Recipient.NodeID, packet.Recipient.UserID, packet.Sender, packet.DeliveryMode, string(packet.Body))
		}
	case "disconnect":
		if data, ok := payload.(map[string]string); ok {
			return fmt.Sprintf("%s disconnect error=%s", now, data["error"])
		}
	case "error":
		if data, ok := payload.(map[string]string); ok {
			return fmt.Sprintf("%s error error=%s", now, data["error"])
		}
	case "demo_user":
		if user, ok := payload.(turntf.User); ok {
			return fmt.Sprintf("%s demo-user node=%d user=%d username=%s", now, user.NodeID, user.UserID, user.Username)
		}
	case "demo_channel":
		if user, ok := payload.(turntf.User); ok {
			return fmt.Sprintf("%s demo-channel node=%d user=%d username=%s", now, user.NodeID, user.UserID, user.Username)
		}
	case "demo_send_message":
		if msg, ok := payload.(turntf.Message); ok {
			return fmt.Sprintf("%s demo-send-message cursor=%d:%d sender=%s body=%q", now, msg.NodeID, msg.Seq, msg.Sender, string(msg.Body))
		}
	case "demo_send_packet":
		if packet, ok := payload.(turntf.RelayAccepted); ok {
			return fmt.Sprintf("%s demo-send-packet packet_id=%d recipient=%d:%d mode=%s", now, packet.PacketID, packet.Recipient.NodeID, packet.Recipient.UserID, packet.DeliveryMode)
		}
	}

	data, _ := json.Marshal(payload)
	return fmt.Sprintf("%s %s %s", now, kind, string(data))
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
