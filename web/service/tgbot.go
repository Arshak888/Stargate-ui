package service

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mhsanaei/3x-ui/v2/config"
	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/mhsanaei/3x-ui/v2/util/common"
	"github.com/mhsanaei/3x-ui/v2/web/global"
	"github.com/mhsanaei/3x-ui/v2/web/locale"
	"github.com/mhsanaei/3x-ui/v2/xray"

	"github.com/google/uuid"
	"github.com/mymmrac/telego"
	th "github.com/mymmrac/telego/telegohandler"
	tu "github.com/mymmrac/telego/telegoutil"
	"github.com/skip2/go-qrcode"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpproxy"
	"gorm.io/gorm"
)

var (
	bot *telego.Bot

	// botCancel stores the function to cancel the context, stopping Long Polling gracefully.
	botCancel context.CancelFunc
	// tgBotMutex protects concurrent access to botCancel variable
	tgBotMutex sync.Mutex
	// botWG waits for the OnReceive Long Polling goroutine to finish.
	botWG sync.WaitGroup

	botHandler  *th.BotHandler
	adminIds    []int64
	isRunning   bool
	hostname    string
	hashStorage *global.HashStorage

	// Performance improvements
	messageWorkerPool   chan struct{} // Semaphore for limiting concurrent message processing
	optimizedHTTPClient *http.Client  // HTTP client with connection pooling and timeouts

	// Simple cache for frequently accessed data
	statusCache struct {
		data      *Status
		timestamp time.Time
		mutex     sync.RWMutex
	}

	serverStatsCache struct {
		data      string
		timestamp time.Time
		mutex     sync.RWMutex
	}

	// clients data to adding new client
	receiver_inbound_ID int
	client_Id           string
	client_Flow         string
	client_Email        string
	client_LimitIP      int
	client_TotalGB      int64
	client_ExpiryTime   int64
	client_Enable       bool
	client_TgID         string
	client_SubID        string
	client_Comment      string
	client_Reset        int
	client_Security     string
	client_ShPassword   string
	client_TrPassword   string
	client_Method       string
)

var userStates = make(map[int64]string)

type resellerFlow struct {
	Stage     string
	InboundID int
	Email     string
	GB        int
	Days      int
}

var resellerFlows sync.Map

// LoginStatus represents the result of a login attempt.
type LoginStatus byte

// Login status constants
const (
	LoginSuccess        LoginStatus = 1        // Login was successful
	LoginFail           LoginStatus = 0        // Login failed
	EmptyTelegramUserID             = int64(0) // Default value for empty Telegram user ID
)

// Tgbot provides business logic for Telegram bot integration.
// It handles bot commands, user interactions, and status reporting via Telegram.
type Tgbot struct {
	inboundService  InboundService
	resellerService ResellerService
	settingService  SettingService
	serverService   ServerService
	xrayService     XrayService
	lastStatus      *Status
}

// NewTgbot creates a new Tgbot instance.
func (t *Tgbot) NewTgbot() *Tgbot {
	return new(Tgbot)
}

// I18nBot retrieves a localized message for the bot interface.
func (t *Tgbot) I18nBot(name string, params ...string) string {
	return locale.I18n(locale.Bot, name, params...)
}

// GetHashStorage returns the hash storage instance for callback queries.
func (t *Tgbot) GetHashStorage() *global.HashStorage {
	return hashStorage
}

// getCachedStatus returns cached server status if it's fresh enough (less than 5 seconds old)
func (t *Tgbot) getCachedStatus() (*Status, bool) {
	statusCache.mutex.RLock()
	defer statusCache.mutex.RUnlock()

	if statusCache.data != nil && time.Since(statusCache.timestamp) < 5*time.Second {
		return statusCache.data, true
	}
	return nil, false
}

// setCachedStatus updates the status cache
func (t *Tgbot) setCachedStatus(status *Status) {
	statusCache.mutex.Lock()
	defer statusCache.mutex.Unlock()

	statusCache.data = status
	statusCache.timestamp = time.Now()
}

// getCachedServerStats returns cached server stats if it's fresh enough (less than 10 seconds old)
func (t *Tgbot) getCachedServerStats() (string, bool) {
	serverStatsCache.mutex.RLock()
	defer serverStatsCache.mutex.RUnlock()

	if serverStatsCache.data != "" && time.Since(serverStatsCache.timestamp) < 10*time.Second {
		return serverStatsCache.data, true
	}
	return "", false
}

// setCachedServerStats updates the server stats cache
func (t *Tgbot) setCachedServerStats(stats string) {
	serverStatsCache.mutex.Lock()
	defer serverStatsCache.mutex.Unlock()

	serverStatsCache.data = stats
	serverStatsCache.timestamp = time.Now()
}

// Start initializes and starts the Telegram bot with the provided translation files.
func (t *Tgbot) Start(i18nFS embed.FS) error {
	// Initialize localizer
	err := locale.InitLocalizer(i18nFS, &t.settingService)
	if err != nil {
		return err
	}

	// If Start is called again (e.g. during reload), ensure any previous long-polling
	// loop is stopped before creating a new bot / receiver.
	StopBot()

	// Initialize hash storage to store callback queries
	hashStorage = global.NewHashStorage(20 * time.Minute)

	// Initialize worker pool for concurrent message processing (max 10 concurrent handlers)
	messageWorkerPool = make(chan struct{}, 10)

	// Initialize optimized HTTP client with connection pooling
	optimizedHTTPClient = &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
			DisableKeepAlives:   false,
		},
	}

	t.SetHostname()

	// Get Telegram bot token
	tgBotToken, err := t.settingService.GetTgBotToken()
	if err != nil || tgBotToken == "" {
		logger.Warning("Failed to get Telegram bot token:", err)
		return err
	}

	// Get Telegram bot chat ID(s)
	tgBotID, err := t.settingService.GetTgBotChatId()
	if err != nil {
		logger.Warning("Failed to get Telegram bot chat ID:", err)
		return err
	}

	parsedAdminIds := make([]int64, 0)
	// Parse admin IDs from comma-separated string
	if tgBotID != "" {
		for _, adminID := range strings.Split(tgBotID, ",") {
			id, err := strconv.ParseInt(adminID, 10, 64)
			if err != nil {
				logger.Warning("Failed to parse admin ID from Telegram bot chat ID:", err)
				return err
			}
			parsedAdminIds = append(parsedAdminIds, int64(id))
		}
	}
	tgBotMutex.Lock()
	adminIds = parsedAdminIds
	tgBotMutex.Unlock()

	// Get Telegram bot proxy URL
	tgBotProxy, err := t.settingService.GetTgBotProxy()
	if err != nil {
		logger.Warning("Failed to get Telegram bot proxy URL:", err)
	}

	// Get Telegram bot API server URL
	tgBotAPIServer, err := t.settingService.GetTgBotAPIServer()
	if err != nil {
		logger.Warning("Failed to get Telegram bot API server URL:", err)
	}

	// Create new Telegram bot instance
	bot, err = t.NewBot(tgBotToken, tgBotProxy, tgBotAPIServer)
	if err != nil {
		logger.Error("Failed to initialize Telegram bot API:", err)
		return err
	}

	t.trySetBotCommands(bot)

	// Start receiving Telegram bot messages
	tgBotMutex.Lock()
	alreadyRunning := isRunning || botCancel != nil
	tgBotMutex.Unlock()
	if !alreadyRunning {
		logger.Info("Telegram bot receiver started")
		go t.OnReceive()
	}

	return nil
}

func (t *Tgbot) trySetBotCommands(bot *telego.Bot) {
	defer func() {
		if r := recover(); r != nil {
			logger.Warning("Failed to register bot commands (Telegram may be rate-limiting); bot will continue without them:", r)
		}
	}()

	err := bot.SetMyCommands(context.Background(), &telego.SetMyCommandsParams{
		Commands: []telego.BotCommand{
			{Command: "start", Description: t.I18nBot("tgbot.commands.startDesc")},
			{Command: "help", Description: t.I18nBot("tgbot.commands.helpDesc")},
			{Command: "status", Description: t.I18nBot("tgbot.commands.statusDesc")},
			{Command: "id", Description: t.I18nBot("tgbot.commands.idDesc")},
			{Command: "reseller", Description: "Open reseller panel"},
			{Command: "balance", Description: "Reseller balance"},
			{Command: "clients", Description: "My reseller accounts"},
			{Command: "inbounds", Description: "My reseller inbounds"},
			{Command: "create", Description: "Create reseller account"},
			{Command: "renew", Description: "Renew reseller account"},
			{Command: "delete", Description: "Delete reseller account"},
			{Command: "reset", Description: "Reset account traffic"},
			{Command: "enable", Description: "Enable reseller account"},
			{Command: "disable", Description: "Disable reseller account"},
		},
	})
	if err != nil {
		logger.Warning("Failed to set bot commands:", err)
	}
}

// createRobustFastHTTPClient creates a fasthttp.Client with proper connection handling
func (t *Tgbot) createRobustFastHTTPClient(proxyUrl string) *fasthttp.Client {
	client := &fasthttp.Client{
		// Connection timeouts
		ReadTimeout:                   30 * time.Second,
		WriteTimeout:                  30 * time.Second,
		MaxIdleConnDuration:           60 * time.Second,
		MaxConnDuration:               0, // unlimited, but controlled by MaxIdleConnDuration
		MaxIdemponentCallAttempts:     3,
		ReadBufferSize:                4096,
		WriteBufferSize:               4096,
		MaxConnsPerHost:               100,
		MaxConnWaitTimeout:            10 * time.Second,
		DisableHeaderNamesNormalizing: false,
		DisablePathNormalizing:        false,
		// Retry on connection errors
		RetryIf: func(request *fasthttp.Request) bool {
			// Retry on connection errors for GET requests
			return string(request.Header.Method()) == "GET" || string(request.Header.Method()) == "POST"
		},
	}

	// Set proxy if provided
	if proxyUrl != "" {
		client.Dial = fasthttpproxy.FasthttpSocksDialer(proxyUrl)
	}

	return client
}

// NewBot creates a new Telegram bot instance with optional proxy and API server settings.
func (t *Tgbot) NewBot(token string, proxyUrl string, apiServerUrl string) (*telego.Bot, error) {
	// Validate proxy URL if provided
	if proxyUrl != "" {
		if !strings.HasPrefix(proxyUrl, "socks5://") {
			logger.Warning("Invalid socks5 URL, ignoring proxy")
			proxyUrl = "" // Clear invalid proxy
		} else {
			_, err := url.Parse(proxyUrl)
			if err != nil {
				logger.Warningf("Can't parse proxy URL, ignoring proxy: %v", err)
				proxyUrl = ""
			}
		}
	}

	// Validate API server URL if provided
	if apiServerUrl != "" {
		if !strings.HasPrefix(apiServerUrl, "http") {
			logger.Warning("Invalid http(s) URL for API server, using default")
			apiServerUrl = ""
		} else {
			_, err := url.Parse(apiServerUrl)
			if err != nil {
				logger.Warningf("Can't parse API server URL, using default: %v", err)
				apiServerUrl = ""
			}
		}
	}

	// Create robust fasthttp client
	client := t.createRobustFastHTTPClient(proxyUrl)

	// Build bot options
	var options []telego.BotOption
	options = append(options, telego.WithFastHTTPClient(client))

	if apiServerUrl != "" {
		options = append(options, telego.WithAPIServer(apiServerUrl))
	}

	return telego.NewBot(token, options...)
}

// SendExpiryReminders sends one Telegram alert for each account at 48h, 24h and expiry.
// The account's TgID is the durable recipient identity, while expiry time is part of
// the reminder key so renewal starts a fresh reminder cycle.
func (t *Tgbot) SendExpiryReminders() {
	if !t.IsRunning() {
		return
	}
	var accounts []model.Account
	if err := database.GetDB().Where("tg_id <> 0 AND expiry_time > 0").Find(&accounts).Error; err != nil {
		logger.Warning("Telegram expiry reminders: failed to load accounts:", err)
		return
	}
	now := time.Now()
	settings, _ := t.settingService.GetAllSetting()
	firstDays, secondDays, notifyExpired := 2, 1, true
	if settings != nil {
		if settings.TgExpiryReminder2Days > 0 {
			firstDays = settings.TgExpiryReminder2Days
		}
		if settings.TgExpiryReminder1Day > 0 {
			secondDays = settings.TgExpiryReminder1Day
		}
		notifyExpired = settings.TgExpiryReminderOnExpire
	}
	if firstDays < secondDays {
		firstDays, secondDays = secondDays, firstDays
	}
	for _, account := range accounts {
		remaining := time.Until(time.UnixMilli(account.ExpiryTime))
		event := ""
		firstWindow := time.Duration(firstDays) * 24 * time.Hour
		secondWindow := time.Duration(secondDays) * 24 * time.Hour
		switch {
		case remaining > secondWindow && remaining <= firstWindow:
			event = "2d"
		case remaining > 0 && remaining <= secondWindow:
			event = "1d"
		case remaining <= 0 && notifyExpired:
			event = "expired"
		}
		if event == "" {
			continue
		}
		var state model.TelegramReminderState
		err := database.GetDB().Where("email = ? AND tg_id = ? AND expiry_time = ? AND event = ?",
			account.Email, account.TgID, account.ExpiryTime, event).First(&state).Error
		if err == nil {
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Warning("Telegram expiry reminders: failed to check state:", err)
			continue
		}
		var traffic xray.ClientTraffic
		database.GetDB().Where("email = ?", account.Email).First(&traffic)
		used := traffic.Up + traffic.Down
		remainingTraffic := account.TotalGB - used
		if remainingTraffic < 0 {
			remainingTraffic = 0
		}
		msg := fmt.Sprintf("Account: %s\nExpiry: %s\nUsed traffic: %s\nRemaining traffic: %s",
			account.Email,
			time.UnixMilli(account.ExpiryTime).Format("2006-01-02 15:04:05"),
			fmt.Sprintf("%d bytes", used),
			fmt.Sprintf("%d bytes", remainingTraffic))
		switch event {
		case "2d":
			msg = fmt.Sprintf("⏰ %d days remaining\n%s", firstDays, msg)
		case "1d":
			msg = fmt.Sprintf("⚠️ %d days remaining\n%s", secondDays, msg)
		case "expired":
			msg = "❌ Account expired\n" + msg
		}
		if err := t.SendMsgToTgbot(account.TgID, msg); err != nil {
			logger.Warning("Telegram expiry reminder send failed:", err)
			continue
		}
		state = model.TelegramReminderState{
			Email: account.Email, TgID: account.TgID,
			ExpiryTime: account.ExpiryTime, Event: event, SentAt: now.Unix(),
		}
		if err := database.GetDB().Create(&state).Error; err != nil {
			logger.Warning("Telegram expiry reminders: failed to persist state:", err)
		}
	}
}

// IsRunning checks if the Telegram bot is currently running.
func (t *Tgbot) IsRunning() bool {
	tgBotMutex.Lock()
	defer tgBotMutex.Unlock()
	return isRunning
}

// SetHostname sets the hostname for the bot.
func (t *Tgbot) SetHostname() {
	host, err := os.Hostname()
	if err != nil {
		logger.Error("get hostname error:", err)
		hostname = ""
		return
	}
	hostname = host
}

// Stop safely stops the Telegram bot's Long Polling operation.
// This method now calls the global StopBot function and cleans up other resources.
func (t *Tgbot) Stop() {
	StopBot()
	logger.Info("Stop Telegram receiver ...")
	tgBotMutex.Lock()
	adminIds = nil
	tgBotMutex.Unlock()
}

// StopBot safely stops the Telegram bot's Long Polling operation by cancelling its context.
// This is the global function called from main.go's signal handler and t.Stop().
func StopBot() {
	// Don't hold the mutex while cancelling/waiting.
	tgBotMutex.Lock()
	cancel := botCancel
	botCancel = nil
	handler := botHandler
	botHandler = nil
	isRunning = false
	tgBotMutex.Unlock()

	if handler != nil {
		handler.Stop()
	}

	if cancel != nil {
		logger.Info("Sending cancellation signal to Telegram bot...")
		// Cancels the context passed to UpdatesViaLongPolling; this closes updates channel
		// and lets botHandler.Start() exit cleanly.
		cancel()
		botWG.Wait()
		logger.Info("Telegram bot successfully stopped.")
	}
}

// encodeQuery encodes the query string if it's longer than 64 characters.
func (t *Tgbot) encodeQuery(query string) string {
	// NOTE: we only need to hash for more than 64 chars
	if len(query) <= 64 {
		return query
	}

	return hashStorage.SaveHash(query)
}

// decodeQuery decodes a hashed query string back to its original form.
func (t *Tgbot) decodeQuery(query string) (string, error) {
	if !hashStorage.IsMD5(query) {
		return query, nil
	}

	decoded, exists := hashStorage.GetValue(query)
	if !exists {
		return "", common.NewError("hash not found in storage!")
	}

	return decoded, nil
}

// OnReceive starts the message receiving loop for the Telegram bot.
func (t *Tgbot) OnReceive() {
	params := telego.GetUpdatesParams{
		Timeout: 20, // Reduced timeout to detect connection issues faster
	}
	// Strict singleton: never start a second long-polling loop.
	tgBotMutex.Lock()
	if botCancel != nil || isRunning {
		tgBotMutex.Unlock()
		logger.Warning("TgBot OnReceive called while already running; ignoring.")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	botCancel = cancel
	isRunning = true
	// Add to WaitGroup before releasing the lock so StopBot() can't return
	// before this receiver goroutine is accounted for.
	botWG.Add(1)
	tgBotMutex.Unlock()

	// Get updates channel using the context with shorter timeout for better error recovery
	updates, _ := bot.UpdatesViaLongPolling(ctx, &params)
	go func() {
		defer botWG.Done()
		h, _ := th.NewBotHandler(bot, updates)
		tgBotMutex.Lock()
		botHandler = h
		tgBotMutex.Unlock()

		h.HandleMessage(func(ctx *th.Context, message telego.Message) error {
			delete(userStates, message.Chat.ID)
			t.SendMsgToTgbot(message.Chat.ID, t.I18nBot("tgbot.keyboardClosed"), tu.ReplyKeyboardRemove())
			return nil
		}, th.TextEqual(t.I18nBot("tgbot.buttons.closeKeyboard")))

		h.HandleMessage(func(ctx *th.Context, message telego.Message) error {
			// Use goroutine with worker pool for concurrent command processing
			go func() {
				messageWorkerPool <- struct{}{}        // Acquire worker
				defer func() { <-messageWorkerPool }() // Release worker

				delete(userStates, message.Chat.ID)
				t.answerCommand(&message, message.Chat.ID, checkAdmin(message.From.ID))
			}()
			return nil
		}, th.AnyCommand())

		h.HandleCallbackQuery(func(ctx *th.Context, query telego.CallbackQuery) error {
			// Use goroutine with worker pool for concurrent callback processing
			go func() {
				messageWorkerPool <- struct{}{}        // Acquire worker
				defer func() { <-messageWorkerPool }() // Release worker

				delete(userStates, query.Message.GetChat().ID)
				t.answerCallback(&query, checkAdmin(query.From.ID))
			}()
			return nil
		}, th.AnyCallbackQueryWithMessage())

		h.HandleMessage(func(ctx *th.Context, message telego.Message) error {
			if t.handleResellerText(&message) {
				return nil
			}
			if userState, exists := userStates[message.Chat.ID]; exists {
				switch userState {
				case "awaiting_id":
					if client_Id == strings.TrimSpace(message.Text) {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.using_default_value"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						inbound, _ := t.inboundService.GetInbound(receiver_inbound_ID)
						message_text, _ := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
						t.addClient(message.Chat.ID, message_text)
						return nil
					}

					client_Id = strings.TrimSpace(message.Text)
					if t.isSingleWord(client_Id) {
						userStates[message.Chat.ID] = "awaiting_id"

						cancel_btn_markup := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
							),
						)

						t.SendMsgToTgbot(message.Chat.ID, t.I18nBot("tgbot.messages.incorrect_input"), cancel_btn_markup)
					} else {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.received_id"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						inbound, _ := t.inboundService.GetInbound(receiver_inbound_ID)
						message_text, _ := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
						t.addClient(message.Chat.ID, message_text)
					}
				case "awaiting_password_tr":
					if client_TrPassword == strings.TrimSpace(message.Text) {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.using_default_value"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						return nil
					}

					client_TrPassword = strings.TrimSpace(message.Text)
					if t.isSingleWord(client_TrPassword) {
						userStates[message.Chat.ID] = "awaiting_password_tr"

						cancel_btn_markup := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
							),
						)

						t.SendMsgToTgbot(message.Chat.ID, t.I18nBot("tgbot.messages.incorrect_input"), cancel_btn_markup)
					} else {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.received_password"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						inbound, _ := t.inboundService.GetInbound(receiver_inbound_ID)
						message_text, _ := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
						t.addClient(message.Chat.ID, message_text)
					}
				case "awaiting_password_sh":
					if client_ShPassword == strings.TrimSpace(message.Text) {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.using_default_value"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						return nil
					}

					client_ShPassword = strings.TrimSpace(message.Text)
					if t.isSingleWord(client_ShPassword) {
						userStates[message.Chat.ID] = "awaiting_password_sh"

						cancel_btn_markup := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
							),
						)

						t.SendMsgToTgbot(message.Chat.ID, t.I18nBot("tgbot.messages.incorrect_input"), cancel_btn_markup)
					} else {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.received_password"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						inbound, _ := t.inboundService.GetInbound(receiver_inbound_ID)
						message_text, _ := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
						t.addClient(message.Chat.ID, message_text)
					}
				case "awaiting_email":
					if client_Email == strings.TrimSpace(message.Text) {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.using_default_value"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						return nil
					}

					client_Email = strings.TrimSpace(message.Text)
					if t.isSingleWord(client_Email) {
						userStates[message.Chat.ID] = "awaiting_email"

						cancel_btn_markup := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
							),
						)

						t.SendMsgToTgbot(message.Chat.ID, t.I18nBot("tgbot.messages.incorrect_input"), cancel_btn_markup)
					} else {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.received_email"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						inbound, _ := t.inboundService.GetInbound(receiver_inbound_ID)
						message_text, _ := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
						t.addClient(message.Chat.ID, message_text)
					}
				case "awaiting_comment":
					if client_Comment == strings.TrimSpace(message.Text) {
						t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.using_default_value"), 3, tu.ReplyKeyboardRemove())
						delete(userStates, message.Chat.ID)
						return nil
					}

					client_Comment = strings.TrimSpace(message.Text)
					t.SendMsgToTgbotDeleteAfter(message.Chat.ID, t.I18nBot("tgbot.messages.received_comment"), 3, tu.ReplyKeyboardRemove())
					delete(userStates, message.Chat.ID)
					inbound, _ := t.inboundService.GetInbound(receiver_inbound_ID)
					message_text, _ := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
					t.addClient(message.Chat.ID, message_text)
				}

			} else {
				if message.UsersShared != nil {
					if checkAdmin(message.From.ID) {
						for _, sharedUser := range message.UsersShared.Users {
							userID := sharedUser.UserID
							needRestart, err := t.inboundService.SetClientTelegramUserID(message.UsersShared.RequestID, userID)
							if needRestart {
								t.xrayService.SetToNeedRestart()
							}
							output := ""
							if err != nil {
								output += t.I18nBot("tgbot.messages.selectUserFailed")
							} else {
								output += t.I18nBot("tgbot.messages.userSaved")
							}
							t.SendMsgToTgbot(message.Chat.ID, output, tu.ReplyKeyboardRemove())
						}
					} else {
						t.SendMsgToTgbot(message.Chat.ID, t.I18nBot("tgbot.noResult"), tu.ReplyKeyboardRemove())
					}
				}
			}
			return nil
		}, th.AnyMessage())

		h.Start()
	}()
}

// answerCommand processes incoming command messages from Telegram users.
func (t *Tgbot) answerCommand(message *telego.Message, chatId int64, isAdmin bool) {
	msg, onlyMessage := "", false

	command, _, commandArgs := tu.ParseCommand(message.Text)

	// Reseller commands are checked by Telegram ID, independently of panel admin
	// authentication. A reseller never gets admin handlers or access to accounts they
	// do not own.
	if !isAdmin {
		if _, _, err := t.resellerService.ProfileForTelegram(message.From.ID); err == nil {
			onlyMessage = true
			if t.answerResellerCommand(chatId, message.From.ID, command, commandArgs) {
				return
			}
		}
	}

	// Helper function to handle unknown commands.
	handleUnknownCommand := func() {
		msg += t.I18nBot("tgbot.commands.unknown")
	}

	// Handle the command.
	switch command {
	case "help":
		msg += t.I18nBot("tgbot.commands.help")
		msg += t.I18nBot("tgbot.commands.pleaseChoose")
	case "start":
		msg += t.I18nBot("tgbot.commands.start", "Firstname=="+html.EscapeString(message.From.FirstName))
		if isAdmin {
			msg += t.I18nBot("tgbot.commands.welcome", "Hostname=="+hostname)
		}
		msg += "\n\n" + t.I18nBot("tgbot.commands.pleaseChoose")
	case "status":
		onlyMessage = true
		msg += t.I18nBot("tgbot.commands.status")
	case "id":
		onlyMessage = true
		msg += t.I18nBot("tgbot.commands.getID", "ID=="+strconv.FormatInt(message.From.ID, 10))
	case "usage":
		onlyMessage = true
		if len(commandArgs) > 0 {
			if isAdmin {
				t.searchClient(chatId, commandArgs[0])
			} else {
				t.getClientUsage(chatId, int64(message.From.ID), commandArgs[0])
			}
		} else {
			msg += t.I18nBot("tgbot.commands.usage")
		}
	case "inbound":
		onlyMessage = true
		if isAdmin && len(commandArgs) > 0 {
			t.searchInbound(chatId, commandArgs[0])
		} else {
			handleUnknownCommand()
		}
	case "restart":
		onlyMessage = true
		if isAdmin {
			if len(commandArgs) == 0 {
				if t.xrayService.IsXrayRunning() {
					err := t.xrayService.RestartXray(true)
					if err != nil {
						msg += t.I18nBot("tgbot.commands.restartFailed", "Error=="+err.Error())
					} else {
						msg += t.I18nBot("tgbot.commands.restartSuccess")
					}
				} else {
					msg += t.I18nBot("tgbot.commands.xrayNotRunning")
				}
			} else {
				handleUnknownCommand()
				msg += t.I18nBot("tgbot.commands.restartUsage")
			}
		} else {
			handleUnknownCommand()
		}
	default:
		handleUnknownCommand()
	}

	if msg != "" {
		t.sendResponse(chatId, msg, onlyMessage, isAdmin)
	}
}

// answerResellerCommand handles the small, deterministic reseller bot surface.
// Syntax:
//
//	/balance
//	/clients
//	/create <inbound-id> <email> <GB> [days]
//	/renew <email> <GB> [days]
//	/delete <email>
//	/reseller
//
// All mutations go through ResellerService's pricing/ownership ledger first.
func (t *Tgbot) answerResellerCommand(chatID, tgID int64, command string, args []string) bool {
	profile, user, err := t.resellerService.ProfileForTelegram(tgID)
	if err != nil {
		return false
	}
	switch command {
	case "start", "reseller":
		t.sendResellerMenu(chatID, user, profile)
		return true
	case "balance":
		b := t.resellerService.BalanceFor(user)
		if b.Unlimited {
			t.SendMsgToTgbot(chatID, fmt.Sprintf("👤 %s\n♾️ Unlimited balance\nActive accounts: %d", user.Username, t.resellerClientCount(user.Id)))
		} else {
			t.SendMsgToTgbot(chatID, fmt.Sprintf("👤 %s\n💰 Balance: %s\n📦 Committed: %s\n🟢 Available: %s\n👥 Accounts: %d",
				user.Username, formatResellerBytes(b.AllowanceBytes), formatResellerBytes(b.SpentBytes),
				formatResellerBytes(b.AvailableBytes), t.resellerClientCount(user.Id)))
		}
		return true
	case "inbounds":
		t.sendResellerInbounds(chatID, user.Id)
		return true
	case "clients":
		t.sendResellerClients(chatID, user.Id)
		return true
	case "create":
		if len(args) < 3 {
			t.SendMsgToTgbot(chatID, "Usage: /create <inbound-id> <email> <GB> [days]")
			return true
		}
		inboundID, e1 := strconv.Atoi(args[0])
		gb, e2 := strconv.Atoi(args[2])
		days := 0
		if len(args) >= 4 {
			days, _ = strconv.Atoi(args[3])
		}
		if e1 != nil || e2 != nil || inboundID <= 0 || gb <= 0 || gb > 1000000 || days < 0 || days > 36500 {
			t.SendMsgToTgbot(chatID, "Invalid arguments.")
			return true
		}
		msg, _ := t.createResellerClient(user, inboundID, strings.TrimSpace(args[1]), gb, days)
		t.SendMsgToTgbot(chatID, msg)
		return true
	case "renew":
		if len(args) < 2 {
			t.SendMsgToTgbot(chatID, "Usage: /renew <email> <GB> [days]")
			return true
		}
		gb, e := strconv.Atoi(args[1])
		days := 0
		if len(args) >= 3 {
			days, _ = strconv.Atoi(args[2])
		}
		if e != nil || gb <= 0 || gb > 1000000 || days < 0 || days > 36500 {
			t.SendMsgToTgbot(chatID, "Invalid arguments.")
			return true
		}
		msg, _ := t.renewResellerClient(user, strings.TrimSpace(args[0]), gb, days)
		t.SendMsgToTgbot(chatID, msg)
		return true
	case "reset":
		if len(args) != 1 {
			t.SendMsgToTgbot(chatID, "Usage: /reset <email>")
			return true
		}
		msg := t.resetResellerClient(user, strings.TrimSpace(args[0]))
		t.SendMsgToTgbot(chatID, msg)
		return true
	case "enable", "disable":
		if len(args) != 1 {
			t.SendMsgToTgbot(chatID, "Usage: /enable <email> or /disable <email>")
			return true
		}
		msg := t.toggleResellerClient(user, strings.TrimSpace(args[0]), command == "enable")
		t.SendMsgToTgbot(chatID, msg)
		return true
	case "delete":
		if len(args) != 1 {
			t.SendMsgToTgbot(chatID, "Usage: /delete <email>")
			return true
		}
		msg := t.deleteResellerClient(user, strings.TrimSpace(args[0]))
		t.SendMsgToTgbot(chatID, msg)
		return true
	default:
		return false
	}
}

func (t *Tgbot) sendResellerMenu(chatID int64, user *model.User, profile *model.ResellerProfile) {
	b := t.resellerService.BalanceFor(user)
	balance := formatResellerBytes(b.AvailableBytes)
	if b.Unlimited {
		balance = "Unlimited"
	}
	t.SendMsgToTgbot(chatID, fmt.Sprintf("🏪 <b>Reseller Panel</b>\nUser: %s\nBalance: %s\n\nSelect an action:", user.Username, balance), t.resellerKeyboard())
}

func (t *Tgbot) resellerKeyboard() *telego.InlineKeyboardMarkup {
	return tu.InlineKeyboard(
		tu.InlineKeyboardRow(tu.InlineKeyboardButton("💰 Balance").WithCallbackData("rs:balance"), tu.InlineKeyboardButton("👥 Accounts").WithCallbackData("rs:clients")),
		tu.InlineKeyboardRow(tu.InlineKeyboardButton("➕ Create").WithCallbackData("rs:create"), tu.InlineKeyboardButton("🔄 Renew").WithCallbackData("rs:renew")),
		tu.InlineKeyboardRow(tu.InlineKeyboardButton("🌐 Inbounds").WithCallbackData("rs:inbounds")),
	)
}

func (t *Tgbot) handleResellerCallback(q *telego.CallbackQuery) bool {
	_, user, err := t.resellerService.ProfileForTelegram(q.From.ID)
	if err != nil || user == nil || !user.Enable {
		return false
	}
	chatID := q.Message.GetChat().ID
	data := q.Data
	if data == "rs:menu" || data == "rs:balance" {
		t.sendCallbackAnswerTgBot(q.ID, "Updated")
		b := t.resellerService.BalanceFor(user)
		bal := formatResellerBytes(b.AvailableBytes)
		if b.Unlimited {
			bal = "Unlimited"
		}
		t.SendMsgToTgbot(chatID, fmt.Sprintf("🏪 <b>Reseller Panel</b>\nUser: %s\nBalance: %s", user.Username, bal), t.resellerKeyboard())
		return true
	}
	if data == "rs:create" {
		t.sendResellerInboundKeyboard(chatID, user.Id, true)
		t.sendCallbackAnswerTgBot(q.ID, "Choose inbound")
		return true
	}
	if data == "rs:inbounds" {
		t.sendResellerInboundKeyboard(chatID, user.Id, false)
		return true
	}
	if data == "rs:clients" || data == "rs:renew" {
		t.sendResellerClientsKeyboard(chatID, user.Id, data == "rs:renew")
		return true
	}
	if strings.HasPrefix(data, "rs:inbound:") {
		id, e := strconv.Atoi(strings.TrimPrefix(data, "rs:inbound:"))
		if e != nil || !t.resellerInboundAllowed(user.Id, id) {
			return true
		}
		resellerFlows.Store(chatID, &resellerFlow{Stage: "create_email", InboundID: id})
		t.sendCallbackAnswerTgBot(q.ID, "Inbound selected")
		t.SendMsgToTgbot(chatID, "✏️ Enter customer email:")
		return true
	}
	if strings.HasPrefix(data, "rs:manage:") {
		id, e := strconv.ParseInt(strings.TrimPrefix(data, "rs:manage:"), 10, 64)
		if e != nil {
			return true
		}
		var row model.ResellerClient
		if database.GetDB().Where("id = ? AND user_id = ?", id, user.Id).First(&row).Error != nil {
			return true
		}
		kb := tu.InlineKeyboard(tu.InlineKeyboardRow(tu.InlineKeyboardButton("✏️ Edit").WithCallbackData("rs:edit:"+strconv.FormatInt(id, 10)), tu.InlineKeyboardButton("🔄 Renew").WithCallbackData("rs:renew:"+strconv.FormatInt(id, 10))), tu.InlineKeyboardRow(tu.InlineKeyboardButton("♻️ Reset").WithCallbackData("rs:reset:"+strconv.FormatInt(id, 10))), tu.InlineKeyboardRow(tu.InlineKeyboardButton("✅ Enable").WithCallbackData("rs:enable:"+strconv.FormatInt(id, 10)), tu.InlineKeyboardButton("⛔ Disable").WithCallbackData("rs:disable:"+strconv.FormatInt(id, 10))), tu.InlineKeyboardRow(tu.InlineKeyboardButton("🗑 Delete").WithCallbackData("rs:delete:"+strconv.FormatInt(id, 10))), tu.InlineKeyboardRow(tu.InlineKeyboardButton("◀️ Menu").WithCallbackData("rs:menu")))
		t.editMessageTgBot(chatID, q.Message.GetMessageID(), fmt.Sprintf("⚙️ <b>%s</b>", html.EscapeString(row.Email)), kb)
		return true
	}
	if strings.HasPrefix(data, "rs:edit:") {
		id, e := strconv.ParseInt(strings.TrimPrefix(data, "rs:edit:"), 10, 64)
		if e != nil {
			return true
		}
		var row model.ResellerClient
		if database.GetDB().Where("id = ? AND user_id = ?", id, user.Id).First(&row).Error != nil {
			return true
		}
		resellerFlows.Store(chatID, &resellerFlow{Stage: "edit_gb", Email: row.Email})
		t.SendMsgToTgbot(chatID, fmt.Sprintf("✏️ Edit <b>%s</b>\nEnter total traffic in GB (0 = unlimited):", html.EscapeString(row.Email)))
		return true
	}
	if strings.HasPrefix(data, "rs:renew:") {
		id, e := strconv.ParseInt(strings.TrimPrefix(data, "rs:renew:"), 10, 64)
		if e != nil {
			return true
		}
		var row model.ResellerClient
		if database.GetDB().Where("id = ? AND user_id = ?", id, user.Id).First(&row).Error != nil {
			return true
		}
		resellerFlows.Store(chatID, &resellerFlow{Stage: "renew_gb", Email: row.Email})
		t.SendMsgToTgbot(chatID, fmt.Sprintf("🔄 Renew <b>%s</b>\nEnter additional GB:", html.EscapeString(row.Email)))
		return true
	}
	if strings.HasPrefix(data, "rs:reset:") || strings.HasPrefix(data, "rs:delete:") || strings.HasPrefix(data, "rs:enable:") || strings.HasPrefix(data, "rs:disable:") {
		prefixes := []string{"rs:reset:", "rs:delete:", "rs:enable:", "rs:disable:"}
		raw := data
		for _, p := range prefixes {
			raw = strings.TrimPrefix(raw, p)
		}
		id, e := strconv.ParseInt(raw, 10, 64)
		if e != nil {
			return true
		}
		var row model.ResellerClient
		if database.GetDB().Where("id = ? AND user_id = ?", id, user.Id).First(&row).Error != nil {
			return true
		}
		var msg string
		switch {
		case strings.HasPrefix(data, "rs:reset:"):
			msg = t.resetResellerClient(user, row.Email)
		case strings.HasPrefix(data, "rs:delete:"):
			msg = t.deleteResellerClient(user, row.Email)
		case strings.HasPrefix(data, "rs:enable:"):
			msg = t.toggleResellerClient(user, row.Email, true)
		default:
			msg = t.toggleResellerClient(user, row.Email, false)
		}
		t.sendCallbackAnswerTgBot(q.ID, msg)
		t.sendResellerClientsKeyboard(chatID, user.Id, false)
		return true
	}
	return false
}

func (t *Tgbot) sendResellerInboundKeyboard(chatID int64, userID int, forCreate bool) {
	var grants []model.InboundAccess
	if database.GetDB().Where("user_id = ?", userID).Find(&grants).Error != nil {
		return
	}
	kb := make([][]telego.InlineKeyboardButton, 0, len(grants)+1)
	for _, g := range grants {
		in, e := t.inboundService.GetInbound(g.InboundId)
		if e == nil && in != nil {
			kb = append(kb, []telego.InlineKeyboardButton{tu.InlineKeyboardButton(fmt.Sprintf("🌐 #%d %s", in.Id, in.Remark)).WithCallbackData("rs:inbound:" + strconv.Itoa(in.Id))})
		}
	}
	if !forCreate {
		kb = append(kb, []telego.InlineKeyboardButton{tu.InlineKeyboardButton("◀️ Menu").WithCallbackData("rs:menu")})
	}
	t.SendMsgToTgbot(chatID, "🌐 Choose an inbound:", &telego.InlineKeyboardMarkup{InlineKeyboard: kb})
}

func (t *Tgbot) sendResellerClientsKeyboard(chatID int64, userID int, renew bool) {
	var rows []model.ResellerClient
	if database.GetDB().Where("user_id = ?", userID).Order("id asc").Find(&rows).Error != nil {
		return
	}
	if len(rows) == 0 {
		t.SendMsgToTgbot(chatID, "No accounts yet.", t.resellerKeyboard())
		return
	}
	kb := make([][]telego.InlineKeyboardButton, 0, len(rows)+1)
	for _, r := range rows {
		action := "rs:manage:"
		if renew {
			action = "rs:renew:"
		}
		kb = append(kb, []telego.InlineKeyboardButton{tu.InlineKeyboardButton("👤 " + r.Email).WithCallbackData(action + strconv.FormatInt(int64(r.Id), 10))})
	}
	kb = append(kb, []telego.InlineKeyboardButton{tu.InlineKeyboardButton("◀️ Menu").WithCallbackData("rs:menu")})
	t.SendMsgToTgbot(chatID, "👥 Choose an account:", &telego.InlineKeyboardMarkup{InlineKeyboard: kb})
}

func (t *Tgbot) handleResellerText(message *telego.Message) bool {
	_, user, err := t.resellerService.ProfileForTelegram(message.From.ID)
	if err != nil || user == nil || !user.Enable {
		return false
	}
	v, ok := resellerFlows.Load(message.Chat.ID)
	if !ok {
		return false
	}
	f := v.(*resellerFlow)
	s := strings.TrimSpace(message.Text)
	if s == "" {
		return true
	}
	switch f.Stage {
	case "create_email":
		if !t.isSingleWord(s) && strings.Contains(s, "@") {
			f.Email = s
			f.Stage = "create_gb"
			t.SendMsgToTgbot(message.Chat.ID, "📦 Enter traffic in GB:")
		} else {
			t.SendMsgToTgbot(message.Chat.ID, "Invalid email. Try again:")
		}
	case "create_gb":
		n, e := strconv.Atoi(s)
		if e != nil || n <= 0 || n > 1000000 {
			t.SendMsgToTgbot(message.Chat.ID, "Enter a valid positive GB number:")
		} else {
			f.GB = n
			f.Stage = "create_days"
			t.SendMsgToTgbot(message.Chat.ID, "⏳ Enter validity in days, 0 for no expiry:")
		}
	case "create_days":
		n, e := strconv.Atoi(s)
		if e != nil || n < 0 || n > 36500 {
			t.SendMsgToTgbot(message.Chat.ID, "Enter days from 0 to 36500:")
		} else {
			m, _ := t.createResellerClient(user, f.InboundID, f.Email, f.GB, n)
			resellerFlows.Delete(message.Chat.ID)
			t.SendMsgToTgbot(message.Chat.ID, m, t.resellerKeyboard())
		}
	case "edit_gb":
		n, e := strconv.Atoi(s)
		if e != nil || n < 0 || n > 1000000 {
			t.SendMsgToTgbot(message.Chat.ID, "Enter a valid GB number from 0 to 1000000:")
		} else {
			f.GB = n
			f.Stage = "edit_days"
			t.SendMsgToTgbot(message.Chat.ID, "Enter validity in days, 0 for unlimited:")
		}
	case "edit_days":
		n, e := strconv.Atoi(s)
		if e != nil || n < 0 || n > 36500 {
			t.SendMsgToTgbot(message.Chat.ID, "Enter days from 0 to 36500:")
		} else {
			m, _ := t.editResellerClient(user, f.Email, f.GB, n)
			resellerFlows.Delete(message.Chat.ID)
			t.SendMsgToTgbot(message.Chat.ID, m, t.resellerKeyboard())
		}
	case "renew_gb":
		n, e := strconv.Atoi(s)
		if e != nil || n <= 0 || n > 1000000 {
			t.SendMsgToTgbot(message.Chat.ID, "Enter a valid positive GB number:")
		} else {
			f.GB = n
			f.Stage = "renew_days"
			t.SendMsgToTgbot(message.Chat.ID, "⏳ Enter additional days, 0 to keep expiry:")
		}
	case "renew_days":
		n, e := strconv.Atoi(s)
		if e != nil || n < 0 || n > 36500 {
			t.SendMsgToTgbot(message.Chat.ID, "Enter days from 0 to 36500:")
		} else {
			m, _ := t.renewResellerClient(user, f.Email, f.GB, n)
			resellerFlows.Delete(message.Chat.ID)
			t.SendMsgToTgbot(message.Chat.ID, m, t.resellerKeyboard())
		}
	}
	return true
}

func (t *Tgbot) resellerClientCount(userID int) int64 {
	var n int64
	database.GetDB().Model(&model.ResellerClient{}).Where("user_id = ?", userID).Count(&n)
	return n
}

func formatResellerBytes(v int64) string {
	if v < 0 {
		v = 0
	}
	const gb = int64(1024 * 1024 * 1024)
	if v >= gb {
		return fmt.Sprintf("%.2f GB", float64(v)/float64(gb))
	}
	const mb = int64(1024 * 1024)
	if v >= mb {
		return fmt.Sprintf("%.2f MB", float64(v)/float64(mb))
	}
	return fmt.Sprintf("%d B", v)
}

func (t *Tgbot) sendResellerInbounds(chatID int64, userID int) {
	var grants []model.InboundAccess
	if err := database.GetDB().Where("user_id = ?", userID).Find(&grants).Error; err != nil {
		t.SendMsgToTgbot(chatID, "Failed to load inbounds.")
		return
	}
	if len(grants) == 0 {
		t.SendMsgToTgbot(chatID, "No inbounds are assigned to you.")
		return
	}
	var b strings.Builder
	b.WriteString("🌐 Your inbounds:\n")
	for _, grant := range grants {
		inbound, err := t.inboundService.GetInbound(grant.InboundId)
		if err != nil || inbound == nil {
			continue
		}
		fmt.Fprintf(&b, "\n• #%d | %s | %s | port %d", inbound.Id, inbound.Remark, inbound.Protocol, inbound.Port)
	}
	t.SendMsgToTgbot(chatID, b.String())
}

func (t *Tgbot) sendResellerClients(chatID int64, userID int) {
	var rows []model.ResellerClient
	if err := database.GetDB().Where("user_id = ?", userID).Order("id asc").Find(&rows).Error; err != nil {
		t.SendMsgToTgbot(chatID, "Failed to load accounts.")
		return
	}
	if len(rows) == 0 {
		t.SendMsgToTgbot(chatID, "No accounts yet.")
		return
	}
	var b strings.Builder
	b.WriteString("👥 Your accounts:\n")
	for _, row := range rows {
		traffic, _ := t.inboundService.GetClientTrafficByEmail(row.Email)
		if traffic == nil {
			fmt.Fprintf(&b, "\n• %s | traffic unavailable", row.Email)
			continue
		}
		remaining := traffic.Total - traffic.Up - traffic.Down
		if remaining < 0 {
			remaining = 0
		}
		status := "active"
		if !traffic.Enable {
			status = "disabled"
		} else if traffic.ExpiryTime > 0 && traffic.ExpiryTime <= time.Now().UnixMilli() {
			status = "expired"
		} else if traffic.Total > 0 && traffic.Up+traffic.Down >= traffic.Total {
			status = "depleted"
		}
		fmt.Fprintf(&b, "\n• %s | %s | %s | used %s / left %s",
			row.Email, status, time.UnixMilli(traffic.ExpiryTime).Format("2006-01-02 15:04"),
			formatResellerBytes(traffic.Up+traffic.Down), formatResellerBytes(remaining))
	}
	t.SendMsgToTgbot(chatID, b.String())
}

func (t *Tgbot) resellerInboundAllowed(userID, inboundID int) bool {
	var n int64
	database.GetDB().Model(&model.InboundAccess{}).
		Where("user_id = ? AND inbound_id = ?", userID, inboundID).Count(&n)
	return n > 0
}

func (t *Tgbot) buildResellerClient(protocol model.Protocol, email string) model.Client {
	c := model.Client{
		Email: email, Security: "auto", Enable: true,
		SubID: t.randomLowerAndNum(16), Comment: "",
	}
	switch protocol {
	case model.VMESS, model.VLESS:
		c.ID = uuid.NewString()
	case model.Trojan, model.Shadowsocks, model.ANYTLS, model.NAIVE:
		c.Password = t.randomLowerAndNum(32)
		c.ID = email
	case model.Hysteria, model.Hysteria2:
		c.Auth = t.randomLowerAndNum(32)
		c.ID = email
	case model.L2TP, model.PPTP, model.OPENVPN, model.OPENCONNECT, model.SSTP, model.IKEV2:
		c.ID = t.randomLowerAndNum(12)
		c.Password = t.randomLowerAndNum(20)
	case model.WGC, model.AWG, model.GRE, model.SSH:
		c.ID = email
	case model.MTPROTO:
		c.ID = email
		c.Secret = t.randomLowerAndNum(32)
	case model.TUIC:
		c.ID = uuid.NewString()
		c.Password = t.randomLowerAndNum(32)
	default:
		c.ID = uuid.NewString()
	}
	return c
}

func (t *Tgbot) createResellerClient(user *model.User, inboundID int, email string, gb int, days int) (string, error) {
	if email == "" {
		return "Email is required.", errors.New("email required")
	}
	if !t.resellerInboundAllowed(user.Id, inboundID) {
		return "You do not have access to this inbound.", errors.New("inbound not assigned")
	}
	inbound, err := t.inboundService.GetInbound(inboundID)
	if err != nil || inbound == nil {
		return "Inbound not found.", err
	}
	client := t.buildResellerClient(inbound.Protocol, email)
	client.TotalGB = int64(gb) * oneGB
	if days > 0 {
		client.ExpiryTime = time.Now().Add(time.Duration(days) * 24 * time.Hour).UnixMilli()
	}
	payload, err := json.Marshal(map[string]any{"clients": []model.Client{client}})
	if err != nil {
		return "Failed to build client.", err
	}
	data := &model.Inbound{Id: inboundID, Settings: string(payload)}
	ticket, err := t.resellerService.PrepareClientCreate(user, data)
	if err != nil {
		return err.Error(), err
	}
	needRestart, err := t.inboundService.AddInboundClient(data)
	if err != nil {
		_ = t.resellerService.Rollback(ticket)
		return err.Error(), err
	}
	if needRestart {
		t.xrayService.SetToNeedRestart()
	}
	message := fmt.Sprintf("✅ Account created\nEmail: %s\nTraffic: %d GB\nExpiry: %s\nCredential: %s",
		email, gb, formatResellerExpiry(client.ExpiryTime), resellerCredential(client, inbound.Protocol))
	if links := t.resellerSubscriptionLinks(client.SubID); links != "" {
		message += "\n\nSubscription:\n" + links
	}
	return message, nil
}

func (t *Tgbot) resellerSubscriptionLinks(subID string) string {
	if strings.TrimSpace(subID) == "" {
		return ""
	}
	settings, err := t.settingService.GetAllSetting()
	if err != nil || settings == nil || !settings.SubEnable {
		return ""
	}
	id := url.PathEscape(strings.TrimSpace(subID))
	build := func(configured, scheme, host, path string) string {
		if strings.TrimSpace(configured) != "" {
			base := strings.TrimRight(strings.TrimSpace(configured), "/")
			return base + "/" + id
		}
		if host == "" || path == "" {
			return ""
		}
		return strings.TrimRight(scheme+"://"+host+path, "/") + "/" + id
	}
	scheme := "http"
	if settings.SubCertFile != "" && settings.SubKeyFile != "" {
		scheme = "https"
	}
	host := settings.SubDomain
	if host == "" {
		if settings.WebDomain != "" {
			host = settings.WebDomain
		} else {
			host = hostname
		}
	}
	if settings.SubPort > 0 && !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, settings.SubPort)
	}
	var links []string
	if link := build(settings.SubURI, scheme, host, settings.SubPath); link != "" {
		links = append(links, "Base: "+link)
	}
	if settings.SubJsonEnable {
		if link := build(settings.SubJsonURI, scheme, host, settings.SubJsonPath); link != "" {
			links = append(links, "JSON: "+link)
		}
	}
	if settings.SubClashEnable {
		if link := build(settings.SubClashURI, scheme, host, settings.SubClashPath); link != "" {
			links = append(links, "Clash: "+link)
		}
	}
	return strings.Join(links, "\n")
}

func (t *Tgbot) editResellerClient(user *model.User, email string, gb int, days int) (string, error) {
	owner, err := t.resellerService.ClientOwner(email)
	if err != nil || owner == nil || owner.UserId != user.Id {
		return "Account not found.", ErrClientNotOwned
	}
	traffic, inbound, err := t.inboundService.GetClientInboundByEmail(email)
	if err != nil || traffic == nil || inbound == nil {
		return "Account data not found.", err
	}
	clients, err := t.inboundService.GetClients(inbound)
	if err != nil {
		return "Account data not found.", err
	}
	var current *model.Client
	for i := range clients {
		if clients[i].Email == email {
			current = &clients[i]
			break
		}
	}
	if current == nil {
		return "Account not found on its home inbound.", errors.New("client not found")
	}
	current.TotalGB = int64(gb) * oneGB
	if days <= 0 {
		current.ExpiryTime = 0
	} else {
		current.ExpiryTime = time.Now().Add(time.Duration(days) * 24 * time.Hour).UnixMilli()
	}
	payload, err := json.Marshal(map[string]any{"clients": []model.Client{*current}})
	if err != nil {
		return "Failed to build update.", err
	}
	data := &model.Inbound{Id: inbound.Id, Settings: string(payload)}
	clientID := clientIdentity(inbound.Protocol, *current)
	ticket, err := t.resellerService.PrepareClientUpdate(user, data, clientID)
	if err != nil {
		return err.Error(), err
	}
	needRestart, err := t.inboundService.UpdateInboundClient(data, clientID)
	if err != nil {
		_ = t.resellerService.Rollback(ticket)
		return err.Error(), err
	}
	if needRestart {
		t.xrayService.SetToNeedRestart()
	}
	return fmt.Sprintf("✏️ Account updated\nEmail: %s\nTraffic: %s\nExpiry: %s", email, formatResellerBytes(current.TotalGB), formatResellerExpiry(current.ExpiryTime)), nil
}

func formatResellerExpiry(ms int64) string {
	if ms <= 0 {
		return "unlimited"
	}
	return time.UnixMilli(ms).Format("2006-01-02 15:04")
}

func resellerCredential(c model.Client, p model.Protocol) string {
	switch p {
	case model.VMESS, model.VLESS, model.WGC, model.AWG, model.GRE, model.SSH, model.MTPROTO:
		return c.ID
	case model.Hysteria, model.Hysteria2:
		return c.Auth
	default:
		return c.Password
	}
}

func (t *Tgbot) renewResellerClient(user *model.User, email string, gb int, days int) (string, error) {
	owner, err := t.resellerService.ClientOwner(email)
	if err != nil || owner == nil || owner.UserId != user.Id {
		return "Account not found.", ErrClientNotOwned
	}
	traffic, inbound, err := t.inboundService.GetClientInboundByEmail(email)
	if err != nil || traffic == nil || inbound == nil {
		return "Account data not found.", err
	}
	clients, err := t.inboundService.GetClients(inbound)
	if err != nil {
		return "Account data not found.", err
	}
	var current *model.Client
	for i := range clients {
		if clients[i].Email == email {
			current = &clients[i]
			break
		}
	}
	if current == nil {
		return "Account not found on its home inbound.", errors.New("client not found")
	}
	current.TotalGB = traffic.Total + int64(gb)*oneGB
	if days > 0 {
		base := time.Now()
		if traffic.ExpiryTime > base.UnixMilli() {
			base = time.UnixMilli(traffic.ExpiryTime)
		}
		current.ExpiryTime = base.Add(time.Duration(days) * 24 * time.Hour).UnixMilli()
	}
	payload, err := json.Marshal(map[string]any{"clients": []model.Client{*current}})
	if err != nil {
		return "Failed to build update.", err
	}
	data := &model.Inbound{Id: inbound.Id, Settings: string(payload)}
	clientID := clientIdentity(inbound.Protocol, *current)
	ticket, err := t.resellerService.PrepareClientUpdate(user, data, clientID)
	if err != nil {
		return err.Error(), err
	}
	needRestart, err := t.inboundService.UpdateInboundClient(data, clientID)
	if err != nil {
		_ = t.resellerService.Rollback(ticket)
		return err.Error(), err
	}
	if needRestart {
		t.xrayService.SetToNeedRestart()
	}
	return fmt.Sprintf("✅ Account renewed\nEmail: %s\nTraffic: %s\nExpiry: %s",
		email, formatResellerBytes(current.TotalGB), formatResellerExpiry(current.ExpiryTime)), nil
}

func (t *Tgbot) resetResellerClient(user *model.User, email string) string {
	owner, err := t.resellerService.ClientOwner(email)
	if err != nil || owner == nil || owner.UserId != user.Id {
		return "Account not found."
	}
	ticket, err := t.resellerService.PrepareClientReset(user, email)
	if err != nil {
		return err.Error()
	}
	if err := t.inboundService.ResetClientTrafficByEmail(email); err != nil {
		_ = t.resellerService.Rollback(ticket)
		return err.Error()
	}
	return "✅ Traffic reset. The reset was charged from your balance."
}

func (t *Tgbot) toggleResellerClient(user *model.User, email string, enable bool) string {
	owner, err := t.resellerService.ClientOwner(email)
	if err != nil || owner == nil || owner.UserId != user.Id {
		return "Account not found."
	}
	if enable {
		traffic, err := t.inboundService.GetClientTrafficByEmail(email)
		if err != nil || traffic == nil {
			return "Account data not found."
		}
		if traffic.Total > 0 && traffic.Up+traffic.Down >= traffic.Total {
			return "Cannot enable a depleted account. Add traffic with /renew first."
		}
	}
	_, needRestart, err := t.inboundService.SetClientEnableByEmail(email, enable)
	if err != nil {
		return err.Error()
	}
	if needRestart {
		t.xrayService.SetToNeedRestart()
	}
	if enable {
		return "✅ Account enabled."
	}
	return "✅ Account disabled."
}

func (t *Tgbot) deleteResellerClient(user *model.User, email string) string {
	owner, err := t.resellerService.ClientOwner(email)
	if err != nil || owner == nil || owner.UserId != user.Id {
		return "Account not found."
	}
	used, known, err := t.resellerService.UsageOf(email)
	if err != nil {
		return "Could not read usage."
	}
	ids, err := servingInboundIds(database.GetDB(), email)
	if err != nil {
		return "Could not resolve account memberships."
	}
	needRestart := false
	for _, id := range ids {
		restart, err := t.inboundService.DelInboundClientByEmail(id, email)
		if err != nil {
			return fmt.Sprintf("Delete stopped: %v", err)
		}
		needRestart = needRestart || restart
	}
	if needRestart {
		t.xrayService.SetToNeedRestart()
	}
	if err := t.resellerService.RefundDeleted(email, used, known); err != nil {
		return fmt.Sprintf("Account removed, but balance settlement failed: %v", err)
	}
	return "✅ Account deleted and unused balance refunded."
}

// sendResponse sends the response message based on the onlyMessage flag.
func (t *Tgbot) sendResponse(chatId int64, msg string, onlyMessage, isAdmin bool) {
	if onlyMessage {
		t.SendMsgToTgbot(chatId, msg)
	} else {
		t.SendAnswer(chatId, msg, isAdmin)
	}
}

// randomLowerAndNum generates a random string of lowercase letters and numbers.
func (t *Tgbot) randomLowerAndNum(length int) string {
	charset := "abcdefghijklmnopqrstuvwxyz0123456789"
	bytes := make([]byte, length)
	for i := range bytes {
		randomIndex, _ := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		bytes[i] = charset[randomIndex.Int64()]
	}
	return string(bytes)
}

// randomShadowSocksPassword generates a random password for Shadowsocks.
func (t *Tgbot) randomShadowSocksPassword() string {
	array := make([]byte, 32)
	_, err := rand.Read(array)
	if err != nil {
		return t.randomLowerAndNum(32)
	}
	return base64.StdEncoding.EncodeToString(array)
}

// answerCallback processes callback queries from inline keyboards.
func (t *Tgbot) answerCallback(callbackQuery *telego.CallbackQuery, isAdmin bool) {
	chatId := callbackQuery.Message.GetChat().ID
	if !isAdmin && t.handleResellerCallback(callbackQuery) {
		return
	}

	if isAdmin {
		// get query from hash storage
		decodedQuery, err := t.decodeQuery(callbackQuery.Data)
		if err != nil {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.noQuery"))
			return
		}
		dataArray := strings.Split(decodedQuery, " ")

		if len(dataArray) >= 2 && len(dataArray[1]) > 0 {
			email := dataArray[1]
			switch dataArray[0] {
			case "get_clients_for_sub":
				inboundId := dataArray[1]
				inboundIdInt, err := strconv.Atoi(inboundId)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				clientsKB, err := t.getInboundClientsFor(inboundIdInt, "client_sub_links")
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				inbound, _ := t.inboundService.GetInbound(inboundIdInt)
				t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseClient", "Inbound=="+inbound.Remark), clientsKB)
			case "get_clients_for_individual":
				inboundId := dataArray[1]
				inboundIdInt, err := strconv.Atoi(inboundId)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				clientsKB, err := t.getInboundClientsFor(inboundIdInt, "client_individual_links")
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				inbound, _ := t.inboundService.GetInbound(inboundIdInt)
				t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseClient", "Inbound=="+inbound.Remark), clientsKB)
			case "get_clients_for_qr":
				inboundId := dataArray[1]
				inboundIdInt, err := strconv.Atoi(inboundId)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				clientsKB, err := t.getInboundClientsFor(inboundIdInt, "client_qr_links")
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				inbound, _ := t.inboundService.GetInbound(inboundIdInt)
				t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseClient", "Inbound=="+inbound.Remark), clientsKB)
			case "client_sub_links":
				t.sendClientSubLinks(chatId, email)
				return
			case "client_individual_links":
				t.sendClientIndividualLinks(chatId, email)
				return
			case "client_qr_links":
				t.sendClientQRLinks(chatId, email)
				return
			case "client_get_usage":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.messages.email", "Email=="+email))
				t.searchClient(chatId, email)
			case "client_refresh":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.clientRefreshSuccess", "Email=="+email))
				t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
			case "client_cancel":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.canceled", "Email=="+email))
				t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
			case "ips_refresh":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.IpRefreshSuccess", "Email=="+email))
				t.searchClientIps(chatId, email, callbackQuery.Message.GetMessageID())
			case "ips_cancel":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.canceled", "Email=="+email))
				t.searchClientIps(chatId, email, callbackQuery.Message.GetMessageID())
			case "tgid_refresh":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.TGIdRefreshSuccess", "Email=="+email))
				t.clientTelegramUserInfo(chatId, email, callbackQuery.Message.GetMessageID())
			case "tgid_cancel":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.canceled", "Email=="+email))
				t.clientTelegramUserInfo(chatId, email, callbackQuery.Message.GetMessageID())
			case "reset_traffic":
				inlineKeyboard := tu.InlineKeyboard(
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancelReset")).WithCallbackData(t.encodeQuery("client_cancel "+email)),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmResetTraffic")).WithCallbackData(t.encodeQuery("reset_traffic_c "+email)),
					),
				)
				t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
			case "reset_traffic_c":
				err := t.inboundService.ResetClientTrafficByEmail(email)
				if err == nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.resetTrafficSuccess", "Email=="+email))
					t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
				} else {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				}
			case "limit_traffic":
				inlineKeyboard := tu.InlineKeyboard(
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("client_cancel "+email)),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.unlimited")).WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 0")),
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.custom")).WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" 0")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton("1 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 1")),
						tu.InlineKeyboardButton("5 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 5")),
						tu.InlineKeyboardButton("10 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 10")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton("20 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 20")),
						tu.InlineKeyboardButton("30 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 30")),
						tu.InlineKeyboardButton("40 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 40")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton("50 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 50")),
						tu.InlineKeyboardButton("60 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 60")),
						tu.InlineKeyboardButton("80 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 80")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton("100 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 100")),
						tu.InlineKeyboardButton("150 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 150")),
						tu.InlineKeyboardButton("200 GB").WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" 200")),
					),
				)
				t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
			case "limit_traffic_c":
				if len(dataArray) == 3 {
					limitTraffic, err := strconv.Atoi(dataArray[2])
					if err == nil {
						needRestart, err := t.inboundService.ResetClientTrafficLimitByEmail(email, limitTraffic)
						if needRestart {
							t.xrayService.SetToNeedRestart()
						}
						if err == nil {
							t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.setTrafficLimitSuccess", "Email=="+email))
							t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
							return
						}
					}
				}
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
			case "limit_traffic_in":
				if len(dataArray) >= 3 {
					oldInputNumber, err := strconv.Atoi(dataArray[2])
					inputNumber := oldInputNumber
					if err == nil {
						if len(dataArray) == 4 {
							num, err := strconv.Atoi(dataArray[3])
							if err == nil {
								switch num {
								case -2:
									inputNumber = 0
								case -1:
									if inputNumber > 0 {
										inputNumber = (inputNumber / 10)
									}
								default:
									inputNumber = (inputNumber * 10) + num
								}
							}
							if inputNumber == oldInputNumber {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
								return
							}
							if inputNumber >= 999999 {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
								return
							}
						}
						inlineKeyboard := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("client_cancel "+email)),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmNumberAdd", "Num=="+strconv.Itoa(inputNumber))).WithCallbackData(t.encodeQuery("limit_traffic_c "+email+" "+strconv.Itoa(inputNumber))),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("1").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 1")),
								tu.InlineKeyboardButton("2").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 2")),
								tu.InlineKeyboardButton("3").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 3")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("4").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 4")),
								tu.InlineKeyboardButton("5").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 5")),
								tu.InlineKeyboardButton("6").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 6")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("7").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 7")),
								tu.InlineKeyboardButton("8").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 8")),
								tu.InlineKeyboardButton("9").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 9")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("🔄").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" -2")),
								tu.InlineKeyboardButton("0").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" 0")),
								tu.InlineKeyboardButton("⬅️").WithCallbackData(t.encodeQuery("limit_traffic_in "+email+" "+strconv.Itoa(inputNumber)+" -1")),
							),
						)
						t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
						return
					}
				}
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
			case "add_client_limit_traffic_c":
				limitTraffic, _ := strconv.ParseInt(dataArray[1], 10, 64)
				client_TotalGB = limitTraffic * 1024 * 1024 * 1024
				messageId := callbackQuery.Message.GetMessageID()
				inbound, err := t.inboundService.GetInbound(receiver_inbound_ID)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				message_text, err := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}

				t.addClient(callbackQuery.Message.GetChat().ID, message_text, messageId)
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
			case "add_client_limit_traffic_in":
				if len(dataArray) >= 2 {
					oldInputNumber, err := strconv.Atoi(dataArray[1])
					inputNumber := oldInputNumber
					if err == nil {
						if len(dataArray) == 3 {
							num, err := strconv.Atoi(dataArray[2])
							if err == nil {
								switch num {
								case -2:
									inputNumber = 0
								case -1:
									if inputNumber > 0 {
										inputNumber = (inputNumber / 10)
									}
								default:
									inputNumber = (inputNumber * 10) + num
								}
							}
							if inputNumber == oldInputNumber {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
								return
							}
							if inputNumber >= 999999 {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
								return
							}
						}
						inlineKeyboard := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("add_client_default_traffic_exp")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmNumberAdd", "Num=="+strconv.Itoa(inputNumber))).WithCallbackData(t.encodeQuery("add_client_limit_traffic_c "+strconv.Itoa(inputNumber))),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("1").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 1")),
								tu.InlineKeyboardButton("2").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 2")),
								tu.InlineKeyboardButton("3").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 3")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("4").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 4")),
								tu.InlineKeyboardButton("5").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 5")),
								tu.InlineKeyboardButton("6").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 6")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("7").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 7")),
								tu.InlineKeyboardButton("8").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 8")),
								tu.InlineKeyboardButton("9").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 9")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("🔄").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" -2")),
								tu.InlineKeyboardButton("0").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" 0")),
								tu.InlineKeyboardButton("⬅️").WithCallbackData(t.encodeQuery("add_client_limit_traffic_in "+strconv.Itoa(inputNumber)+" -1")),
							),
						)
						t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
						return
					}
				}
			case "reset_exp":
				inlineKeyboard := tu.InlineKeyboard(
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancelReset")).WithCallbackData(t.encodeQuery("client_cancel "+email)),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.unlimited")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 0")),
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.custom")).WithCallbackData(t.encodeQuery("reset_exp_in "+email+" 0")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 7 "+t.I18nBot("tgbot.days")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 7")),
						tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 10 "+t.I18nBot("tgbot.days")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 10")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 14 "+t.I18nBot("tgbot.days")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 14")),
						tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 20 "+t.I18nBot("tgbot.days")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 20")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 1 "+t.I18nBot("tgbot.month")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 30")),
						tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 3 "+t.I18nBot("tgbot.months")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 90")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 6 "+t.I18nBot("tgbot.months")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 180")),
						tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 12 "+t.I18nBot("tgbot.months")).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" 365")),
					),
				)
				t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
			case "reset_exp_c":
				if len(dataArray) == 3 {
					days, err := strconv.ParseInt(dataArray[2], 10, 64)
					if err == nil {
						var date int64
						if days > 0 {
							traffic, err := t.inboundService.GetClientTrafficByEmail(email)
							if err != nil {
								logger.Warning(err)
								msg := t.I18nBot("tgbot.wentWrong")
								t.SendMsgToTgbot(chatId, msg)
								return
							}
							if traffic == nil {
								msg := t.I18nBot("tgbot.noResult")
								t.SendMsgToTgbot(chatId, msg)
								return
							}

							if traffic.ExpiryTime > 0 {
								if traffic.ExpiryTime-time.Now().Unix()*1000 < 0 {
									date = -int64(days * 24 * 60 * 60000)
								} else {
									date = traffic.ExpiryTime + int64(days*24*60*60000)
								}
							} else {
								date = traffic.ExpiryTime - int64(days*24*60*60000)
							}

						}
						needRestart, err := t.inboundService.ResetClientExpiryTimeByEmail(email, date)
						if needRestart {
							t.xrayService.SetToNeedRestart()
						}
						if err == nil {
							t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.expireResetSuccess", "Email=="+email))
							t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
							return
						}
					}
				}
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
			case "reset_exp_in":
				if len(dataArray) >= 3 {
					oldInputNumber, err := strconv.Atoi(dataArray[2])
					inputNumber := oldInputNumber
					if err == nil {
						if len(dataArray) == 4 {
							num, err := strconv.Atoi(dataArray[3])
							if err == nil {
								switch num {
								case -2:
									inputNumber = 0
								case -1:
									if inputNumber > 0 {
										inputNumber = (inputNumber / 10)
									}
								default:
									inputNumber = (inputNumber * 10) + num
								}
							}
							if inputNumber == oldInputNumber {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
								return
							}
							if inputNumber >= 999999 {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
								return
							}
						}
						inlineKeyboard := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("client_cancel "+email)),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmNumber", "Num=="+strconv.Itoa(inputNumber))).WithCallbackData(t.encodeQuery("reset_exp_c "+email+" "+strconv.Itoa(inputNumber))),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("1").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 1")),
								tu.InlineKeyboardButton("2").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 2")),
								tu.InlineKeyboardButton("3").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 3")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("4").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 4")),
								tu.InlineKeyboardButton("5").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 5")),
								tu.InlineKeyboardButton("6").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 6")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("7").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 7")),
								tu.InlineKeyboardButton("8").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 8")),
								tu.InlineKeyboardButton("9").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 9")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("🔄").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" -2")),
								tu.InlineKeyboardButton("0").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" 0")),
								tu.InlineKeyboardButton("⬅️").WithCallbackData(t.encodeQuery("reset_exp_in "+email+" "+strconv.Itoa(inputNumber)+" -1")),
							),
						)
						t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
						return
					}
				}
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
			case "add_client_reset_exp_c":
				client_ExpiryTime = 0
				days, _ := strconv.ParseInt(dataArray[1], 10, 64)
				var date int64
				if client_ExpiryTime > 0 {
					if client_ExpiryTime-time.Now().Unix()*1000 < 0 {
						date = -int64(days * 24 * 60 * 60000)
					} else {
						date = client_ExpiryTime + int64(days*24*60*60000)
					}
				} else {
					date = client_ExpiryTime - int64(days*24*60*60000)
				}
				client_ExpiryTime = date

				messageId := callbackQuery.Message.GetMessageID()
				inbound, err := t.inboundService.GetInbound(receiver_inbound_ID)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				message_text, err := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}

				t.addClient(callbackQuery.Message.GetChat().ID, message_text, messageId)
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
			case "add_client_reset_exp_in":
				if len(dataArray) >= 2 {
					oldInputNumber, err := strconv.Atoi(dataArray[1])
					inputNumber := oldInputNumber
					if err == nil {
						if len(dataArray) == 3 {
							num, err := strconv.Atoi(dataArray[2])
							if err == nil {
								switch num {
								case -2:
									inputNumber = 0
								case -1:
									if inputNumber > 0 {
										inputNumber = (inputNumber / 10)
									}
								default:
									inputNumber = (inputNumber * 10) + num
								}
							}
							if inputNumber == oldInputNumber {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
								return
							}
							if inputNumber >= 999999 {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
								return
							}
						}
						inlineKeyboard := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("add_client_default_traffic_exp")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmNumberAdd", "Num=="+strconv.Itoa(inputNumber))).WithCallbackData(t.encodeQuery("add_client_reset_exp_c "+strconv.Itoa(inputNumber))),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("1").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 1")),
								tu.InlineKeyboardButton("2").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 2")),
								tu.InlineKeyboardButton("3").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 3")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("4").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 4")),
								tu.InlineKeyboardButton("5").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 5")),
								tu.InlineKeyboardButton("6").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 6")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("7").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 7")),
								tu.InlineKeyboardButton("8").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 8")),
								tu.InlineKeyboardButton("9").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 9")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("🔄").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" -2")),
								tu.InlineKeyboardButton("0").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" 0")),
								tu.InlineKeyboardButton("⬅️").WithCallbackData(t.encodeQuery("add_client_reset_exp_in "+strconv.Itoa(inputNumber)+" -1")),
							),
						)
						t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
						return
					}
				}
			case "ip_limit":
				inlineKeyboard := tu.InlineKeyboard(
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancelIpLimit")).WithCallbackData(t.encodeQuery("client_cancel "+email)),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.unlimited")).WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 0")),
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.custom")).WithCallbackData(t.encodeQuery("ip_limit_in "+email+" 0")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton("1").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 1")),
						tu.InlineKeyboardButton("2").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 2")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton("3").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 3")),
						tu.InlineKeyboardButton("4").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 4")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton("5").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 5")),
						tu.InlineKeyboardButton("6").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 6")),
						tu.InlineKeyboardButton("7").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 7")),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton("8").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 8")),
						tu.InlineKeyboardButton("9").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 9")),
						tu.InlineKeyboardButton("10").WithCallbackData(t.encodeQuery("ip_limit_c "+email+" 10")),
					),
				)
				t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
			case "ip_limit_c":
				if len(dataArray) == 3 {
					count, err := strconv.Atoi(dataArray[2])
					if err == nil {
						needRestart, err := t.inboundService.ResetClientIpLimitByEmail(email, count)
						if needRestart {
							t.xrayService.SetToNeedRestart()
						}
						if err == nil {
							t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.resetIpSuccess", "Email=="+email, "Count=="+strconv.Itoa(count)))
							t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
							return
						}
					}
				}
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
			case "ip_limit_in":
				if len(dataArray) >= 3 {
					oldInputNumber, err := strconv.Atoi(dataArray[2])
					inputNumber := oldInputNumber
					if err == nil {
						if len(dataArray) == 4 {
							num, err := strconv.Atoi(dataArray[3])
							if err == nil {
								switch num {
								case -2:
									inputNumber = 0
								case -1:
									if inputNumber > 0 {
										inputNumber = (inputNumber / 10)
									}
								default:
									inputNumber = (inputNumber * 10) + num
								}
							}
							if inputNumber == oldInputNumber {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
								return
							}
							if inputNumber >= 999999 {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
								return
							}
						}
						inlineKeyboard := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("client_cancel "+email)),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmNumber", "Num=="+strconv.Itoa(inputNumber))).WithCallbackData(t.encodeQuery("ip_limit_c "+email+" "+strconv.Itoa(inputNumber))),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("1").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 1")),
								tu.InlineKeyboardButton("2").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 2")),
								tu.InlineKeyboardButton("3").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 3")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("4").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 4")),
								tu.InlineKeyboardButton("5").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 5")),
								tu.InlineKeyboardButton("6").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 6")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("7").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 7")),
								tu.InlineKeyboardButton("8").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 8")),
								tu.InlineKeyboardButton("9").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 9")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("🔄").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" -2")),
								tu.InlineKeyboardButton("0").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" 0")),
								tu.InlineKeyboardButton("⬅️").WithCallbackData(t.encodeQuery("ip_limit_in "+email+" "+strconv.Itoa(inputNumber)+" -1")),
							),
						)
						t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
						return
					}
				}
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
			case "add_client_ip_limit_c":
				if len(dataArray) == 2 {
					count, _ := strconv.Atoi(dataArray[1])
					client_LimitIP = count
				}

				messageId := callbackQuery.Message.GetMessageID()
				inbound, err := t.inboundService.GetInbound(receiver_inbound_ID)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				message_text, err := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}

				t.addClient(callbackQuery.Message.GetChat().ID, message_text, messageId)
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
			case "add_client_ip_limit_in":
				if len(dataArray) >= 2 {
					oldInputNumber, err := strconv.Atoi(dataArray[1])
					inputNumber := oldInputNumber
					if err == nil {
						if len(dataArray) == 3 {
							num, err := strconv.Atoi(dataArray[2])
							if err == nil {
								switch num {
								case -2:
									inputNumber = 0
								case -1:
									if inputNumber > 0 {
										inputNumber = (inputNumber / 10)
									}
								default:
									inputNumber = (inputNumber * 10) + num
								}
							}
							if inputNumber == oldInputNumber {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
								return
							}
							if inputNumber >= 999999 {
								t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
								return
							}
						}
						inlineKeyboard := tu.InlineKeyboard(
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("add_client_default_ip_limit")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmNumber", "Num=="+strconv.Itoa(inputNumber))).WithCallbackData(t.encodeQuery("add_client_ip_limit_c "+strconv.Itoa(inputNumber))),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("1").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 1")),
								tu.InlineKeyboardButton("2").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 2")),
								tu.InlineKeyboardButton("3").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 3")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("4").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 4")),
								tu.InlineKeyboardButton("5").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 5")),
								tu.InlineKeyboardButton("6").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 6")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("7").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 7")),
								tu.InlineKeyboardButton("8").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 8")),
								tu.InlineKeyboardButton("9").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 9")),
							),
							tu.InlineKeyboardRow(
								tu.InlineKeyboardButton("🔄").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" -2")),
								tu.InlineKeyboardButton("0").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" 0")),
								tu.InlineKeyboardButton("⬅️").WithCallbackData(t.encodeQuery("add_client_ip_limit_in "+strconv.Itoa(inputNumber)+" -1")),
							),
						)
						t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
						return
					}
				}
			case "clear_ips":
				inlineKeyboard := tu.InlineKeyboard(
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("ips_cancel "+email)),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmClearIps")).WithCallbackData(t.encodeQuery("clear_ips_c "+email)),
					),
				)
				t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
			case "clear_ips_c":
				err := t.inboundService.ClearClientIps(email)
				if err == nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.clearIpSuccess", "Email=="+email))
					t.searchClientIps(chatId, email, callbackQuery.Message.GetMessageID())
				} else {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				}
			case "ip_log":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.getIpLog", "Email=="+email))
				t.searchClientIps(chatId, email)
			case "tg_user":
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.getUserInfo", "Email=="+email))
				t.clientTelegramUserInfo(chatId, email)
			case "tgid_remove":
				inlineKeyboard := tu.InlineKeyboard(
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("tgid_cancel "+email)),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmRemoveTGUser")).WithCallbackData(t.encodeQuery("tgid_remove_c "+email)),
					),
				)
				t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
			case "tgid_remove_c":
				traffic, err := t.inboundService.GetClientTrafficByEmail(email)
				if err != nil || traffic == nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
					return
				}
				needRestart, err := t.inboundService.SetClientTelegramUserID(traffic.Id, EmptyTelegramUserID)
				if needRestart {
					t.xrayService.SetToNeedRestart()
				}
				if err == nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.removedTGUserSuccess", "Email=="+email))
					t.clientTelegramUserInfo(chatId, email, callbackQuery.Message.GetMessageID())
				} else {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				}
			case "toggle_enable":
				inlineKeyboard := tu.InlineKeyboard(
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("client_cancel "+email)),
					),
					tu.InlineKeyboardRow(
						tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmToggle")).WithCallbackData(t.encodeQuery("toggle_enable_c "+email)),
					),
				)
				t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
			case "toggle_enable_c":
				enabled, needRestart, err := t.inboundService.ToggleClientEnableByEmail(email)
				if needRestart {
					t.xrayService.SetToNeedRestart()
				}
				if err == nil {
					if enabled {
						t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.enableSuccess", "Email=="+email))
					} else {
						t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.disableSuccess", "Email=="+email))
					}
					t.searchClient(chatId, email, callbackQuery.Message.GetMessageID())
				} else {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.errorOperation"))
				}
			case "get_clients":
				inboundId := dataArray[1]
				inboundIdInt, err := strconv.Atoi(inboundId)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				inbound, err := t.inboundService.GetInbound(inboundIdInt)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				clients, err := t.getInboundClients(inboundIdInt)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseClient", "Inbound=="+inbound.Remark), clients)
			case "add_client_to":
				// assign default values to clients variables
				client_Id = uuid.New().String()
				client_Flow = ""
				client_Email = t.randomLowerAndNum(8)
				client_LimitIP = 0
				client_TotalGB = 0
				client_ExpiryTime = 0
				client_Enable = true
				client_TgID = ""
				client_SubID = t.randomLowerAndNum(16)
				client_Comment = ""
				client_Reset = 0
				client_Security = "auto"
				client_ShPassword = t.randomShadowSocksPassword()
				client_TrPassword = t.randomLowerAndNum(10)
				client_Method = ""

				inboundId := dataArray[1]
				inboundIdInt, err := strconv.Atoi(inboundId)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				receiver_inbound_ID = inboundIdInt
				inbound, err := t.inboundService.GetInbound(inboundIdInt)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}

				message_text, err := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}

				t.addClient(callbackQuery.Message.GetChat().ID, message_text)
			}
			return
		} else {
			switch callbackQuery.Data {
			case "get_inbounds":
				inbounds, err := t.getInbounds()
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return

				}
				t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.allClients"))
				t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseInbound"), inbounds)
			case "admin_client_sub_links":
				inbounds, err := t.getInboundsFor("get_clients_for_sub")
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseInbound"), inbounds)
			case "admin_client_individual_links":
				inbounds, err := t.getInboundsFor("get_clients_for_individual")
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseInbound"), inbounds)
			case "admin_client_qr_links":
				inbounds, err := t.getInboundsFor("get_clients_for_qr")
				if err != nil {
					t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
					return
				}
				t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseInbound"), inbounds)
			}

		}
	}

	switch callbackQuery.Data {
	case "get_usage":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.serverUsage"))
		t.getServerUsage(chatId)
	case "usage_refresh":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
		t.getServerUsage(chatId, callbackQuery.Message.GetMessageID())
	case "inbounds":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.getInbounds"))
		t.SendMsgToTgbot(chatId, t.getInboundUsages())
	case "deplete_soon":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.depleteSoon"))
		t.getExhausted(chatId)
	case "get_backup":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.dbBackup"))
		t.sendBackup(chatId)
	case "get_banlogs":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.getBanLogs"))
		t.sendBanLogs(chatId, true)
	case "client_traffic":
		tgUserID := callbackQuery.From.ID
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.clientUsage"))
		t.getClientUsage(chatId, tgUserID)
	case "client_commands":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.commands"))
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.commands.helpClientCommands"))
	case "client_sub_links":
		// show user's own clients to choose one for sub links
		tgUserID := callbackQuery.From.ID
		traffics, err := t.inboundService.GetClientTrafficTgBot(tgUserID)
		if err != nil {
			// fallback to message
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
			return
		}
		if len(traffics) == 0 {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.askToAddUserId", "TgUserID=="+strconv.FormatInt(tgUserID, 10)))
			return
		}
		var buttons []telego.InlineKeyboardButton
		for _, tr := range traffics {
			buttons = append(buttons, tu.InlineKeyboardButton(tr.Email).WithCallbackData(t.encodeQuery("client_sub_links "+tr.Email)))
		}
		cols := 1
		if len(buttons) >= 6 {
			cols = 2
		}
		keyboard := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols, buttons...))
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.commands.pleaseChoose"), keyboard)
	case "client_individual_links":
		// show user's clients to choose for individual links
		tgUserID := callbackQuery.From.ID
		traffics, err := t.inboundService.GetClientTrafficTgBot(tgUserID)
		if err != nil {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
			return
		}
		if len(traffics) == 0 {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.askToAddUserId", "TgUserID=="+strconv.FormatInt(tgUserID, 10)))
			return
		}
		var buttons2 []telego.InlineKeyboardButton
		for _, tr := range traffics {
			buttons2 = append(buttons2, tu.InlineKeyboardButton(tr.Email).WithCallbackData(t.encodeQuery("client_individual_links "+tr.Email)))
		}
		cols2 := 1
		if len(buttons2) >= 6 {
			cols2 = 2
		}
		keyboard2 := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols2, buttons2...))
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.commands.pleaseChoose"), keyboard2)
	case "client_qr_links":
		// show user's clients to choose for QR codes
		tgUserID := callbackQuery.From.ID
		traffics, err := t.inboundService.GetClientTrafficTgBot(tgUserID)
		if err != nil {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOccurred")+"\r\n"+err.Error())
			return
		}
		if len(traffics) == 0 {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.askToAddUserId", "TgUserID=="+strconv.FormatInt(tgUserID, 10)))
			return
		}
		var buttons3 []telego.InlineKeyboardButton
		for _, tr := range traffics {
			buttons3 = append(buttons3, tu.InlineKeyboardButton(tr.Email).WithCallbackData(t.encodeQuery("client_qr_links "+tr.Email)))
		}
		cols3 := 1
		if len(buttons3) >= 6 {
			cols3 = 2
		}
		keyboard3 := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols3, buttons3...))
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.commands.pleaseChoose"), keyboard3)
	case "onlines":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.onlines"))
		t.onlineClients(chatId)
	case "onlines_refresh":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.successfulOperation"))
		t.onlineClients(chatId, callbackQuery.Message.GetMessageID())
	case "commands":
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.commands"))
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.commands.helpAdminCommands"))
	case "add_client":
		// assign default values to clients variables
		client_Id = uuid.New().String()
		client_Flow = ""
		client_Email = t.randomLowerAndNum(8)
		client_LimitIP = 0
		client_TotalGB = 0
		client_ExpiryTime = 0
		client_Enable = true
		client_TgID = ""
		client_SubID = t.randomLowerAndNum(16)
		client_Comment = ""
		client_Reset = 0
		client_Security = "auto"
		client_ShPassword = t.randomShadowSocksPassword()
		client_TrPassword = t.randomLowerAndNum(10)
		client_Method = ""

		inbounds, err := t.getInboundsAddClient()
		if err != nil {
			t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
			return
		}
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.buttons.addClient"))
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.chooseInbound"), inbounds)
	case "add_client_ch_default_email":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		userStates[chatId] = "awaiting_email"
		cancel_btn_markup := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
			),
		)
		prompt_message := t.I18nBot("tgbot.messages.email_prompt", "ClientEmail=="+client_Email)
		t.SendMsgToTgbot(chatId, prompt_message, cancel_btn_markup)
	case "add_client_ch_default_id":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		userStates[chatId] = "awaiting_id"
		cancel_btn_markup := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
			),
		)
		prompt_message := t.I18nBot("tgbot.messages.id_prompt", "ClientId=="+client_Id)
		t.SendMsgToTgbot(chatId, prompt_message, cancel_btn_markup)
	case "add_client_ch_default_pass_tr":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		userStates[chatId] = "awaiting_password_tr"
		cancel_btn_markup := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
			),
		)
		prompt_message := t.I18nBot("tgbot.messages.pass_prompt", "ClientPassword=="+client_TrPassword)
		t.SendMsgToTgbot(chatId, prompt_message, cancel_btn_markup)
	case "add_client_ch_default_pass_sh":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		userStates[chatId] = "awaiting_password_sh"
		cancel_btn_markup := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
			),
		)
		prompt_message := t.I18nBot("tgbot.messages.pass_prompt", "ClientPassword=="+client_ShPassword)
		t.SendMsgToTgbot(chatId, prompt_message, cancel_btn_markup)
	case "add_client_ch_default_comment":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		userStates[chatId] = "awaiting_comment"
		cancel_btn_markup := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.use_default")).WithCallbackData("add_client_default_info"),
			),
		)
		prompt_message := t.I18nBot("tgbot.messages.comment_prompt", "ClientComment=="+client_Comment)
		t.SendMsgToTgbot(chatId, prompt_message, cancel_btn_markup)
	case "add_client_ch_default_traffic":
		inlineKeyboard := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("add_client_default_traffic_exp")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.unlimited")).WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 0")),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.custom")).WithCallbackData(t.encodeQuery("add_client_limit_traffic_in 0")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("1 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 1")),
				tu.InlineKeyboardButton("5 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 5")),
				tu.InlineKeyboardButton("10 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 10")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("20 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 20")),
				tu.InlineKeyboardButton("30 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 30")),
				tu.InlineKeyboardButton("40 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 40")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("50 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 50")),
				tu.InlineKeyboardButton("60 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 60")),
				tu.InlineKeyboardButton("80 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 80")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("100 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 100")),
				tu.InlineKeyboardButton("150 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 150")),
				tu.InlineKeyboardButton("200 GB").WithCallbackData(t.encodeQuery("add_client_limit_traffic_c 200")),
			),
		)
		t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
	case "add_client_ch_default_exp":
		inlineKeyboard := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("add_client_default_traffic_exp")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.unlimited")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 0")),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.custom")).WithCallbackData(t.encodeQuery("add_client_reset_exp_in 0")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 7 "+t.I18nBot("tgbot.days")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 7")),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 10 "+t.I18nBot("tgbot.days")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 10")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 14 "+t.I18nBot("tgbot.days")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 14")),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 20 "+t.I18nBot("tgbot.days")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 20")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 1 "+t.I18nBot("tgbot.month")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 30")),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 3 "+t.I18nBot("tgbot.months")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 90")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 6 "+t.I18nBot("tgbot.months")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 180")),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.add")+" 12 "+t.I18nBot("tgbot.months")).WithCallbackData(t.encodeQuery("add_client_reset_exp_c 365")),
			),
		)
		t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
	case "add_client_ch_default_ip_limit":
		inlineKeyboard := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData(t.encodeQuery("add_client_default_ip_limit")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.unlimited")).WithCallbackData(t.encodeQuery("add_client_ip_limit_c 0")),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.custom")).WithCallbackData(t.encodeQuery("add_client_ip_limit_in 0")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("1").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 1")),
				tu.InlineKeyboardButton("2").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 2")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("3").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 3")),
				tu.InlineKeyboardButton("4").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 4")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("5").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 5")),
				tu.InlineKeyboardButton("6").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 6")),
				tu.InlineKeyboardButton("7").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 7")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton("8").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 8")),
				tu.InlineKeyboardButton("9").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 9")),
				tu.InlineKeyboardButton("10").WithCallbackData(t.encodeQuery("add_client_ip_limit_c 10")),
			),
		)
		t.editMessageCallbackTgBot(chatId, callbackQuery.Message.GetMessageID(), inlineKeyboard)
	case "add_client_default_info":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		t.SendMsgToTgbotDeleteAfter(chatId, t.I18nBot("tgbot.messages.using_default_value"), 3, tu.ReplyKeyboardRemove())
		delete(userStates, chatId)
		inbound, _ := t.inboundService.GetInbound(receiver_inbound_ID)
		message_text, _ := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
		t.addClient(chatId, message_text)
	case "add_client_cancel":
		delete(userStates, chatId)
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		t.SendMsgToTgbotDeleteAfter(chatId, t.I18nBot("tgbot.messages.cancel"), 3, tu.ReplyKeyboardRemove())
	case "add_client_default_traffic_exp":
		messageId := callbackQuery.Message.GetMessageID()
		inbound, err := t.inboundService.GetInbound(receiver_inbound_ID)
		if err != nil {
			t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
			return
		}
		message_text, err := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
		if err != nil {
			t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
			return
		}
		t.addClient(chatId, message_text, messageId)
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.canceled", "Email=="+client_Email))
	case "add_client_default_ip_limit":
		messageId := callbackQuery.Message.GetMessageID()
		inbound, err := t.inboundService.GetInbound(receiver_inbound_ID)
		if err != nil {
			t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
			return
		}
		message_text, err := t.BuildInboundClientDataMessage(inbound.Remark, inbound.Protocol)
		if err != nil {
			t.sendCallbackAnswerTgBot(callbackQuery.ID, err.Error())
			return
		}
		t.addClient(chatId, message_text, messageId)
		t.sendCallbackAnswerTgBot(callbackQuery.ID, t.I18nBot("tgbot.answers.canceled", "Email=="+client_Email))
	case "add_client_submit_disable":
		client_Enable = false
		_, err := t.SubmitAddClient()
		if err != nil {
			errorMessage := fmt.Sprintf("%v", err)
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.messages.error_add_client", "error=="+errorMessage), tu.ReplyKeyboardRemove())
		} else {
			t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.successfulOperation"), tu.ReplyKeyboardRemove())
			t.sendClientIndividualLinks(chatId, client_Email)
			t.sendClientQRLinks(chatId, client_Email)
		}
	case "add_client_submit_enable":
		client_Enable = true
		_, err := t.SubmitAddClient()
		if err != nil {
			errorMessage := fmt.Sprintf("%v", err)
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.messages.error_add_client", "error=="+errorMessage), tu.ReplyKeyboardRemove())
		} else {
			t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.successfulOperation"), tu.ReplyKeyboardRemove())
			t.sendClientIndividualLinks(chatId, client_Email)
			t.sendClientQRLinks(chatId, client_Email)
		}
	case "reset_all_traffics_cancel":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		t.SendMsgToTgbotDeleteAfter(chatId, t.I18nBot("tgbot.messages.cancel"), 1, tu.ReplyKeyboardRemove())
	case "reset_all_traffics":
		inlineKeyboard := tu.InlineKeyboard(
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancelReset")).WithCallbackData(t.encodeQuery("reset_all_traffics_cancel")),
			),
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.confirmResetTraffic")).WithCallbackData(t.encodeQuery("reset_all_traffics_c")),
			),
		)
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.messages.AreYouSure"), inlineKeyboard)
	case "reset_all_traffics_c":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		emails, err := t.inboundService.getAllEmails()
		if err != nil {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation"), tu.ReplyKeyboardRemove())
			return
		}

		for _, email := range emails {
			err := t.inboundService.ResetClientTrafficByEmail(email)
			if err == nil {
				msg := t.I18nBot("tgbot.messages.SuccessResetTraffic", "ClientEmail=="+email)
				t.SendMsgToTgbot(chatId, msg, tu.ReplyKeyboardRemove())
			} else {
				msg := t.I18nBot("tgbot.messages.FailedResetTraffic", "ClientEmail=="+email, "ErrorMessage=="+err.Error())
				t.SendMsgToTgbot(chatId, msg, tu.ReplyKeyboardRemove())
			}
		}

		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.messages.FinishProcess"), tu.ReplyKeyboardRemove())
	case "get_sorted_traffic_usage_report":
		t.deleteMessageTgBot(chatId, callbackQuery.Message.GetMessageID())
		emails, err := t.inboundService.getAllEmails()

		if err != nil {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation"), tu.ReplyKeyboardRemove())
			return
		}
		valid_emails, extra_emails, err := t.inboundService.FilterAndSortClientEmails(emails)
		if err != nil {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation"), tu.ReplyKeyboardRemove())
			return
		}

		for _, valid_emails := range valid_emails {
			traffic, err := t.inboundService.GetClientTrafficByEmail(valid_emails)
			if err != nil {
				logger.Warning(err)
				msg := t.I18nBot("tgbot.wentWrong")
				t.SendMsgToTgbot(chatId, msg)
				continue
			}
			if traffic == nil {
				msg := t.I18nBot("tgbot.noResult")
				t.SendMsgToTgbot(chatId, msg)
				continue
			}

			output := t.clientInfoMsg(traffic, false, false, false, false, true, false)
			t.SendMsgToTgbot(chatId, output, tu.ReplyKeyboardRemove())
		}
		for _, extra_emails := range extra_emails {
			msg := fmt.Sprintf("📧 %s\n%s", extra_emails, t.I18nBot("tgbot.noResult"))
			t.SendMsgToTgbot(chatId, msg, tu.ReplyKeyboardRemove())

		}
	default:
		if after, ok := strings.CutPrefix(callbackQuery.Data, "client_sub_links "); ok {
			email := after
			t.sendClientSubLinks(chatId, email)
			return
		}
		if after, ok := strings.CutPrefix(callbackQuery.Data, "client_individual_links "); ok {
			email := after
			t.sendClientIndividualLinks(chatId, email)
			return
		}
		if after, ok := strings.CutPrefix(callbackQuery.Data, "client_qr_links "); ok {
			email := after
			t.sendClientQRLinks(chatId, email)
			return
		}
	}
}

// BuildInboundClientDataMessage builds a message with client data for the given inbound and protocol.
func (t *Tgbot) BuildInboundClientDataMessage(inbound_remark string, protocol model.Protocol) (string, error) {
	var message string

	currentTime := time.Now()
	timestampMillis := currentTime.UnixNano() / int64(time.Millisecond)

	expiryTime := ""
	diff := client_ExpiryTime/1000 - timestampMillis
	if client_ExpiryTime == 0 {
		expiryTime = t.I18nBot("tgbot.unlimited")
	} else if diff > 172800 {
		expiryTime = time.Unix((client_ExpiryTime / 1000), 0).Format("2006-01-02 15:04:05")
	} else if client_ExpiryTime < 0 {
		expiryTime = fmt.Sprintf("%d %s", client_ExpiryTime/-86400000, t.I18nBot("tgbot.days"))
	} else {
		expiryTime = fmt.Sprintf("%d %s", diff/3600, t.I18nBot("tgbot.hours"))
	}

	traffic_value := ""
	if client_TotalGB == 0 {
		traffic_value = "♾️ Unlimited(Reset)"
	} else {
		traffic_value = common.FormatTraffic(client_TotalGB)
	}

	ip_limit := ""
	if client_LimitIP == 0 {
		ip_limit = "♾️ Unlimited(Reset)"
	} else {
		ip_limit = fmt.Sprint(client_LimitIP)
	}

	switch protocol {
	case model.VMESS, model.VLESS:
		message = t.I18nBot("tgbot.messages.inbound_client_data_id", "InboundRemark=="+inbound_remark, "ClientId=="+client_Id, "ClientEmail=="+client_Email, "ClientTraffic=="+traffic_value, "ClientExp=="+expiryTime, "IpLimit=="+ip_limit, "ClientComment=="+client_Comment)

	case model.Trojan:
		message = t.I18nBot("tgbot.messages.inbound_client_data_pass", "InboundRemark=="+inbound_remark, "ClientPass=="+client_TrPassword, "ClientEmail=="+client_Email, "ClientTraffic=="+traffic_value, "ClientExp=="+expiryTime, "IpLimit=="+ip_limit, "ClientComment=="+client_Comment)

	case model.Shadowsocks:
		message = t.I18nBot("tgbot.messages.inbound_client_data_pass", "InboundRemark=="+inbound_remark, "ClientPass=="+client_ShPassword, "ClientEmail=="+client_Email, "ClientTraffic=="+traffic_value, "ClientExp=="+expiryTime, "IpLimit=="+ip_limit, "ClientComment=="+client_Comment)

	default:
		return "", errors.New("unknown protocol")
	}

	return message, nil
}

// BuildJSONForProtocol builds a JSON string for the given protocol with client data.
func (t *Tgbot) BuildJSONForProtocol(protocol model.Protocol) (string, error) {
	var jsonString string

	switch protocol {
	case model.VMESS:
		jsonString = fmt.Sprintf(`{
            "clients": [{
                "id": "%s",
                "security": "%s",
                "email": "%s",
                "limitIp": %d,
                "totalGB": %d,
                "expiryTime": %d,
                "enable": %t,
                "tgId": "%s",
                "subId": "%s",
                "comment": "%s",
                "reset": %d
            }]
        }`, client_Id, client_Security, client_Email, client_LimitIP, client_TotalGB, client_ExpiryTime, client_Enable, client_TgID, client_SubID, client_Comment, client_Reset)

	case model.VLESS:
		jsonString = fmt.Sprintf(`{
            "clients": [{
                "id": "%s",
                "flow": "%s",
                "email": "%s",
                "limitIp": %d,
                "totalGB": %d,
                "expiryTime": %d,
                "enable": %t,
                "tgId": "%s",
                "subId": "%s",
                "comment": "%s",
                "reset": %d
            }]
        }`, client_Id, client_Flow, client_Email, client_LimitIP, client_TotalGB, client_ExpiryTime, client_Enable, client_TgID, client_SubID, client_Comment, client_Reset)

	case model.Trojan:
		jsonString = fmt.Sprintf(`{
            "clients": [{
                "password": "%s",
                "email": "%s",
                "limitIp": %d,
                "totalGB": %d,
                "expiryTime": %d,
                "enable": %t,
                "tgId": "%s",
                "subId": "%s",
                "comment": "%s",
                "reset": %d
            }]
        }`, client_TrPassword, client_Email, client_LimitIP, client_TotalGB, client_ExpiryTime, client_Enable, client_TgID, client_SubID, client_Comment, client_Reset)

	case model.Shadowsocks:
		jsonString = fmt.Sprintf(`{
            "clients": [{
                "method": "%s",
                "password": "%s",
                "email": "%s",
                "limitIp": %d,
                "totalGB": %d,
                "expiryTime": %d,
                "enable": %t,
                "tgId": "%s",
                "subId": "%s",
                "comment": "%s",
                "reset": %d
            }]
        }`, client_Method, client_ShPassword, client_Email, client_LimitIP, client_TotalGB, client_ExpiryTime, client_Enable, client_TgID, client_SubID, client_Comment, client_Reset)

	default:
		return "", errors.New("unknown protocol")
	}

	return jsonString, nil
}

// SubmitAddClient submits the client addition request to the inbound service.
func (t *Tgbot) SubmitAddClient() (bool, error) {

	inbound, err := t.inboundService.GetInbound(receiver_inbound_ID)
	if err != nil {
		logger.Warning("getIboundClients run failed:", err)
		return false, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}

	jsonString, err := t.BuildJSONForProtocol(inbound.Protocol)
	if err != nil {
		logger.Warning("BuildJSONForProtocol run failed:", err)
		return false, errors.New("failed to build JSON for protocol")
	}

	newInbound := &model.Inbound{
		Id:       receiver_inbound_ID,
		Settings: jsonString,
	}

	return t.inboundService.AddInboundClient(newInbound)
}

// checkAdmin checks if the given Telegram ID is an admin.
func checkAdmin(tgId int64) bool {
	for _, adminId := range adminIds {
		if adminId == tgId {
			return true
		}
	}
	return false
}

// SendAnswer sends a response message with an inline keyboard to the specified chat.
func (t *Tgbot) SendAnswer(chatId int64, msg string, isAdmin bool) {
	numericKeyboard := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.SortedTrafficUsageReport")).WithCallbackData(t.encodeQuery("get_sorted_traffic_usage_report")),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.serverUsage")).WithCallbackData(t.encodeQuery("get_usage")),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.ResetAllTraffics")).WithCallbackData(t.encodeQuery("reset_all_traffics")),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.dbBackup")).WithCallbackData(t.encodeQuery("get_backup")),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.getBanLogs")).WithCallbackData(t.encodeQuery("get_banlogs")),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.getInbounds")).WithCallbackData(t.encodeQuery("inbounds")),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.depleteSoon")).WithCallbackData(t.encodeQuery("deplete_soon")),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.commands")).WithCallbackData(t.encodeQuery("commands")),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.onlines")).WithCallbackData(t.encodeQuery("onlines")),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.allClients")).WithCallbackData(t.encodeQuery("get_inbounds")),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.addClient")).WithCallbackData(t.encodeQuery("add_client")),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("pages.settings.subSettings")).WithCallbackData(t.encodeQuery("admin_client_sub_links")),
			tu.InlineKeyboardButton(t.I18nBot("subscription.individualLinks")).WithCallbackData(t.encodeQuery("admin_client_individual_links")),
			tu.InlineKeyboardButton(t.I18nBot("qrCode")).WithCallbackData(t.encodeQuery("admin_client_qr_links")),
		),
		// TODOOOOOOOOOOOOOO: Add restart button here.
	)
	numericKeyboardClient := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.clientUsage")).WithCallbackData(t.encodeQuery("client_traffic")),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.commands")).WithCallbackData(t.encodeQuery("client_commands")),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("pages.settings.subSettings")).WithCallbackData(t.encodeQuery("client_sub_links")),
			tu.InlineKeyboardButton(t.I18nBot("subscription.individualLinks")).WithCallbackData(t.encodeQuery("client_individual_links")),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("qrCode")).WithCallbackData(t.encodeQuery("client_qr_links")),
		),
	)

	var ReplyMarkup telego.ReplyMarkup
	if isAdmin {
		ReplyMarkup = numericKeyboard
	} else {
		ReplyMarkup = numericKeyboardClient
	}
	t.SendMsgToTgbot(chatId, msg, ReplyMarkup)
}

// SendMsgToTgbot sends a message to the Telegram bot with optional reply markup.
func (t *Tgbot) SendMsgToTgbot(chatId int64, msg string, replyMarkup ...telego.ReplyMarkup) {
	if !isRunning {
		return
	}

	if msg == "" {
		logger.Info("[tgbot] message is empty!")
		return
	}

	var allMessages []string
	limit := 2000

	// paging message if it is big
	if len(msg) > limit {
		messages := strings.Split(msg, "\r\n\r\n")
		lastIndex := -1

		for _, message := range messages {
			if (len(allMessages) == 0) || (len(allMessages[lastIndex])+len(message) > limit) {
				allMessages = append(allMessages, message)
				lastIndex++
			} else {
				allMessages[lastIndex] += "\r\n\r\n" + message
			}
		}
		if strings.TrimSpace(allMessages[len(allMessages)-1]) == "" {
			allMessages = allMessages[:len(allMessages)-1]
		}
	} else {
		allMessages = append(allMessages, msg)
	}
	for n, message := range allMessages {
		params := telego.SendMessageParams{
			ChatID:    tu.ID(chatId),
			Text:      message,
			ParseMode: "HTML",
		}
		// only add replyMarkup to last message
		if len(replyMarkup) > 0 && n == (len(allMessages)-1) {
			params.ReplyMarkup = replyMarkup[0]
		}

		// Retry logic with exponential backoff for connection errors
		maxRetries := 3
		for attempt := range maxRetries {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := bot.SendMessage(ctx, &params)
			cancel()

			if err == nil {
				break // Success
			}

			// Check if error is a connection error
			errStr := err.Error()
			isConnectionError := strings.Contains(errStr, "connection") ||
				strings.Contains(errStr, "timeout") ||
				strings.Contains(errStr, "closed")

			if isConnectionError && attempt < maxRetries-1 {
				// Exponential backoff: 1s, 2s, 4s
				backoff := time.Duration(1<<uint(attempt)) * time.Second
				logger.Warningf("Connection error sending telegram message (attempt %d/%d), retrying in %v: %v",
					attempt+1, maxRetries, backoff, err)
				time.Sleep(backoff)
			} else {
				logger.Warning("Error sending telegram message:", err)
				break
			}
		}

		// Reduced delay to improve performance (only needed for rate limiting)
		if n < len(allMessages)-1 { // Only delay between messages, not after the last one
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// buildSubscriptionURLs builds the HTML sub page URL and JSON subscription URL for a client email
func (t *Tgbot) buildSubscriptionURLs(email string) (string, string, error) {
	// Resolve subId from client email
	traffic, client, err := t.inboundService.GetClientByEmail(email)
	_ = traffic
	if err != nil || client == nil {
		return "", "", errors.New("client not found")
	}

	// Gather settings to construct absolute URLs
	subURI, _ := t.settingService.GetSubURI()
	subJsonURI, _ := t.settingService.GetSubJsonURI()
	subDomain, _ := t.settingService.GetSubDomain()
	subPort, _ := t.settingService.GetSubPort()
	subPath, _ := t.settingService.GetSubPath()
	subJsonPath, _ := t.settingService.GetSubJsonPath()
	subJsonEnable, _ := t.settingService.GetSubJsonEnable()
	subKeyFile, _ := t.settingService.GetSubKeyFile()
	subCertFile, _ := t.settingService.GetSubCertFile()

	tls := (subKeyFile != "" && subCertFile != "")
	scheme := "http"
	if tls {
		scheme = "https"
	}

	// Fallbacks
	if subDomain == "" {
		// try panel domain, otherwise OS hostname
		if d, err := t.settingService.GetWebDomain(); err == nil && d != "" {
			subDomain = d
		} else if hostname != "" {
			subDomain = hostname
		} else {
			subDomain = "localhost"
		}
	}

	host := subDomain
	if (subPort == 443 && tls) || (subPort == 80 && !tls) {
		// standard ports: no port in host
	} else {
		host = fmt.Sprintf("%s:%d", subDomain, subPort)
	}

	// Ensure paths
	if !strings.HasPrefix(subPath, "/") {
		subPath = "/" + subPath
	}
	if !strings.HasSuffix(subPath, "/") {
		subPath = subPath + "/"
	}
	if !strings.HasPrefix(subJsonPath, "/") {
		subJsonPath = "/" + subJsonPath
	}
	if !strings.HasSuffix(subJsonPath, "/") {
		subJsonPath = subJsonPath + "/"
	}

	var subURL string
	var subJsonURL string

	// If pre-configured URIs are available, use them directly
	if subURI != "" {
		if !strings.HasSuffix(subURI, "/") {
			subURI = subURI + "/"
		}
		subURL = fmt.Sprintf("%s%s", subURI, client.SubID)
	} else {
		subURL = fmt.Sprintf("%s://%s%s%s", scheme, host, subPath, client.SubID)
	}

	if subJsonURI != "" {
		if !strings.HasSuffix(subJsonURI, "/") {
			subJsonURI = subJsonURI + "/"
		}
		subJsonURL = fmt.Sprintf("%s%s", subJsonURI, client.SubID)
	} else {

		subJsonURL = fmt.Sprintf("%s://%s%s%s", scheme, host, subJsonPath, client.SubID)
	}

	if !subJsonEnable {
		subJsonURL = ""
	}
	return subURL, subJsonURL, nil
}

// sendClientSubLinks sends the subscription links for the client to the chat.
func (t *Tgbot) sendClientSubLinks(chatId int64, email string) {
	subURL, subJsonURL, err := t.buildSubscriptionURLs(email)
	if err != nil {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
		return
	}
	msg := "Subscription URL:\r\n<code>" + subURL + "</code>"
	if subJsonURL != "" {
		msg += "\r\n\r\nJSON URL:\r\n<code>" + subJsonURL + "</code>"
	}
	inlineKeyboard := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("subscription.individualLinks")).WithCallbackData(t.encodeQuery("client_individual_links "+email)),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("qrCode")).WithCallbackData(t.encodeQuery("client_qr_links "+email)),
		),
	)
	t.SendMsgToTgbot(chatId, msg, inlineKeyboard)
}

// sendClientIndividualLinks fetches the subscription content (individual links) and sends it to the user
func (t *Tgbot) sendClientIndividualLinks(chatId int64, email string) {
	// Build the HTML sub page URL; we'll call it with header Accept to get raw content
	subURL, _, err := t.buildSubscriptionURLs(email)
	if err != nil {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
		return
	}

	// Try to fetch raw subscription links. Prefer plain text response.
	req, err := http.NewRequest("GET", subURL, nil)
	if err != nil {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
		return
	}
	// Force plain text to avoid HTML page; controller respects Accept header
	req.Header.Set("Accept", "text/plain, */*;q=0.1")

	// Use optimized client with connection pooling
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := optimizedHTTPClient.Do(req)
	if err != nil {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
		return
	}

	// If service is configured to encode (Base64), decode it
	encoded, _ := t.settingService.GetSubEncrypt()
	var content string
	if encoded {
		decoded, err := base64.StdEncoding.DecodeString(string(bodyBytes))
		if err != nil {
			// fallback to raw text
			content = string(bodyBytes)
		} else {
			content = string(decoded)
		}
	} else {
		content = string(bodyBytes)
	}

	// Normalize line endings and trim
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	var cleaned []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			cleaned = append(cleaned, l)
		}
	}
	if len(cleaned) == 0 {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.noResult"))
		return
	}

	// Send in chunks to respect message length; use monospace formatting
	const maxPerMessage = 50
	for i := 0; i < len(cleaned); i += maxPerMessage {
		j := i + maxPerMessage
		if j > len(cleaned) {
			j = len(cleaned)
		}
		chunk := cleaned[i:j]
		msg := t.I18nBot("subscription.individualLinks") + ":\r\n"
		for _, link := range chunk {
			// wrap each link in <code>
			msg += "<code>" + link + "</code>\r\n"
		}
		t.SendMsgToTgbot(chatId, msg)
	}
}

// sendClientQRLinks generates QR images for subscription URL, JSON URL, and a few individual links, then sends them
func (t *Tgbot) sendClientQRLinks(chatId int64, email string) {
	subURL, subJsonURL, err := t.buildSubscriptionURLs(email)
	if err != nil {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
		return
	}

	// Helper to create QR PNG bytes from content
	createQR := func(content string, size int) ([]byte, error) {
		if size <= 0 {
			size = 256
		}
		return qrcode.Encode(content, qrcode.Medium, size)
	}

	// Inform user
	t.SendMsgToTgbot(chatId, "QRCode"+":")

	// Send sub URL QR (filename: sub.png)
	if png, err := createQR(subURL, 320); err == nil {
		document := tu.Document(
			tu.ID(chatId),
			tu.FileFromBytes(png, "sub.png"),
		)
		_, _ = bot.SendDocument(context.Background(), document)
	} else {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
	}

	// Send JSON URL QR (filename: subjson.png) when available
	if subJsonURL != "" {
		if png, err := createQR(subJsonURL, 320); err == nil {
			document := tu.Document(
				tu.ID(chatId),
				tu.FileFromBytes(png, "subjson.png"),
			)
			_, _ = bot.SendDocument(context.Background(), document)
		} else {
			t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.errorOperation")+"\r\n"+err.Error())
		}
	}

	// Also generate a few individual links' QRs (first up to 5)
	subPageURL := subURL
	req, err := http.NewRequest("GET", subPageURL, nil)
	if err == nil {
		req.Header.Set("Accept", "text/plain, */*;q=0.1")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req = req.WithContext(ctx)
		if resp, err := optimizedHTTPClient.Do(req); err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			encoded, _ := t.settingService.GetSubEncrypt()
			var content string
			if encoded {
				if dec, err := base64.StdEncoding.DecodeString(string(body)); err == nil {
					content = string(dec)
				} else {
					content = string(body)
				}
			} else {
				content = string(body)
			}
			lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
			var cleaned []string
			for _, l := range lines {
				l = strings.TrimSpace(l)
				if l != "" {
					cleaned = append(cleaned, l)
				}
			}
			if len(cleaned) > 0 {
				max := min(len(cleaned), 5)
				for i := range max {
					if png, err := createQR(cleaned[i], 320); err == nil {
						// Use the email as filename for individual link QR
						filename := email + ".png"
						document := tu.Document(
							tu.ID(chatId),
							tu.FileFromBytes(png, filename),
						)
						_, _ = bot.SendDocument(context.Background(), document)
						// Reduced delay for better performance
						if i < max-1 { // Only delay between documents, not after the last one
							time.Sleep(50 * time.Millisecond)
						}
					}
				}
			}
		}
	}
}

// SendMsgToTgbotAdmins sends a message to all admin Telegram chats.
func (t *Tgbot) SendMsgToTgbotAdmins(msg string, replyMarkup ...telego.ReplyMarkup) {
	if len(replyMarkup) > 0 {
		for _, adminId := range adminIds {
			t.SendMsgToTgbot(adminId, msg, replyMarkup[0])
		}
	} else {
		for _, adminId := range adminIds {
			t.SendMsgToTgbot(adminId, msg)
		}
	}
}

// SendReport sends a periodic report to admin chats.
func (t *Tgbot) SendReport() {
	runTime, err := t.settingService.GetTgbotRuntime()
	if err == nil && len(runTime) > 0 {
		msg := ""
		msg += t.I18nBot("tgbot.messages.report", "RunTime=="+runTime)
		msg += t.I18nBot("tgbot.messages.datetime", "DateTime=="+time.Now().Format("2006-01-02 15:04:05"))
		t.SendMsgToTgbotAdmins(msg)
	}

	info := t.sendServerUsage()
	t.SendMsgToTgbotAdmins(info)

	t.sendExhaustedToAdmins()
	t.notifyExhausted()

	backupEnable, err := t.settingService.GetTgBotBackup()
	if err == nil && backupEnable {
		t.SendBackupToAdmins()
	}
}

// SendBackupToAdmins sends a database backup to admin chats.
func (t *Tgbot) SendBackupToAdmins() {
	if !t.IsRunning() {
		return
	}
	for i, adminId := range adminIds {
		t.sendBackup(int64(adminId))
		// Add delay between sends to avoid Telegram rate limits
		if i < len(adminIds)-1 {
			time.Sleep(1 * time.Second)
		}
	}
}

// sendExhaustedToAdmins sends notifications about exhausted clients to admins.
func (t *Tgbot) sendExhaustedToAdmins() {
	if !t.IsRunning() {
		return
	}
	for _, adminId := range adminIds {
		t.getExhausted(int64(adminId))
	}
}

// getServerUsage retrieves and formats server usage information.
func (t *Tgbot) getServerUsage(chatId int64, messageID ...int) string {
	info := t.prepareServerUsageInfo()

	keyboard := tu.InlineKeyboard(tu.InlineKeyboardRow(
		tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.refresh")).WithCallbackData(t.encodeQuery("usage_refresh"))))

	if len(messageID) > 0 {
		t.editMessageTgBot(chatId, messageID[0], info, keyboard)
	} else {
		t.SendMsgToTgbot(chatId, info, keyboard)
	}

	return info
}

// Send server usage without an inline keyboard
func (t *Tgbot) sendServerUsage() string {
	info := t.prepareServerUsageInfo()
	return info
}

// prepareServerUsageInfo prepares the server usage information string.
func (t *Tgbot) prepareServerUsageInfo() string {
	// Check if we have cached data first
	if cachedStats, found := t.getCachedServerStats(); found {
		return cachedStats
	}

	info, ipv4, ipv6 := "", "", ""

	// get latest status of server with caching
	if cachedStatus, found := t.getCachedStatus(); found {
		t.lastStatus = cachedStatus
	} else {
		t.lastStatus = t.serverService.GetStatus(t.lastStatus)
		t.setCachedStatus(t.lastStatus)
	}
	onlines := p.GetOnlineClients()

	info += t.I18nBot("tgbot.messages.hostname", "Hostname=="+hostname)
	info += t.I18nBot("tgbot.messages.version", "Version=="+config.GetVersion())
	info += t.I18nBot("tgbot.messages.xrayVersion", "XrayVersion=="+fmt.Sprint(t.lastStatus.Xray.Version))

	// get ip address
	netInterfaces, err := net.Interfaces()
	if err != nil {
		logger.Error("net.Interfaces failed, err: ", err.Error())
		info += t.I18nBot("tgbot.messages.ip", "IP=="+t.I18nBot("tgbot.unknown"))
		info += "\r\n"
	} else {
		for i := range netInterfaces {
			if (netInterfaces[i].Flags & net.FlagUp) != 0 {
				addrs, _ := netInterfaces[i].Addrs()

				for _, address := range addrs {
					if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
						if ipnet.IP.To4() != nil {
							ipv4 += ipnet.IP.String() + " "
						} else if ipnet.IP.To16() != nil && !ipnet.IP.IsLinkLocalUnicast() {
							ipv6 += ipnet.IP.String() + " "
						}
					}
				}
			}
		}

		info += t.I18nBot("tgbot.messages.ipv4", "IPv4=="+ipv4)
		info += t.I18nBot("tgbot.messages.ipv6", "IPv6=="+ipv6)
	}

	info += t.I18nBot("tgbot.messages.serverUpTime", "UpTime=="+strconv.FormatUint(t.lastStatus.Uptime/86400, 10), "Unit=="+t.I18nBot("tgbot.days"))
	info += t.I18nBot("tgbot.messages.serverLoad", "Load1=="+strconv.FormatFloat(t.lastStatus.Loads[0], 'f', 2, 64), "Load2=="+strconv.FormatFloat(t.lastStatus.Loads[1], 'f', 2, 64), "Load3=="+strconv.FormatFloat(t.lastStatus.Loads[2], 'f', 2, 64))
	info += t.I18nBot("tgbot.messages.serverMemory", "Current=="+common.FormatTraffic(int64(t.lastStatus.Mem.Current)), "Total=="+common.FormatTraffic(int64(t.lastStatus.Mem.Total)))
	info += t.I18nBot("tgbot.messages.onlinesCount", "Count=="+fmt.Sprint(len(onlines)))
	info += t.I18nBot("tgbot.messages.tcpCount", "Count=="+strconv.Itoa(t.lastStatus.TcpCount))
	info += t.I18nBot("tgbot.messages.udpCount", "Count=="+strconv.Itoa(t.lastStatus.UdpCount))
	info += t.I18nBot("tgbot.messages.traffic", "Total=="+common.FormatTraffic(int64(t.lastStatus.NetTraffic.Sent+t.lastStatus.NetTraffic.Recv)), "Upload=="+common.FormatTraffic(int64(t.lastStatus.NetTraffic.Sent)), "Download=="+common.FormatTraffic(int64(t.lastStatus.NetTraffic.Recv)))
	info += t.I18nBot("tgbot.messages.xrayStatus", "State=="+fmt.Sprint(t.lastStatus.Xray.State))

	// Cache the complete server stats
	t.setCachedServerStats(info)

	return info
}

// UserLoginNotify sends a notification about user login attempts to admins.
func (t *Tgbot) UserLoginNotify(username string, password string, ip string, time string, status LoginStatus) {
	if !t.IsRunning() {
		return
	}

	if username == "" || ip == "" || time == "" {
		logger.Warning("UserLoginNotify failed, invalid info!")
		return
	}

	loginNotifyEnabled, err := t.settingService.GetTgBotLoginNotify()
	if err != nil || !loginNotifyEnabled {
		return
	}

	msg := ""
	switch status {
	case LoginSuccess:
		msg += t.I18nBot("tgbot.messages.loginSuccess")
		msg += t.I18nBot("tgbot.messages.hostname", "Hostname=="+hostname)
	case LoginFail:
		msg += t.I18nBot("tgbot.messages.loginFailed")
		msg += t.I18nBot("tgbot.messages.hostname", "Hostname=="+hostname)
		msg += t.I18nBot("tgbot.messages.password", "Password=="+password)
	}
	msg += t.I18nBot("tgbot.messages.username", "Username=="+username)
	msg += t.I18nBot("tgbot.messages.ip", "IP=="+ip)
	msg += t.I18nBot("tgbot.messages.time", "Time=="+time)
	t.SendMsgToTgbotAdmins(msg)
}

// getInboundUsages retrieves and formats inbound usage information.
func (t *Tgbot) getInboundUsages() string {
	var info strings.Builder
	// get traffic
	inbounds, err := t.inboundService.GetAllInbounds()
	if err != nil {
		logger.Warning("GetAllInbounds run failed:", err)
		info.WriteString(t.I18nBot("tgbot.answers.getInboundsFailed"))
	} else {
		// NOTE:If there no any sessions here,need to notify here
		// TODO:Sub-node push, automatic conversion format
		for _, inbound := range inbounds {
			info.WriteString(t.I18nBot("tgbot.messages.inbound", "Remark=="+inbound.Remark))
			info.WriteString(t.I18nBot("tgbot.messages.port", "Port=="+strconv.Itoa(inbound.Port)))
			info.WriteString(t.I18nBot("tgbot.messages.traffic", "Total=="+common.FormatTraffic((inbound.Up+inbound.Down)), "Upload=="+common.FormatTraffic(inbound.Up), "Download=="+common.FormatTraffic(inbound.Down)))

			if inbound.ExpiryTime == 0 {
				info.WriteString(t.I18nBot("tgbot.messages.expire", "Time=="+t.I18nBot("tgbot.unlimited")))
			} else {
				info.WriteString(t.I18nBot("tgbot.messages.expire", "Time=="+time.Unix((inbound.ExpiryTime/1000), 0).Format("2006-01-02 15:04:05")))
			}
			info.WriteString("\r\n")
		}
	}
	return info.String()
}

// getInbounds creates an inline keyboard with all inbounds.
func (t *Tgbot) getInbounds() (*telego.InlineKeyboardMarkup, error) {
	inbounds, err := t.inboundService.GetAllInbounds()
	if err != nil {
		logger.Warning("GetAllInbounds run failed:", err)
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}

	if len(inbounds) == 0 {
		logger.Warning("No inbounds found")
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}

	var buttons []telego.InlineKeyboardButton
	for _, inbound := range inbounds {
		status := "❌"
		if inbound.Enable {
			status = "✅"
		}
		callbackData := t.encodeQuery(fmt.Sprintf("%s %d", "get_clients", inbound.Id))
		buttons = append(buttons, tu.InlineKeyboardButton(fmt.Sprintf("%v - %v", inbound.Remark, status)).WithCallbackData(callbackData))
	}

	cols := 1
	if len(buttons) >= 6 {
		cols = 2
	}

	keyboard := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols, buttons...))
	return keyboard, nil
}

// getInboundsFor builds an inline keyboard of inbounds for a custom next action.
func (t *Tgbot) getInboundsFor(nextAction string) (*telego.InlineKeyboardMarkup, error) {
	inbounds, err := t.inboundService.GetAllInbounds()
	if err != nil {
		logger.Warning("GetAllInbounds run failed:", err)
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}

	if len(inbounds) == 0 {
		logger.Warning("No inbounds found")
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}

	var buttons []telego.InlineKeyboardButton
	for _, inbound := range inbounds {
		status := "❌"
		if inbound.Enable {
			status = "✅"
		}
		callbackData := t.encodeQuery(fmt.Sprintf("%s %d", nextAction, inbound.Id))
		buttons = append(buttons, tu.InlineKeyboardButton(fmt.Sprintf("%v - %v", inbound.Remark, status)).WithCallbackData(callbackData))
	}

	cols := 1
	if len(buttons) >= 6 {
		cols = 2
	}

	keyboard := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols, buttons...))
	return keyboard, nil
}

// getInboundClientsFor lists clients of an inbound with a specific action prefix to be appended with email
func (t *Tgbot) getInboundClientsFor(inboundID int, action string) (*telego.InlineKeyboardMarkup, error) {
	inbound, err := t.inboundService.GetInbound(inboundID)
	if err != nil {
		logger.Warning("getInboundClientsFor run failed:", err)
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}
	clients, err := t.inboundService.GetClients(inbound)
	var buttons []telego.InlineKeyboardButton

	if err != nil {
		logger.Warning("GetInboundClients run failed:", err)
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	} else {
		if len(clients) > 0 {
			for _, client := range clients {
				buttons = append(buttons, tu.InlineKeyboardButton(client.Email).WithCallbackData(t.encodeQuery(action+" "+client.Email)))
			}

		} else {
			return nil, errors.New(t.I18nBot("tgbot.answers.getClientsFailed"))
		}

	}
	cols := 0
	if len(buttons) < 6 {
		cols = 3
	} else {
		cols = 2
	}
	keyboard := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols, buttons...))

	return keyboard, nil
}

// getInboundsAddClient creates an inline keyboard for adding clients to inbounds.
func (t *Tgbot) getInboundsAddClient() (*telego.InlineKeyboardMarkup, error) {
	inbounds, err := t.inboundService.GetAllInbounds()
	if err != nil {
		logger.Warning("GetAllInbounds run failed:", err)
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}

	if len(inbounds) == 0 {
		logger.Warning("No inbounds found")
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}

	excludedProtocols := map[model.Protocol]bool{
		model.Tunnel:    true,
		model.Mixed:     true,
		model.WireGuard: true,
		model.HTTP:      true,
		model.L2TP:      true,
	}

	var buttons []telego.InlineKeyboardButton
	for _, inbound := range inbounds {
		if excludedProtocols[inbound.Protocol] {
			continue
		}

		status := "❌"
		if inbound.Enable {
			status = "✅"
		}
		callbackData := t.encodeQuery(fmt.Sprintf("%s %d", "add_client_to", inbound.Id))
		buttons = append(buttons, tu.InlineKeyboardButton(fmt.Sprintf("%v - %v", inbound.Remark, status)).WithCallbackData(callbackData))
	}

	cols := 1
	if len(buttons) >= 6 {
		cols = 2
	}

	keyboard := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols, buttons...))
	return keyboard, nil
}

// getInboundClients creates an inline keyboard with clients of a specific inbound.
func (t *Tgbot) getInboundClients(id int) (*telego.InlineKeyboardMarkup, error) {
	inbound, err := t.inboundService.GetInbound(id)
	if err != nil {
		logger.Warning("getIboundClients run failed:", err)
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	}
	clients, err := t.inboundService.GetClients(inbound)
	var buttons []telego.InlineKeyboardButton

	if err != nil {
		logger.Warning("GetInboundClients run failed:", err)
		return nil, errors.New(t.I18nBot("tgbot.answers.getInboundsFailed"))
	} else {
		if len(clients) > 0 {
			for _, client := range clients {
				buttons = append(buttons, tu.InlineKeyboardButton(client.Email).WithCallbackData(t.encodeQuery("client_get_usage "+client.Email)))
			}

		} else {
			return nil, errors.New(t.I18nBot("tgbot.answers.getClientsFailed"))
		}

	}
	cols := 0
	if len(buttons) < 6 {
		cols = 3
	} else {
		cols = 2
	}
	keyboard := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols, buttons...))

	return keyboard, nil
}

// clientInfoMsg formats client information message based on traffic and flags.
func (t *Tgbot) clientInfoMsg(
	traffic *xray.ClientTraffic,
	printEnabled bool,
	printOnline bool,
	printActive bool,
	printDate bool,
	printTraffic bool,
	printRefreshed bool,
) string {
	now := time.Now().Unix()
	expiryTime := ""
	flag := false
	diff := traffic.ExpiryTime/1000 - now
	if traffic.ExpiryTime == 0 {
		expiryTime = t.I18nBot("tgbot.unlimited")
	} else if diff > 172800 || !traffic.Enable {
		expiryTime = time.Unix((traffic.ExpiryTime / 1000), 0).Format("2006-01-02 15:04:05")
		if diff > 0 {
			days := diff / 86400
			hours := (diff % 86400) / 3600
			minutes := (diff % 3600) / 60
			remainingTime := ""
			if days > 0 {
				remainingTime += fmt.Sprintf("%d %s ", days, t.I18nBot("tgbot.days"))
			}
			if hours > 0 {
				remainingTime += fmt.Sprintf("%d %s ", hours, t.I18nBot("tgbot.hours"))
			}
			if minutes > 0 {
				remainingTime += fmt.Sprintf("%d %s", minutes, t.I18nBot("tgbot.minutes"))
			}
			expiryTime += fmt.Sprintf(" (%s)", remainingTime)
		}
	} else if traffic.ExpiryTime < 0 {
		expiryTime = fmt.Sprintf("%d %s", traffic.ExpiryTime/-86400000, t.I18nBot("tgbot.days"))
		flag = true
	} else {
		expiryTime = fmt.Sprintf("%d %s", diff/3600, t.I18nBot("tgbot.hours"))
		flag = true
	}

	total := ""
	if traffic.Total == 0 {
		total = t.I18nBot("tgbot.unlimited")
	} else {
		total = common.FormatTraffic((traffic.Total))
	}

	enabled := ""
	isEnabled, err := t.inboundService.checkIsEnabledByEmail(traffic.Email)
	if err != nil {
		logger.Warning(err)
		enabled = t.I18nBot("tgbot.wentWrong")
	} else if isEnabled {
		enabled = t.I18nBot("tgbot.messages.yes")
	} else {
		enabled = t.I18nBot("tgbot.messages.no")
	}

	active := ""
	if traffic.Enable {
		active = t.I18nBot("tgbot.messages.yes")
	} else {
		active = t.I18nBot("tgbot.messages.no")
	}

	status := t.I18nBot("tgbot.offline")
	isOnline := false
	if p.IsRunning() {
		if slices.Contains(p.GetOnlineClients(), traffic.Email) {
			status = t.I18nBot("tgbot.online")
			isOnline = true
		}
	}

	output := ""
	output += t.I18nBot("tgbot.messages.email", "Email=="+traffic.Email)
	if printEnabled {
		output += t.I18nBot("tgbot.messages.enabled", "Enable=="+enabled)
	}
	if printOnline {
		output += t.I18nBot("tgbot.messages.online", "Status=="+status)
		if !isOnline && traffic.LastOnline > 0 {
			output += t.I18nBot("tgbot.messages.lastOnline", "Time=="+time.UnixMilli(traffic.LastOnline).Format("2006-01-02 15:04:05"))
		}
	}
	if printActive {
		output += t.I18nBot("tgbot.messages.active", "Enable=="+active)
	}
	if printDate {
		if flag {
			output += t.I18nBot("tgbot.messages.expireIn", "Time=="+expiryTime)
		} else {
			output += t.I18nBot("tgbot.messages.expire", "Time=="+expiryTime)
		}
	}
	if printTraffic {
		output += t.I18nBot("tgbot.messages.upload", "Upload=="+common.FormatTraffic(traffic.Up))
		output += t.I18nBot("tgbot.messages.download", "Download=="+common.FormatTraffic(traffic.Down))
		output += t.I18nBot("tgbot.messages.total", "UpDown=="+common.FormatTraffic((traffic.Up+traffic.Down)), "Total=="+total)
	}
	if printRefreshed {
		output += t.I18nBot("tgbot.messages.refreshedOn", "Time=="+time.Now().Format("2006-01-02 15:04:05"))
	}

	return output
}

// getClientUsage retrieves and sends client usage information to the chat.
func (t *Tgbot) getClientUsage(chatId int64, tgUserID int64, email ...string) {
	traffics, err := t.inboundService.GetClientTrafficTgBot(tgUserID)
	if err != nil {
		logger.Warning(err)
		msg := t.I18nBot("tgbot.wentWrong")
		t.SendMsgToTgbot(chatId, msg)
		return
	}

	if len(traffics) == 0 {
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.answers.askToAddUserId", "TgUserID=="+strconv.FormatInt(tgUserID, 10)))
		return
	}

	output := ""

	if len(traffics) > 0 {
		if len(email) > 0 {
			for _, traffic := range traffics {
				if traffic.Email == email[0] {
					output := t.clientInfoMsg(traffic, true, true, true, true, true, true)
					t.SendMsgToTgbot(chatId, output)
					return
				}
			}
			msg := t.I18nBot("tgbot.noResult")
			t.SendMsgToTgbot(chatId, msg)
			return
		} else {
			for _, traffic := range traffics {
				output += t.clientInfoMsg(traffic, true, true, true, true, true, false)
				output += "\r\n"
			}
		}
	}

	output += t.I18nBot("tgbot.messages.refreshedOn", "Time=="+time.Now().Format("2006-01-02 15:04:05"))
	t.SendMsgToTgbot(chatId, output)
	output = t.I18nBot("tgbot.commands.pleaseChoose")
	t.SendAnswer(chatId, output, false)
}

// searchClientIps searches and sends client IP addresses for the given email.
func (t *Tgbot) searchClientIps(chatId int64, email string, messageID ...int) {
	ips, err := t.inboundService.GetInboundClientIps(email)
	if err != nil || len(ips) == 0 {
		ips = t.I18nBot("tgbot.noIpRecord")
	}

	formattedIps := ips
	if err == nil && len(ips) > 0 {
		type ipWithTimestamp struct {
			IP        string `json:"ip"`
			Timestamp int64  `json:"timestamp"`
		}

		var ipsWithTime []ipWithTimestamp
		if json.Unmarshal([]byte(ips), &ipsWithTime) == nil && len(ipsWithTime) > 0 {
			lines := make([]string, 0, len(ipsWithTime))
			for _, item := range ipsWithTime {
				if item.IP == "" {
					continue
				}
				if item.Timestamp > 0 {
					ts := time.Unix(item.Timestamp, 0).Format("2006-01-02 15:04:05")
					lines = append(lines, fmt.Sprintf("%s (%s)", item.IP, ts))
					continue
				}
				lines = append(lines, item.IP)
			}
			if len(lines) > 0 {
				formattedIps = strings.Join(lines, "\n")
			}
		} else {
			var oldIps []string
			if json.Unmarshal([]byte(ips), &oldIps) == nil && len(oldIps) > 0 {
				formattedIps = strings.Join(oldIps, "\n")
			}
		}
	}

	output := ""
	output += t.I18nBot("tgbot.messages.email", "Email=="+email)
	output += t.I18nBot("tgbot.messages.ips", "IPs=="+formattedIps)
	output += t.I18nBot("tgbot.messages.refreshedOn", "Time=="+time.Now().Format("2006-01-02 15:04:05"))

	inlineKeyboard := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.refresh")).WithCallbackData(t.encodeQuery("ips_refresh "+email)),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.clearIPs")).WithCallbackData(t.encodeQuery("clear_ips "+email)),
		),
	)

	if len(messageID) > 0 {
		t.editMessageTgBot(chatId, messageID[0], output, inlineKeyboard)
	} else {
		t.SendMsgToTgbot(chatId, output, inlineKeyboard)
	}
}

// clientTelegramUserInfo retrieves and sends Telegram user info for the client.
func (t *Tgbot) clientTelegramUserInfo(chatId int64, email string, messageID ...int) {
	traffic, client, err := t.inboundService.GetClientByEmail(email)
	if err != nil {
		logger.Warning(err)
		msg := t.I18nBot("tgbot.wentWrong")
		t.SendMsgToTgbot(chatId, msg)
		return
	}
	if client == nil {
		msg := t.I18nBot("tgbot.noResult")
		t.SendMsgToTgbot(chatId, msg)
		return
	}
	tgId := "None"
	if client.TgID != 0 {
		tgId = strconv.FormatInt(client.TgID, 10)
	}

	output := ""
	output += t.I18nBot("tgbot.messages.email", "Email=="+email)
	output += t.I18nBot("tgbot.messages.TGUser", "TelegramID=="+tgId)
	output += t.I18nBot("tgbot.messages.refreshedOn", "Time=="+time.Now().Format("2006-01-02 15:04:05"))

	inlineKeyboard := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.refresh")).WithCallbackData(t.encodeQuery("tgid_refresh "+email)),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.removeTGUser")).WithCallbackData(t.encodeQuery("tgid_remove "+email)),
		),
	)

	if len(messageID) > 0 {
		t.editMessageTgBot(chatId, messageID[0], output, inlineKeyboard)
	} else {
		t.SendMsgToTgbot(chatId, output, inlineKeyboard)
		requestUser := telego.KeyboardButtonRequestUsers{
			RequestID: int32(traffic.Id),
			UserIsBot: new(bool),
		}
		keyboard := tu.Keyboard(
			tu.KeyboardRow(
				tu.KeyboardButton(t.I18nBot("tgbot.buttons.selectTGUser")).WithRequestUsers(&requestUser),
			),
			tu.KeyboardRow(
				tu.KeyboardButton(t.I18nBot("tgbot.buttons.closeKeyboard")),
			),
		).WithIsPersistent().WithResizeKeyboard()
		t.SendMsgToTgbot(chatId, t.I18nBot("tgbot.buttons.selectOneTGUser"), keyboard)
	}
}

// searchClient searches for a client by email and sends the information.
func (t *Tgbot) searchClient(chatId int64, email string, messageID ...int) {
	traffic, err := t.inboundService.GetClientTrafficByEmail(email)
	if err != nil {
		logger.Warning(err)
		msg := t.I18nBot("tgbot.wentWrong")
		t.SendMsgToTgbot(chatId, msg)
		return
	}
	if traffic == nil {
		msg := t.I18nBot("tgbot.noResult")
		t.SendMsgToTgbot(chatId, msg)
		return
	}

	output := t.clientInfoMsg(traffic, true, true, true, true, true, true)

	inlineKeyboard := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.refresh")).WithCallbackData(t.encodeQuery("client_refresh "+email)),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.resetTraffic")).WithCallbackData(t.encodeQuery("reset_traffic "+email)),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.limitTraffic")).WithCallbackData(t.encodeQuery("limit_traffic "+email)),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.resetExpire")).WithCallbackData(t.encodeQuery("reset_exp "+email)),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.ipLog")).WithCallbackData(t.encodeQuery("ip_log "+email)),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.ipLimit")).WithCallbackData(t.encodeQuery("ip_limit "+email)),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.setTGUser")).WithCallbackData(t.encodeQuery("tg_user "+email)),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.toggle")).WithCallbackData(t.encodeQuery("toggle_enable "+email)),
		),
	)
	if len(messageID) > 0 {
		t.editMessageTgBot(chatId, messageID[0], output, inlineKeyboard)
	} else {
		t.SendMsgToTgbot(chatId, output, inlineKeyboard)
	}
}

// getCommonClientButtons returns the shared inline keyboard rows for client configuration
func (t *Tgbot) getCommonClientButtons() [][]telego.InlineKeyboardButton {
	return [][]telego.InlineKeyboardButton{
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.limitTraffic")).WithCallbackData("add_client_ch_default_traffic"),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.resetExpire")).WithCallbackData("add_client_ch_default_exp"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.change_comment")).WithCallbackData("add_client_ch_default_comment"),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.ipLimit")).WithCallbackData("add_client_ch_default_ip_limit"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.submitDisable")).WithCallbackData("add_client_submit_disable"),
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.submitEnable")).WithCallbackData("add_client_submit_enable"),
		),
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.cancel")).WithCallbackData("add_client_cancel"),
		),
	}
}

// addClient handles the process of adding a new client to an inbound.
func (t *Tgbot) addClient(chatId int64, msg string, messageID ...int) {
	inbound, err := t.inboundService.GetInbound(receiver_inbound_ID)
	if err != nil {
		t.SendMsgToTgbot(chatId, err.Error())
		return
	}

	protocol := inbound.Protocol

	var protocolRows [][]telego.InlineKeyboardButton
	switch protocol {
	case model.VMESS, model.VLESS:
		protocolRows = [][]telego.InlineKeyboardButton{
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.change_email")).WithCallbackData("add_client_ch_default_email"),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.change_id")).WithCallbackData("add_client_ch_default_id"),
			),
		}
	case model.Trojan:
		protocolRows = [][]telego.InlineKeyboardButton{
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.change_email")).WithCallbackData("add_client_ch_default_email"),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.change_password")).WithCallbackData("add_client_ch_default_pass_tr"),
			),
		}
	case model.Shadowsocks:
		protocolRows = [][]telego.InlineKeyboardButton{
			tu.InlineKeyboardRow(
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.change_email")).WithCallbackData("add_client_ch_default_email"),
				tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.change_password")).WithCallbackData("add_client_ch_default_pass_sh"),
			),
		}
	}

	commonRows := t.getCommonClientButtons()
	inlineKeyboard := tu.InlineKeyboard(append(protocolRows, commonRows...)...)

	if len(messageID) > 0 {
		t.editMessageTgBot(chatId, messageID[0], msg, inlineKeyboard)
	} else {
		t.SendMsgToTgbot(chatId, msg, inlineKeyboard)
	}

}

// searchInbound searches for inbounds by remark and sends the results.
func (t *Tgbot) searchInbound(chatId int64, remark string) {
	inbounds, err := t.inboundService.SearchInbounds(remark)
	if err != nil {
		logger.Warning(err)
		msg := t.I18nBot("tgbot.wentWrong")
		t.SendMsgToTgbot(chatId, msg)
		return
	}
	if len(inbounds) == 0 {
		msg := t.I18nBot("tgbot.noInbounds")
		t.SendMsgToTgbot(chatId, msg)
		return
	}

	for _, inbound := range inbounds {
		info := ""
		info += t.I18nBot("tgbot.messages.inbound", "Remark=="+inbound.Remark)
		info += t.I18nBot("tgbot.messages.port", "Port=="+strconv.Itoa(inbound.Port))
		info += t.I18nBot("tgbot.messages.traffic", "Total=="+common.FormatTraffic((inbound.Up+inbound.Down)), "Upload=="+common.FormatTraffic(inbound.Up), "Download=="+common.FormatTraffic(inbound.Down))

		if inbound.ExpiryTime == 0 {
			info += t.I18nBot("tgbot.messages.expire", "Time=="+t.I18nBot("tgbot.unlimited"))
		} else {
			info += t.I18nBot("tgbot.messages.expire", "Time=="+time.Unix((inbound.ExpiryTime/1000), 0).Format("2006-01-02 15:04:05"))
		}
		t.SendMsgToTgbot(chatId, info)

		if len(inbound.ClientStats) > 0 {
			var output strings.Builder
			for _, traffic := range inbound.ClientStats {
				output.WriteString(t.clientInfoMsg(&traffic, true, true, true, true, true, true))
			}
			t.SendMsgToTgbot(chatId, output.String())
		}
	}
}

// getExhausted retrieves and sends information about exhausted clients.
func (t *Tgbot) getExhausted(chatId int64) {
	trDiff := int64(0)
	exDiff := int64(0)
	now := time.Now().Unix() * 1000
	var exhaustedInbounds []model.Inbound
	var exhaustedClients []xray.ClientTraffic
	var disabledInbounds []model.Inbound
	var disabledClients []xray.ClientTraffic

	TrafficThreshold, err := t.settingService.GetTrafficDiff()
	if err == nil && TrafficThreshold > 0 {
		trDiff = int64(TrafficThreshold) * 1073741824
	}
	ExpireThreshold, err := t.settingService.GetExpireDiff()
	if err == nil && ExpireThreshold > 0 {
		exDiff = int64(ExpireThreshold) * 86400000
	}
	inbounds, err := t.inboundService.GetAllInbounds()
	if err != nil {
		logger.Warning("Unable to load Inbounds", err)
	}

	// One line per ACCOUNT, not per membership. An inbound's ClientStats now lists
	// every account the inbound serves (it used to hold only the accounts homed on
	// it), so an account on three inbounds would otherwise be reported as three
	// separate customers running out of traffic.
	reported := make(map[string]bool)
	for _, inbound := range inbounds {
		if inbound.Enable {
			if (inbound.ExpiryTime > 0 && (inbound.ExpiryTime-now < exDiff)) ||
				(inbound.Total > 0 && (inbound.Total-(inbound.Up+inbound.Down) < trDiff)) {
				exhaustedInbounds = append(exhaustedInbounds, *inbound)
			}
			if len(inbound.ClientStats) > 0 {
				for _, client := range inbound.ClientStats {
					if reported[AccountKeyOf(client.Email)] {
						continue
					}
					reported[AccountKeyOf(client.Email)] = true
					if client.Enable {
						if (client.ExpiryTime > 0 && (client.ExpiryTime-now < exDiff)) ||
							(client.Total > 0 && (client.Total-(client.Up+client.Down) < trDiff)) {
							exhaustedClients = append(exhaustedClients, client)
						}
					} else {
						disabledClients = append(disabledClients, client)
					}
				}
			}
		} else {
			disabledInbounds = append(disabledInbounds, *inbound)
		}
	}

	// Inbounds
	output := ""
	output += t.I18nBot("tgbot.messages.exhaustedCount", "Type=="+t.I18nBot("tgbot.inbounds"))
	output += t.I18nBot("tgbot.messages.disabled", "Disabled=="+strconv.Itoa(len(disabledInbounds)))
	output += t.I18nBot("tgbot.messages.depleteSoon", "Deplete=="+strconv.Itoa(len(exhaustedInbounds)))

	if len(exhaustedInbounds) > 0 {
		output += t.I18nBot("tgbot.messages.depleteSoon", "Deplete=="+t.I18nBot("tgbot.inbounds"))

		for _, inbound := range exhaustedInbounds {
			output += t.I18nBot("tgbot.messages.inbound", "Remark=="+inbound.Remark)
			output += t.I18nBot("tgbot.messages.port", "Port=="+strconv.Itoa(inbound.Port))
			output += t.I18nBot("tgbot.messages.traffic", "Total=="+common.FormatTraffic((inbound.Up+inbound.Down)), "Upload=="+common.FormatTraffic(inbound.Up), "Download=="+common.FormatTraffic(inbound.Down))
			if inbound.ExpiryTime == 0 {
				output += t.I18nBot("tgbot.messages.expire", "Time=="+t.I18nBot("tgbot.unlimited"))
			} else {
				output += t.I18nBot("tgbot.messages.expire", "Time=="+time.Unix((inbound.ExpiryTime/1000), 0).Format("2006-01-02 15:04:05"))
			}
			output += "\r\n"
		}
	}

	// Clients
	exhaustedCC := len(exhaustedClients)
	output += t.I18nBot("tgbot.messages.exhaustedCount", "Type=="+t.I18nBot("tgbot.clients"))
	output += t.I18nBot("tgbot.messages.disabled", "Disabled=="+strconv.Itoa(len(disabledClients)))
	output += t.I18nBot("tgbot.messages.depleteSoon", "Deplete=="+strconv.Itoa(exhaustedCC))

	if exhaustedCC > 0 {
		output += t.I18nBot("tgbot.messages.depleteSoon", "Deplete=="+t.I18nBot("tgbot.clients"))
		var buttons []telego.InlineKeyboardButton
		for _, traffic := range exhaustedClients {
			output += t.clientInfoMsg(&traffic, true, false, false, true, true, false)
			output += "\r\n"
			buttons = append(buttons, tu.InlineKeyboardButton(traffic.Email).WithCallbackData(t.encodeQuery("client_get_usage "+traffic.Email)))
		}
		cols := 0
		if exhaustedCC < 11 {
			cols = 1
		} else {
			cols = 2
		}
		output += t.I18nBot("tgbot.messages.refreshedOn", "Time=="+time.Now().Format("2006-01-02 15:04:05"))
		keyboard := tu.InlineKeyboardGrid(tu.InlineKeyboardCols(cols, buttons...))
		t.SendMsgToTgbot(chatId, output, keyboard)
	} else {
		output += t.I18nBot("tgbot.messages.refreshedOn", "Time=="+time.Now().Format("2006-01-02 15:04:05"))
		t.SendMsgToTgbot(chatId, output)
	}
}

// notifyExhausted sends notifications for exhausted clients.
func (t *Tgbot) notifyExhausted() {
	trDiff := int64(0)
	exDiff := int64(0)
	now := time.Now().Unix() * 1000

	TrafficThreshold, err := t.settingService.GetTrafficDiff()
	if err == nil && TrafficThreshold > 0 {
		trDiff = int64(TrafficThreshold) * 1073741824
	}
	ExpireThreshold, err := t.settingService.GetExpireDiff()
	if err == nil && ExpireThreshold > 0 {
		exDiff = int64(ExpireThreshold) * 86400000
	}
	inbounds, err := t.inboundService.GetAllInbounds()
	if err != nil {
		logger.Warning("Unable to load Inbounds", err)
	}

	var chatIDsDone []int64
	for _, inbound := range inbounds {
		if inbound.Enable {
			if len(inbound.ClientStats) > 0 {
				clients, err := t.inboundService.GetClients(inbound)
				if err == nil {
					for _, client := range clients {
						if client.TgID != 0 {
							chatID := client.TgID
							if !int64Contains(chatIDsDone, chatID) && !checkAdmin(chatID) {
								var disabledClients []xray.ClientTraffic
								var exhaustedClients []xray.ClientTraffic
								traffics, err := t.inboundService.GetClientTrafficTgBot(client.TgID)
								if err == nil && len(traffics) > 0 {
									output := t.I18nBot("tgbot.messages.exhaustedCount", "Type=="+t.I18nBot("tgbot.clients"))
									for _, traffic := range traffics {
										if traffic.Enable {
											if (traffic.ExpiryTime > 0 && (traffic.ExpiryTime-now < exDiff)) ||
												(traffic.Total > 0 && (traffic.Total-(traffic.Up+traffic.Down) < trDiff)) {
												exhaustedClients = append(exhaustedClients, *traffic)
											}
										} else {
											disabledClients = append(disabledClients, *traffic)
										}
									}
									if len(exhaustedClients) > 0 {
										output += t.I18nBot("tgbot.messages.disabled", "Disabled=="+strconv.Itoa(len(disabledClients)))
										if len(disabledClients) > 0 {
											output += t.I18nBot("tgbot.clients") + ":\r\n"
											for _, traffic := range disabledClients {
												output += " " + traffic.Email
											}
											output += "\r\n"
										}
										output += "\r\n"
										output += t.I18nBot("tgbot.messages.depleteSoon", "Deplete=="+strconv.Itoa(len(exhaustedClients)))
										for _, traffic := range exhaustedClients {
											output += t.clientInfoMsg(&traffic, true, false, false, true, true, false)
											output += "\r\n"
										}
										t.SendMsgToTgbot(chatID, output)
									}
									chatIDsDone = append(chatIDsDone, chatID)
								}
							}
						}
					}
				}
			}
		}
	}
}

// int64Contains checks if an int64 slice contains a specific item.
func int64Contains(slice []int64, item int64) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// onlineClients retrieves and sends information about online clients.
func (t *Tgbot) onlineClients(chatId int64, messageID ...int) {
	if !p.IsRunning() {
		return
	}

	onlines := p.GetOnlineClients()
	onlinesCount := len(onlines)
	output := t.I18nBot("tgbot.messages.onlinesCount", "Count=="+fmt.Sprint(onlinesCount))
	keyboard := tu.InlineKeyboard(tu.InlineKeyboardRow(
		tu.InlineKeyboardButton(t.I18nBot("tgbot.buttons.refresh")).WithCallbackData(t.encodeQuery("onlines_refresh"))))

	if onlinesCount > 0 {
		var buttons []telego.InlineKeyboardButton
		for _, online := range onlines {
			buttons = append(buttons, tu.InlineKeyboardButton(online).WithCallbackData(t.encodeQuery("client_get_usage "+online)))
		}
		cols := 0
		if onlinesCount < 21 {
			cols = 2
		} else if onlinesCount < 61 {
			cols = 3
		} else {
			cols = 4
		}
		keyboard.InlineKeyboard = append(keyboard.InlineKeyboard, tu.InlineKeyboardCols(cols, buttons...)...)
	}

	if len(messageID) > 0 {
		t.editMessageTgBot(chatId, messageID[0], output, keyboard)
	} else {
		t.SendMsgToTgbot(chatId, output, keyboard)
	}
}

// sendBackup sends a backup of the database and configuration files.
func (t *Tgbot) sendBackup(chatId int64) {
	output := t.I18nBot("tgbot.messages.backupTime", "Time=="+time.Now().Format("2006-01-02 15:04:05"))
	t.SendMsgToTgbot(chatId, output)

	// Update by manually trigger a checkpoint operation
	err := database.Checkpoint()
	if err != nil {
		logger.Error("Error in trigger a checkpoint operation: ", err)
	}

	// Send database backup
	file, err := os.Open(config.GetDBPath())
	if err == nil {
		defer file.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		document := tu.Document(
			tu.ID(chatId),
			tu.File(file),
		)
		_, err = bot.SendDocument(ctx, document)
		if err != nil {
			logger.Error("Error in uploading backup: ", err)
		}
	} else {
		logger.Error("Error in opening db file for backup: ", err)
	}

	// Small delay between file sends
	time.Sleep(500 * time.Millisecond)

	// Send config.json backup
	file, err = os.Open(xray.GetConfigPath())
	if err == nil {
		defer file.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		document := tu.Document(
			tu.ID(chatId),
			tu.File(file),
		)
		_, err = bot.SendDocument(ctx, document)
		if err != nil {
			logger.Error("Error in uploading config.json: ", err)
		}
	} else {
		logger.Error("Error in opening config.json file for backup: ", err)
	}
}

// sendBanLogs sends the ban logs to the specified chat.
func (t *Tgbot) sendBanLogs(chatId int64, dt bool) {
	if dt {
		output := t.I18nBot("tgbot.messages.datetime", "DateTime=="+time.Now().Format("2006-01-02 15:04:05"))
		t.SendMsgToTgbot(chatId, output)
	}

	file, err := os.Open(xray.GetIPLimitBannedPrevLogPath())
	if err == nil {
		// Check if the file is non-empty before attempting to upload
		fileInfo, _ := file.Stat()
		if fileInfo.Size() > 0 {
			document := tu.Document(
				tu.ID(chatId),
				tu.File(file),
			)
			_, err = bot.SendDocument(context.Background(), document)
			if err != nil {
				logger.Error("Error in uploading IPLimitBannedPrevLog: ", err)
			}
		} else {
			logger.Warning("IPLimitBannedPrevLog file is empty, not uploading.")
		}
		file.Close()
	} else {
		logger.Error("Error in opening IPLimitBannedPrevLog file for backup: ", err)
	}

	file, err = os.Open(xray.GetIPLimitBannedLogPath())
	if err == nil {
		// Check if the file is non-empty before attempting to upload
		fileInfo, _ := file.Stat()
		if fileInfo.Size() > 0 {
			document := tu.Document(
				tu.ID(chatId),
				tu.File(file),
			)
			_, err = bot.SendDocument(context.Background(), document)
			if err != nil {
				logger.Error("Error in uploading IPLimitBannedLog: ", err)
			}
		} else {
			logger.Warning("IPLimitBannedLog file is empty, not uploading.")
		}
		file.Close()
	} else {
		logger.Error("Error in opening IPLimitBannedLog file for backup: ", err)
	}
}

// sendCallbackAnswerTgBot answers a callback query with a message.
func (t *Tgbot) sendCallbackAnswerTgBot(id string, message string) {
	params := telego.AnswerCallbackQueryParams{
		CallbackQueryID: id,
		Text:            message,
	}
	if err := bot.AnswerCallbackQuery(context.Background(), &params); err != nil {
		logger.Warning(err)
	}
}

// editMessageCallbackTgBot edits the reply markup of a message.
func (t *Tgbot) editMessageCallbackTgBot(chatId int64, messageID int, inlineKeyboard *telego.InlineKeyboardMarkup) {
	params := telego.EditMessageReplyMarkupParams{
		ChatID:      tu.ID(chatId),
		MessageID:   messageID,
		ReplyMarkup: inlineKeyboard,
	}
	if _, err := bot.EditMessageReplyMarkup(context.Background(), &params); err != nil {
		logger.Warning(err)
	}
}

// editMessageTgBot edits the text and reply markup of a message.
func (t *Tgbot) editMessageTgBot(chatId int64, messageID int, text string, inlineKeyboard ...*telego.InlineKeyboardMarkup) {
	params := telego.EditMessageTextParams{
		ChatID:    tu.ID(chatId),
		MessageID: messageID,
		Text:      text,
		ParseMode: "HTML",
	}
	if len(inlineKeyboard) > 0 {
		params.ReplyMarkup = inlineKeyboard[0]
	}
	if _, err := bot.EditMessageText(context.Background(), &params); err != nil {
		logger.Warning(err)
	}
}

// SendMsgToTgbotDeleteAfter sends a message and deletes it after a specified delay.
func (t *Tgbot) SendMsgToTgbotDeleteAfter(chatId int64, msg string, delayInSeconds int, replyMarkup ...telego.ReplyMarkup) {
	// Determine if replyMarkup was passed; otherwise, set it to nil
	var replyMarkupParam telego.ReplyMarkup
	if len(replyMarkup) > 0 {
		replyMarkupParam = replyMarkup[0] // Use the first element
	}

	// Send the message
	sentMsg, err := bot.SendMessage(context.Background(), &telego.SendMessageParams{
		ChatID:      tu.ID(chatId),
		Text:        msg,
		ReplyMarkup: replyMarkupParam, // Use the correct replyMarkup value
	})
	if err != nil {
		logger.Warning("Failed to send message:", err)
		return
	}

	// Delete the sent message after the specified number of seconds
	go func() {
		time.Sleep(time.Duration(delayInSeconds) * time.Second) // Wait for the specified delay
		t.deleteMessageTgBot(chatId, sentMsg.MessageID)         // Delete the message
		delete(userStates, chatId)
	}()
}

// deleteMessageTgBot deletes a message from the chat.
func (t *Tgbot) deleteMessageTgBot(chatId int64, messageID int) {
	params := telego.DeleteMessageParams{
		ChatID:    tu.ID(chatId),
		MessageID: messageID,
	}
	if err := bot.DeleteMessage(context.Background(), &params); err != nil {
		logger.Warning("Failed to delete message:", err)
	} else {
		logger.Info("Message deleted successfully")
	}
}

// isSingleWord checks if the text contains only a single word.
func (t *Tgbot) isSingleWord(text string) bool {
	text = strings.TrimSpace(text)
	re := regexp.MustCompile(`\s+`)
	return re.MatchString(text)
}
