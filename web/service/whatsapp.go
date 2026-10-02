package service

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"gorm.io/gorm"
)

type WhatsAppService struct {
	settingService *SettingService
	client         *http.Client
}

func NewWhatsAppService() *WhatsAppService {
	return &WhatsAppService{settingService: new(SettingService), client: &http.Client{Timeout: 15 * time.Second}}
}

func normalizeWhatsAppPhone(v string) string {
	v = strings.TrimSpace(v)
	v = strings.NewReplacer(" ", "", "-", "", "(", "", ")", "").Replace(v)
	v = strings.TrimPrefix(v, "+")
	if strings.HasPrefix(v, "00") {
		v = strings.TrimPrefix(v, "00")
	}
	return v
}

type AllWhatsAppSettings struct {
	Enable                                                                    bool
	AccessToken, PhoneNumberID, VerifyToken, AppSecret, GraphVersion, Runtime string
	TwoDayTemplate, OneDayTemplate, ExpiredTemplate, TemplateLanguage         string
	TwoDayText, OneDayText, ExpiredText                                       string
}

func (s *WhatsAppService) settings() (*AllWhatsAppSettings, error) {
	all, err := s.settingService.GetAllSetting()
	if err != nil {
		return nil, err
	}
	return &AllWhatsAppSettings{
		Enable: all.WaEnable, AccessToken: all.WaAccessToken, PhoneNumberID: all.WaPhoneNumberID,
		VerifyToken: all.WaVerifyToken, AppSecret: all.WaAppSecret, GraphVersion: all.WaGraphVersion,
		Runtime: all.WaExpiryReminderRuntime, TwoDayTemplate: all.WaExpiryTemplate2Days,
		OneDayTemplate: all.WaExpiryTemplate1Day, ExpiredTemplate: all.WaExpiryTemplateExpired,
		TemplateLanguage: all.WaTemplateLanguage, TwoDayText: all.WaExpiryText2Days,
		OneDayText: all.WaExpiryText1Day, ExpiredText: all.WaExpiryTextExpired,
	}, nil
}

func (s *WhatsAppService) send(payload any) error {
	cfg, err := s.settings()
	if err != nil {
		return err
	}
	if !cfg.Enable {
		return errors.New("whatsapp notifications are disabled")
	}
	if cfg.AccessToken == "" || cfg.PhoneNumberID == "" {
		return errors.New("whatsapp credentials are incomplete")
	}
	version := strings.TrimSpace(cfg.GraphVersion)
	if version == "" {
		version = "v23.0"
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	u := fmt.Sprintf("https://graph.facebook.com/%s/%s/messages", version, cfg.PhoneNumberID)
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return fmt.Errorf("whatsapp api returned %s: %v", resp.Status, out)
	}
	return nil
}

func (s *WhatsAppService) SendText(phone, text string) error {
	phone = normalizeWhatsAppPhone(phone)
	if phone == "" || text == "" {
		return errors.New("whatsapp phone or message is empty")
	}
	return s.send(map[string]any{"messaging_product": "whatsapp", "recipient_type": "individual", "to": phone, "type": "text", "text": map[string]any{"preview_url": false, "body": text}})
}

func (s *WhatsAppService) SendTemplate(phone, template, language, variable string) error {
	phone, template, language = normalizeWhatsAppPhone(phone), strings.TrimSpace(template), strings.TrimSpace(language)
	if phone == "" || template == "" {
		return errors.New("whatsapp phone or template is empty")
	}
	if language == "" {
		language = "en_US"
	}
	tmpl := map[string]any{"name": template, "language": map[string]string{"code": language}}
	if variable != "" {
		tmpl["components"] = []any{map[string]any{"type": "body", "parameters": []any{map[string]any{"type": "text", "text": variable}}}}
	}
	return s.send(map[string]any{"messaging_product": "whatsapp", "recipient_type": "individual", "to": phone, "type": "template", "template": tmpl})
}

func whatsappTemplateText(text, email string, days int, expiry time.Time, remainingTraffic int64) string {
	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, "{email}", email)
	text = strings.ReplaceAll(text, "{days}", fmt.Sprint(days))
	text = strings.ReplaceAll(text, "{expiry}", expiry.Format("2006-01-02 15:04:05"))
	text = strings.ReplaceAll(text, "{remaining_traffic}", formatResellerBytes(remainingTraffic))
	return text
}

func (s *WhatsAppService) SendExpiryReminders() {
	cfg, err := s.settings()
	if err != nil || !cfg.Enable {
		return
	}
	var accounts []model.Account
	if err := database.GetDB().Where("expiry_time > 0 AND whatsapp_phone <> ''").Find(&accounts).Error; err != nil {
		logger.Warning("WhatsApp expiry reminders: failed to load accounts:", err)
		return
	}
	now := time.Now()
	for _, account := range accounts {
		remaining := time.Until(time.UnixMilli(account.ExpiryTime))
		event, days, template, text := "", 0, "", ""
		switch {
		case remaining > 24*time.Hour && remaining <= 2*24*time.Hour:
			event, days, template, text = "warn-2d", 2, cfg.TwoDayTemplate, cfg.TwoDayText
		case remaining > 0 && remaining <= 24*time.Hour:
			event, days, template, text = "warn-1d", 1, cfg.OneDayTemplate, cfg.OneDayText
		case remaining <= 0:
			event, days, template, text = "expired", 0, cfg.ExpiredTemplate, cfg.ExpiredText
		}
		if event == "" || template == "" {
			continue
		}
		var state model.WhatsAppReminderState
		err := database.GetDB().Where("email = ? AND phone = ? AND expiry_time = ? AND event = ?", account.Email, account.WhatsAppPhone, account.ExpiryTime, event).First(&state).Error
		if err == nil {
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		var traffic struct{ Up, Down int64 }
		database.GetDB().Table("client_traffics").Where("email = ?", account.Email).Scan(&traffic)
		remainingTraffic := account.TotalGB - traffic.Up - traffic.Down
		if remainingTraffic < 0 {
			remainingTraffic = 0
		}
		message := whatsappTemplateText(text, account.Email, days, time.UnixMilli(account.ExpiryTime), remainingTraffic)
		if message == "" {
			message = fmt.Sprintf("Account %s expires on %s.", account.Email, time.UnixMilli(account.ExpiryTime).Format("2006-01-02 15:04:05"))
		}
		if err := s.SendTemplate(account.WhatsAppPhone, template, cfg.TemplateLanguage, message); err != nil {
			logger.Warning("WhatsApp expiry reminder failed:", err)
			continue
		}
		_ = database.GetDB().Create(&model.WhatsAppReminderState{Email: account.Email, Phone: account.WhatsAppPhone, ExpiryTime: account.ExpiryTime, Event: event, SentAt: now.Unix()}).Error
	}
}

func (s *WhatsAppService) WebhookVerifyToken() (string, error) {
	cfg, err := s.settings()
	if err != nil {
		return "", err
	}
	return cfg.VerifyToken, nil
}

func (s *WhatsAppService) VerifyWebhook(signature string, body []byte) bool {
	cfg, err := s.settings()
	if err != nil || cfg.AppSecret == "" || !strings.HasPrefix(signature, "sha256=") {
		return false
	}
	mac := hmac.New(sha256.New, []byte(cfg.AppSecret))
	_, _ = mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}

type whatsappWebhook struct {
	Object string
	Entry  []struct {
		Changes []struct {
			Value struct {
				Messages []struct {
					From string
					Text struct{ Body string }
					Type string
				}
			}
		}
	}
}

func (s *WhatsAppService) HandleWebhook(body []byte) {
	var hook whatsappWebhook
	if err := json.Unmarshal(body, &hook); err != nil {
		return
	}
	for _, entry := range hook.Entry {
		for _, change := range entry.Changes {
			for _, message := range change.Value.Messages {
				if message.Type != "text" {
					continue
				}
				from, text := normalizeWhatsAppPhone(message.From), strings.TrimSpace(message.Text.Body)
				if from == "" || text == "" {
					continue
				}
				if strings.HasPrefix(strings.ToUpper(text), "START ") {
					email := strings.TrimSpace(text[len("START "):])
					var account model.Account
					if err := database.GetDB().Where("LOWER(TRIM(email)) = ?", strings.ToLower(email)).First(&account).Error; err != nil {
						_ = s.SendText(from, "Account not found. Send START followed by your account email.")
						continue
					}
					account.WhatsAppPhone = from
					if err := database.GetDB().Save(&account).Error; err != nil {
						logger.Warning("WhatsApp bind failed:", err)
						continue
					}
					_ = s.SendText(from, fmt.Sprintf("WhatsApp notifications enabled for %s.", account.Email))
					continue
				}
				if strings.EqualFold(text, "STOP") {
					var account model.Account
					if database.GetDB().Where("whatsapp_phone = ?", from).First(&account).Error == nil {
						account.WhatsAppPhone = ""
						_ = database.GetDB().Save(&account)
					}
					continue
				}
				_ = s.SendText(from, "Send START followed by your account email to link this WhatsApp number.")
			}
		}
	}
}
