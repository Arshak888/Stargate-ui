package service

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/mymmrac/telego"
	tu "github.com/mymmrac/telego/telegoutil"
	"gorm.io/gorm"
)

type telegramRenewalPlan struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	GB    int64  `json:"gb"`
	Days  int    `json:"days"`
	Price string `json:"price"`
}

type telegramRenewalFlow struct {
	AccountID int
	Email     string
	Plan      telegramRenewalPlan
}

var telegramRenewalFlows sync.Map

func (t *Tgbot) telegramRenewalSettings() (bool, string, []telegramRenewalPlan, error) {
	s, err := t.settingService.GetAllSetting()
	if err != nil {
		return false, "", nil, err
	}
	var plans []telegramRenewalPlan
	if strings.TrimSpace(s.TgRenewalPlans) != "" {
		if err := json.Unmarshal([]byte(s.TgRenewalPlans), &plans); err != nil {
			return false, s.TgRenewalPaymentInfo, nil, err
		}
	}
	valid := make([]telegramRenewalPlan, 0, len(plans))
	for _, p := range plans {
		if strings.TrimSpace(p.ID) == "" || strings.TrimSpace(p.Name) == "" || p.GB < 0 || p.Days < 1 {
			continue
		}
		valid = append(valid, p)
	}
	return s.TgRenewalEnable, s.TgRenewalPaymentInfo, valid, nil
}
func (t *Tgbot) handleRenewalCommand(chatID, tgID int64) {
	enabled, paymentInfo, plans, err := t.telegramRenewalSettings()
	if err != nil || !enabled {
		t.SendMsgToTgbot(chatID, "Customer self-service renewal is currently disabled.")
		return
	}

	// Prefer the canonical Account projection, but also recover accounts from the
	// legacy inbound client tgId. This matters for clients created before the account
	// migration or linked to Telegram before the Account row was projected.
	var accounts []model.Account
	if err := database.GetDB().Where("tg_id = ?", tgID).Order("id asc").Find(&accounts).Error; err != nil {
		t.SendMsgToTgbot(chatID, "Could not load your linked accounts.")
		return
	}

	seen := make(map[string]bool, len(accounts))
	for _, account := range accounts {
		seen[strings.ToLower(strings.TrimSpace(account.Email))] = true
	}
	if traffics, trafficErr := t.inboundService.GetClientTrafficTgBot(tgID); trafficErr == nil {
		for _, traffic := range traffics {
			if traffic == nil || strings.TrimSpace(traffic.Email) == "" {
				continue
			}
			var account model.Account
			if err := database.GetDB().
				Where("LOWER(TRIM(email)) = ?", strings.ToLower(strings.TrimSpace(traffic.Email))).
				First(&account).Error; err != nil {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(account.Email))
			if seen[key] {
				continue
			}
			// Keep the Account projection aligned with the Telegram binding that the
			// user is already using successfully for usage/subscription actions.
			if account.TgID != tgID {
				account.TgID = tgID
				if err := database.GetDB().Save(&account).Error; err != nil {
					logger.Warning("telegram renewal: failed to sync account Telegram ID:", err)
				}
			}
			accounts = append(accounts, account)
			seen[key] = true
		}
	}

	if len(accounts) == 0 {
		t.SendMsgToTgbot(chatID, "No VPN account is linked to this Telegram ID. Ask the admin to link your Telegram ID first.")
		return
	}
	if len(plans) == 0 {
		t.SendMsgToTgbot(chatID, "No renewal plans are configured by the admin.")
		return
	}
	if len(accounts) == 1 {
		t.sendRenewalPlanKeyboard(chatID, tgID, accounts[0], paymentInfo, plans)
		return
	}
	rows := make([][]telego.InlineKeyboardButton, 0, len(accounts))
	for _, account := range accounts {
		label := account.Email
		if len(label) > 32 {
			label = label[:32]
		}
		rows = append(rows, []telego.InlineKeyboardButton{telego.InlineKeyboardButton{Text: label}.WithCallbackData(fmt.Sprintf("tr:a:%d", account.Id))})
	}
	t.SendMsgToTgbot(chatID, "Select the account you want to renew:", tu.InlineKeyboard(rows...))
}

func (t *Tgbot) sendRenewalPlanKeyboard(chatID, tgID int64, account model.Account, paymentInfo string, plans []telegramRenewalPlan) {
	rows := make([][]telego.InlineKeyboardButton, 0, len(plans))
	for i, p := range plans {
		label := p.Name
		if p.Price != "" {
			label += " • " + p.Price
		}
		rows = append(rows, []telego.InlineKeyboardButton{telego.InlineKeyboardButton{Text: label}.WithCallbackData(fmt.Sprintf("tr:p:%d:%d", account.Id, i))})
	}
	text := fmt.Sprintf("🔄 <b>Renewal</b>\nAccount: <code>%s</code>\n\n%s\n\nChoose a plan, then send your payment receipt as a photo or document.", html.EscapeString(account.Email), html.EscapeString(paymentInfo))
	t.SendMsgToTgbot(chatID, text, tu.InlineKeyboard(rows...))
	telegramRenewalFlows.Delete(tgID)
}
func (t *Tgbot) handleRenewalCallback(q *telego.CallbackQuery, isAdmin bool) bool {
	if !strings.HasPrefix(q.Data, "tr:") {
		return false
	}
	chatID := q.Message.GetChat().ID
	parts := strings.Split(q.Data, ":")
	if len(parts) < 3 {
		return true
	}
	switch parts[1] {
	case "a":
		if isAdmin {
			return true
		}
		accountID, err := strconv.Atoi(parts[2])
		if err != nil {
			return true
		}
		var account model.Account
		if err := database.GetDB().Where("id = ? AND tg_id = ?", accountID, q.From.ID).First(&account).Error; err != nil {
			t.sendCallbackAnswerTgBot(q.ID, "Account not found.")
			return true
		}
		_, paymentInfo, plans, err := t.telegramRenewalSettings()
		if err != nil {
			t.sendCallbackAnswerTgBot(q.ID, "Renewal settings are invalid.")
			return true
		}
		t.sendRenewalPlanKeyboard(chatID, q.From.ID, account, paymentInfo, plans)
		t.sendCallbackAnswerTgBot(q.ID, "Account selected.")
		return true
	case "p":
		if isAdmin || len(parts) != 4 {
			return true
		}
		accountID, err1 := strconv.Atoi(parts[2])
		planIndex, err2 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil {
			return true
		}
		var account model.Account
		if err := database.GetDB().Where("id = ? AND tg_id = ?", accountID, q.From.ID).First(&account).Error; err != nil {
			t.sendCallbackAnswerTgBot(q.ID, "Account not found.")
			return true
		}
		enabled, paymentInfo, plans, err := t.telegramRenewalSettings()
		if err != nil || !enabled || planIndex < 0 || planIndex >= len(plans) {
			t.sendCallbackAnswerTgBot(q.ID, "Renewal plan is unavailable.")
			return true
		}
		plan := plans[planIndex]
		telegramRenewalFlows.Store(q.From.ID, telegramRenewalFlow{AccountID: account.Id, Email: account.Email, Plan: plan})
		price := ""
		if plan.Price != "" {
			price = "\n💵 Price: <b>" + html.EscapeString(plan.Price) + "</b>"
		}
		t.SendMsgToTgbot(chatID, fmt.Sprintf("📦 <b>%s</b>%s\n\n%s\n\nNow send the payment receipt as a photo or document.", html.EscapeString(plan.Name), price, html.EscapeString(paymentInfo)))
		t.sendCallbackAnswerTgBot(q.ID, "Plan selected.")
		return true
	case "approve", "reject":
		if !isAdmin || len(parts) != 3 {
			return true
		}
		requestID, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return true
		}
		if parts[1] == "approve" {
			t.approveTelegramRenewal(chatID, q, requestID)
		} else {
			t.rejectTelegramRenewal(chatID, q, requestID)
		}
		return true
	default:
		return true
	}
}
func (t *Tgbot) handleRenewalMedia(message *telego.Message) bool {
	value, ok := telegramRenewalFlows.Load(message.Chat.ID)
	if !ok {
		return false
	}
	flow := value.(telegramRenewalFlow)
	telegramRenewalFlows.Delete(message.Chat.ID)
	var fileID, receiptType string
	if len(message.Photo) > 0 {
		fileID = message.Photo[len(message.Photo)-1].FileID
		receiptType = "photo"
	} else if message.Document != nil {
		fileID = message.Document.FileID
		receiptType = "document"
	} else {
		t.SendMsgToTgbot(message.Chat.ID, "Please send the receipt as a photo or document.")
		telegramRenewalFlows.Store(message.Chat.ID, flow)
		return true
	}
	var pending model.TelegramRenewalRequest
	if err := database.GetDB().Where("tg_id = ? AND status = ?", message.Chat.ID, "pending").First(&pending).Error; err == nil {
		t.SendMsgToTgbot(message.Chat.ID, "You already have a renewal request waiting for admin review.")
		return true
	}
	req := &model.TelegramRenewalRequest{Email: flow.Email, TgID: message.Chat.ID, PlanID: flow.Plan.ID, PlanName: flow.Plan.Name, TotalGB: flow.Plan.GB, Days: flow.Plan.Days, Status: "pending", ReceiptChatID: message.Chat.ID, ReceiptMessageID: message.MessageID, ReceiptFileID: fileID, ReceiptType: receiptType, CreatedAt: time.Now().UnixMilli()}
	if err := database.GetDB().Create(req).Error; err != nil {
		t.SendMsgToTgbot(message.Chat.ID, "Could not create the renewal request.")
		logger.Warning("telegram renewal create:", err)
		return true
	}
	price := ""
	if flow.Plan.Price != "" {
		price = "\nPrice: " + html.EscapeString(flow.Plan.Price)
	}
	summary := fmt.Sprintf("🧾 <b>New renewal request #%d</b>\nUser Telegram ID: <code>%d</code>\nAccount: <code>%s</code>\nPlan: <b>%s</b>%s\nReceipt: %s", req.Id, req.TgID, html.EscapeString(req.Email), html.EscapeString(req.PlanName), price, receiptType)
	kb := tu.InlineKeyboard(tu.InlineKeyboardRow(telego.InlineKeyboardButton{Text: "✅ Approve"}.WithCallbackData(fmt.Sprintf("tr:approve:%d", req.Id)), telego.InlineKeyboardButton{Text: "❌ Reject"}.WithCallbackData(fmt.Sprintf("tr:reject:%d", req.Id))))
	for _, adminID := range adminIds {
		if adminID == 0 {
			continue
		}
		t.SendMsgToTgbot(adminID, summary, kb)
		if _, err := bot.CopyMessage(context.Background(), tu.CopyMessage(tu.ID(adminID), tu.ID(message.Chat.ID), message.MessageID)); err != nil {
			logger.Warning("telegram renewal receipt copy:", err)
		}
	}
	t.SendMsgToTgbot(message.Chat.ID, "✅ Your receipt was received. The admin will review it and notify you.")
	return true
}
func (t *Tgbot) approveTelegramRenewal(adminChatID int64, q *telego.CallbackQuery, requestID int64) {
	var req model.TelegramRenewalRequest
	err := database.GetDB().Transaction(func(tx *gorm.DB) error {
		claim := tx.Model(&model.TelegramRenewalRequest{}).Where("id = ? AND status = ?", requestID, "pending").
			Updates(map[string]any{"status": "processing", "reviewedAt": time.Now().UnixMilli(), "reviewedBy": q.From.ID})
		if claim.Error != nil || claim.RowsAffected != 1 {
			return fmt.Errorf("request is no longer pending")
		}
		if err := tx.First(&req, requestID).Error; err != nil {
			return err
		}
		var account model.Account
		if err := tx.Where("email = ? AND tg_id = ?", req.Email, req.TgID).First(&account).Error; err != nil {
			return fmt.Errorf("linked account not found")
		}
		now := time.Now()
		if req.TotalGB == 0 {
			account.TotalGB = 0
		} else if account.TotalGB > 0 {
			account.TotalGB += req.TotalGB * oneGB
		}
		if req.Days > 0 {
			base := now
			if account.ExpiryTime > now.UnixMilli() {
				base = time.UnixMilli(account.ExpiryTime)
			}
			account.ExpiryTime = base.Add(time.Duration(req.Days) * 24 * time.Hour).UnixMilli()
		}
		if err := tx.Save(&account).Error; err != nil {
			return err
		}
		svc := AccountService{}
		if _, err := svc.ProjectAccount(tx, account.Id); err != nil {
			return err
		}
		return tx.Model(&model.TelegramRenewalRequest{}).Where("id = ? AND status = ?", requestID, "processing").Updates(map[string]any{"status": "approved", "reviewedAt": now.UnixMilli(), "reviewedBy": q.From.ID}).Error
	})
	if err != nil {
		t.sendCallbackAnswerTgBot(q.ID, "Approval failed: "+err.Error())
		return
	}
	t.xrayService.SetToNeedRestart()
	t.editMessageTgBot(adminChatID, q.Message.GetMessageID(), "✅ Renewal request approved.")
	t.SendMsgToTgbot(req.TgID, fmt.Sprintf("✅ Your renewal was approved.\nAccount: <code>%s</code>\nPlan: <b>%s</b>", html.EscapeString(req.Email), html.EscapeString(req.PlanName)))
	t.sendCallbackAnswerTgBot(q.ID, "Approved.")
}

func (t *Tgbot) rejectTelegramRenewal(adminChatID int64, q *telego.CallbackQuery, requestID int64) {
	now := time.Now().UnixMilli()
	result := database.GetDB().Model(&model.TelegramRenewalRequest{}).Where("id = ? AND status = ?", requestID, "pending").Updates(map[string]any{"status": "rejected", "reviewedAt": now, "reviewedBy": q.From.ID})
	if result.Error != nil {
		t.sendCallbackAnswerTgBot(q.ID, "Reject failed.")
		return
	}
	if result.RowsAffected == 0 {
		t.sendCallbackAnswerTgBot(q.ID, "Request is already reviewed.")
		return
	}
	var req model.TelegramRenewalRequest
	if err := database.GetDB().First(&req, requestID).Error; err == nil {
		t.SendMsgToTgbot(req.TgID, fmt.Sprintf("❌ Your renewal request was rejected.\nAccount: <code>%s</code>\nPlan: <b>%s</b>", html.EscapeString(req.Email), html.EscapeString(req.PlanName)))
	}
	t.editMessageTgBot(adminChatID, q.Message.GetMessageID(), "❌ Renewal request rejected.")
	t.sendCallbackAnswerTgBot(q.ID, "Rejected.")
}
