package service

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/mymmrac/telego"
	tu "github.com/mymmrac/telego/telegoutil"
)

// SubmitWebRenewalRequest creates a renewal request for a public subscription page.
// The receipt is kept in memory only and is sent directly to Telegram. No receipt
// file is written to the VPS filesystem.
func SubmitWebRenewalRequest(email, planID, receiptType, filename string, receipt []byte) error {
	t := &Tgbot{settingService: SettingService{}}
	enabled, _, plans, err := t.telegramRenewalSettings()
	if err != nil {
		return err
	}
	if !enabled {
		return fmt.Errorf("customer renewal is disabled")
	}
	email = strings.TrimSpace(email)
	if email == "" {
		return fmt.Errorf("invalid account")
	}
	var account model.Account
	if err := database.GetDB().Where("LOWER(TRIM(email)) = ?", strings.ToLower(email)).First(&account).Error; err != nil {
		return fmt.Errorf("account not found")
	}

	var plan telegramRenewalPlan
	found := false
	for _, p := range plans {
		if p.ID == planID {
			plan = p
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("renewal plan not found")
	}
	if len(receipt) == 0 {
		return fmt.Errorf("receipt is empty")
	}
	const maxReceipt = 10 << 20
	if len(receipt) > maxReceipt {
		return fmt.Errorf("receipt is too large (max 10 MB)")
	}
	receiptType = strings.ToLower(strings.TrimSpace(receiptType))
	if receiptType != "photo" && receiptType != "document" {
		receiptType = "document"
	}
	if filename == "" {
		filename = "receipt"
	}

	var pending model.TelegramRenewalRequest
	if err := database.GetDB().Where("email = ? AND status = ?", account.Email, "pending").First(&pending).Error; err == nil {
		return fmt.Errorf("a renewal request for this account is already pending")
	}

	if bot == nil || len(adminIds) == 0 {
		return fmt.Errorf("telegram admin notification is not available")
	}

	req := &model.TelegramRenewalRequest{
		Email: account.Email,
		TgID: account.TgID,
		PlanID: plan.ID,
		PlanName: plan.Name,
		TotalGB: plan.GB,
		Days: plan.Days,
		Status: "pending",
		CreatedAt: time.Now().UnixMilli(),
		ReceiptType: receiptType,
	}
	if err := database.GetDB().Create(req).Error; err != nil {
		return err
	}

	price := ""
	if plan.Price != "" {
		price = "
Price: " + html.EscapeString(plan.Price)
	}
	summary := fmt.Sprintf("🌐 <b>Web renewal request #%d</b>
Account: <code>%s</code>
Plan: <b>%s</b>%s
Source: public subscription page",
		req.Id, html.EscapeString(req.Email), html.EscapeString(req.PlanName), price)
	kb := tu.InlineKeyboard(tu.InlineKeyboardRow(
		telego.InlineKeyboardButton{Text: "✅ Approve"}.WithCallbackData(fmt.Sprintf("tr:approve:%d", req.Id)),
		telego.InlineKeyboardButton{Text: "❌ Reject"}.WithCallbackData(fmt.Sprintf("tr:reject:%d", req.Id)),
	))

	var sentFileID string
	var sentMessageID int
	successfulAdmins := 0
	for _, adminID := range adminIds {
		if adminID == 0 {
			continue
		}
		if _, sendErr := bot.SendMessage(context.Background(), tu.Message(tu.ID(adminID), summary).WithReplyMarkup(kb)); sendErr != nil {
			logger.Warning("web renewal admin summary:", sendErr)
			continue
		}

		var sent *telego.Message
		var sendErr error
		file := tu.FileFromBytes(receipt, filename)
		if receiptType == "photo" {
			sent, sendErr = bot.SendPhoto(context.Background(), tu.Photo(tu.ID(adminID), file))
		} else {
			sent, sendErr = bot.SendDocument(context.Background(), tu.Document(tu.ID(adminID), file))
		}
		if sendErr != nil {
			logger.Warning("web renewal admin receipt:", sendErr)
			continue
		}
		successfulAdmins++
		if sentMessageID == 0 {
			sentMessageID = sent.GetMessageID()
			if receiptType == "photo" && len(sent.Photo) > 0 {
				sentFileID = sent.Photo[len(sent.Photo)-1].FileID
			} else if sent.Document != nil {
				sentFileID = sent.Document.FileID
			}
		}
	}

	if successfulAdmins == 0 {
		_ = database.GetDB().Delete(&model.TelegramRenewalRequest{}, req.Id).Error
		return fmt.Errorf("could not send the receipt to Telegram admins")
	}

	updates := map[string]any{"receiptMessageId": sentMessageID, "receiptFileId": sentFileID}
	if err := database.GetDB().Model(&model.TelegramRenewalRequest{}).Where("id = ?", req.Id).Updates(updates).Error; err != nil {
		logger.Warning("web renewal receipt metadata:", err)
	}
	return nil
}
