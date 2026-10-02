package job

import "github.com/mhsanaei/3x-ui/v2/web/service"

type WhatsAppExpiryReminderJob struct {
	service service.WhatsAppService
}

func NewWhatsAppExpiryReminderJob() *WhatsAppExpiryReminderJob {
	return &WhatsAppExpiryReminderJob{}
}

func (j *WhatsAppExpiryReminderJob) Run() {
	j.service.SendExpiryReminders()
}
