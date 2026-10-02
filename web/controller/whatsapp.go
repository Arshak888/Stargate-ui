package controller

import (
	"net/http"

	"github.com/mhsanaei/3x-ui/v2/web/service"
	"github.com/gin-gonic/gin"
)

type WhatsAppController struct {
	service *service.WhatsAppService
}

func NewWhatsAppController(r *gin.Engine) *WhatsAppController {
	c := &WhatsAppController{service: service.NewWhatsAppService()}
	r.GET("/whatsapp/webhook", c.verify)
	r.POST("/whatsapp/webhook", c.receive)
	return c
}

func (c *WhatsAppController) verify(ctx *gin.Context) {
	cfg, err := c.serviceSettings()
	if err != nil {
		ctx.Status(http.StatusForbidden)
		return
	}
	if ctx.Query("hub.mode") != "subscribe" || ctx.Query("hub.verify_token") != cfg {
		ctx.Status(http.StatusForbidden)
		return
	}
	ctx.String(http.StatusOK, ctx.Query("hub.challenge"))
}

func (c *WhatsAppController) serviceSettings() (string, error) {
	all, err := service.NewWhatsAppServiceSettings()
	return all, err
}

func (c *WhatsAppController) receive(ctx *gin.Context) {
	body, err := ctx.GetRawData()
	if err != nil {
		ctx.Status(http.StatusBadRequest)
		return
	}
	if !c.service.VerifyWebhook(ctx.GetHeader("X-Hub-Signature-256"), body) {
		ctx.Status(http.StatusUnauthorized)
		return
	}
	c.service.HandleWebhook(body)
	ctx.Status(http.StatusOK)
}
