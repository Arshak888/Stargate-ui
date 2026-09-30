class AllSetting {

    constructor(data) {
        this.webListen = "";
        this.webDomain = "";
        this.webPort = 2053;
        this.webCertFile = "";
        this.webKeyFile = "";
        this.webBasePath = "/";
        this.panelBrandName = "Stargate UI";
        this.loginLogoURL = "";
        this.loginTitle = "Welcome back";
        this.loginSubtitle = "Sign in to your panel";
        this.sessionMaxAge = 360;
        this.pageSize = 25;
        this.expireDiff = 0;
        this.trafficDiff = 0;
        this.remarkModel = "-ieo";
        this.datepicker = "gregorian";
        this.tgBotEnable = false;
        this.tgBotToken = "";
        this.tgBotProxy = "";
        this.tgBotAPIServer = "";
        this.tgBotChatId = "";
        this.tgRunTime = "@daily";
        this.tgBotBackup = false;
        this.tgBotLoginNotify = true;
        this.tgCpu = 80;
        this.tgLang = "en-US";
        this.tgExpiryReminderRuntime = "@every 10m";
        this.tgExpiryReminder2Days = 2;
        this.tgExpiryReminder1Day = 1;
        this.tgExpiryReminderOnExpire = true;
        this.tgRenewalEnable = false;
        this.tgRenewalPaymentInfo = "";
        this.tgRenewalPlans = '[{"id":"20gb-30d","name":"20 GB / 30 days","gb":20,"days":30,"price":"240000"},{"id":"30gb-30d","name":"30 GB / 30 days","gb":30,"days":30,"price":"360000"},{"id":"40gb-30d","name":"40 GB / 30 days","gb":40,"days":30,"price":"480000"},{"id":"50gb-30d","name":"50 GB / 30 days","gb":50,"days":30,"price":"600000"},{"id":"unlimited-30d","name":"Unlimited / 30 days","gb":0,"days":30,"price":"600000"}]';
        this.twoFactorEnable = false;
        this.twoFactorToken = "";
        this.xrayTemplateConfig = "";
        this.subEnable = true;
        this.subJsonEnable = false;
        this.subTitle = "";
        this.subSupportUrl = "";
        this.subProfileUrl = "";
        this.subAnnounce = "";
        this.subEnableRouting = true;
        this.subRoutingRules = "";
        this.subListen = "";
        this.subPort = 2096;
        this.subPath = "/sub/";
        this.subJsonPath = "/json/";
        this.subClashEnable = true;
        this.subClashPath = "/clash/";
        this.subDomain = "";
        this.externalTrafficInformEnable = false;
        this.externalTrafficInformURI = "";
        this.subCertFile = "";
        this.subKeyFile = "";
        this.subUpdates = 12;
        this.subEncrypt = true;
        this.subShowInfo = true;
        this.subURI = "";
        this.subJsonURI = "";
        this.subClashURI = "";
        this.subJsonFragment = "";
        this.subJsonNoises = "";
        this.subJsonMux = "";
        this.subJsonRules = "";

        this.timeLocation = "Local";

        // LDAP settings
        this.ldapEnable = false;
        this.ldapHost = "";
        this.ldapPort = 389;
        this.ldapUseTLS = false;
        this.ldapBindDN = "";
        this.ldapPassword = "";
        this.ldapBaseDN = "";
        this.ldapUserFilter = "(objectClass=person)";
        this.ldapUserAttr = "mail";
        this.ldapVlessField = "vless_enabled";
        this.ldapSyncCron = "@every 1m";
        this.ldapFlagField = "";
        this.ldapTruthyValues = "true,1,yes,on";
        this.ldapInvertFlag = false;
        this.ldapInboundTags = "";
        this.ldapAutoCreate = false;
        this.ldapAutoDelete = false;
        this.ldapDefaultTotalGB = 0;
        this.ldapDefaultExpiryDays = 0;
        this.ldapDefaultLimitIP = 0;

        if (data == null) {
            return
        }
        ObjectUtil.cloneProps(this, data);
    }

    equals(other) {
        return ObjectUtil.equals(this, other);
    }
}