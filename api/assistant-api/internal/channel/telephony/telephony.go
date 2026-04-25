// Copyright (c) 2023-2025 RapidaAI
// Author: Prashant Srivastav <prashant@rapida.ai>
//
// Licensed under GPL-2.0 with Rapida Additional Terms.
// See LICENSE.md or contact sales@rapida.ai for commercial usage.

package channel_telephony

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/gorilla/websocket"
	"github.com/rapidaai/api/assistant-api/config"
	callcontext "github.com/rapidaai/api/assistant-api/internal/callcontext"
	internal_asterisk_telephony "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/asterisk"
	internal_asterisk_audiosocket "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/asterisk/audiosocket"
	internal_asterisk_websocket "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/asterisk/websocket"
	internal_exotel_telephony "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/exotel"
	internal_sip_telephony "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/sip"
	internal_telnyx_telephony "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/telnyx"
	internal_twilio_telephony "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/twilio"
	internal_vonage_telephony "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/vonage"
	internal_services "github.com/rapidaai/api/assistant-api/internal/services"
	internal_type "github.com/rapidaai/api/assistant-api/internal/type"
	sip_infra "github.com/rapidaai/api/assistant-api/sip/infra"
	web_client "github.com/rapidaai/pkg/clients/web"
	"github.com/rapidaai/pkg/commons"
	"github.com/rapidaai/protos"
)

// Telephony is a string type identifying a telephony provider.
type Telephony string

const (
	Twilio   Telephony = "twilio"
	Exotel   Telephony = "exotel"
	Vonage   Telephony = "vonage"
	Telnyx   Telephony = "telnyx"
	Asterisk Telephony = "asterisk"
	SIP      Telephony = "sip"
)

func (at Telephony) String() string {
	return string(at)
}

// GetTelephony is the factory function that creates a telephony provider for the
// given type. This follows the platform factory pattern — providers are created
// per-request through a switch-based lookup.
//
// For SIP, the caller must supply the SIPServer via TelephonyOption.
func GetTelephony(at Telephony, cfg *config.AssistantConfig, logger commons.Logger, opts ...TelephonyOption) (internal_type.Telephony, error) {
	var opt TelephonyOption
	if len(opts) > 0 {
		opt = opts[0]
	}

	switch at {
	case Twilio:
		return internal_twilio_telephony.NewTwilioTelephony(cfg, logger)
	case Exotel:
		return internal_exotel_telephony.NewExotelTelephony(cfg, logger)
	case Vonage:
		return internal_vonage_telephony.NewVonageTelephony(cfg, logger)
	case Asterisk:
		return internal_asterisk_telephony.NewAsteriskTelephony(cfg, logger)
	case Telnyx:
		return internal_telnyx_telephony.NewTelnyxTelephony(cfg, logger)
	case SIP:
		if opt.SIPServer == nil {
			return nil, errors.New("SIP server not available — SIP telephony requires a running SIP server")
		}
		return internal_sip_telephony.NewSIPTelephony(cfg, logger, opt.SIPServer)
	default:
		return nil, fmt.Errorf("unknown telephony provider %q", at)
	}
}

// TelephonyOption configures optional dependencies for telephony providers.
type TelephonyOption struct {
	SIPServer *sip_infra.Server
}

// TelephonyDispatcherDeps contains the shared dependencies used by both
// InboundDispatcher and OutboundDispatcher.
type TelephonyDispatcherDeps struct {
	Cfg                 *config.AssistantConfig
	Logger              commons.Logger
	Store               callcontext.Store
	VaultClient         web_client.VaultClient
	AssistantService    internal_services.AssistantService
	ConversationService internal_services.AssistantConversationService
	TelephonyOpt        TelephonyOption
}

// StreamerOption carries the transport-specific parameters needed to construct a
// streamer. Callers populate only the fields relevant to their transport:
//
//   - WebSocket providers (Twilio, Exotel, Vonage, Asterisk WS): set WebSocketConn
//   - AudioSocket (Asterisk): set AudioSocketConn, AudioSocketReader, AudioSocketWriter
//   - SIP: set Ctx, SIPSession, SIPConfig
type StreamerOption struct {
	// WebSocket transport
	WebSocketConn *websocket.Conn

	// AudioSocket transport (Asterisk)
	AudioSocketConn   net.Conn
	AudioSocketReader *bufio.Reader
	AudioSocketWriter *bufio.Writer

	// SIP transport
	Ctx        context.Context
	SIPSession *sip_infra.Session
	SIPConfig  *sip_infra.Config
}

// NewStreamer is the unified streamer factory. It creates a transport-specific
// streamer based on the telephony provider, using the CallContext (identity) and
// vault credential (secrets) that are common across all transports.
func (at Telephony) NewStreamer(
	logger commons.Logger,
	cc *callcontext.CallContext,
	vaultCred *protos.VaultCredential,
	opt StreamerOption,
) (internal_type.Streamer, error) {
	switch at {
	case Twilio:
		return internal_twilio_telephony.NewTwilioWebsocketStreamer(logger, opt.WebSocketConn, cc, vaultCred), nil
	case Exotel:
		return internal_exotel_telephony.NewExotelWebsocketStreamer(logger, opt.WebSocketConn, cc, vaultCred), nil
	case Vonage:
		return internal_vonage_telephony.NewVonageWebsocketStreamer(logger, opt.WebSocketConn, cc, vaultCred), nil
	case Asterisk:
		if opt.AudioSocketConn != nil {
			return internal_asterisk_audiosocket.NewStreamer(logger, opt.AudioSocketConn, opt.AudioSocketReader, opt.AudioSocketWriter, cc, vaultCred)
		}
		return internal_asterisk_websocket.NewAsteriskWebsocketStreamer(logger, opt.WebSocketConn, cc, vaultCred)
	case Telnyx:
		return internal_telnyx_telephony.NewTelnyxWebsocketStreamer(logger, opt.WebSocketConn, cc, vaultCred), nil
	case SIP:
		return internal_sip_telephony.NewStreamer(opt.Ctx, logger, opt.SIPSession, cc, vaultCred)
	default:
		return nil, fmt.Errorf("streamer not supported for provider %q", at)
	}
}
