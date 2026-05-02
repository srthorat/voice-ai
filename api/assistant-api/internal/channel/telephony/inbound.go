// Copyright (c) 2023-2025 RapidaAI
// Author: Prashant Srivastav <prashant@rapida.ai>
//
// Licensed under GPL-2.0 with Rapida Additional Terms.
// See LICENSE.md or contact sales@rapida.ai for commercial usage.

package channel_telephony

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/rapidaai/api/assistant-api/config"
	callcontext "github.com/rapidaai/api/assistant-api/internal/callcontext"
	internal_assistant_entity "github.com/rapidaai/api/assistant-api/internal/entity/assistants"
	internal_services "github.com/rapidaai/api/assistant-api/internal/services"
	internal_type "github.com/rapidaai/api/assistant-api/internal/type"
	web_client "github.com/rapidaai/pkg/clients/web"
	"github.com/rapidaai/pkg/commons"
	"github.com/rapidaai/pkg/types"
	type_enums "github.com/rapidaai/pkg/types/enums"
	"github.com/rapidaai/pkg/utils"
	"github.com/rapidaai/protos"
)

// InboundDispatcher handles inbound call processing across all telephony
// channels (SIP, Asterisk, Twilio, Exotel, Vonage). It encapsulates the
// common business logic: provider resolution, call reception, conversation
// creation, call-context persistence, telemetry application, and session resolution.
type InboundDispatcher struct {
	cfg                 *config.AssistantConfig
	store               callcontext.Store
	logger              commons.Logger
	vaultClient         web_client.VaultClient
	assistantService    internal_services.AssistantService
	conversationService internal_services.AssistantConversationService
	telephonyOpt        TelephonyOption

	createConversation CreateConversationFunc
}

// CreateConversationFunc creates a conversation and returns its ID.
type CreateConversationFunc func(ctx context.Context, auth types.SimplePrinciple, callerNumber string, assistantID, assistantProviderID uint64, direction type_enums.ConversationDirection, source utils.RapidaSource) (conversationID uint64, err error)

// NewInboundDispatcher creates a new inbound call dispatcher.
func NewInboundDispatcher(deps TelephonyDispatcherDeps) *InboundDispatcher {
	return &InboundDispatcher{
		cfg:                 deps.Cfg,
		store:               deps.Store,
		logger:              deps.Logger,
		vaultClient:         deps.VaultClient,
		assistantService:    deps.AssistantService,
		conversationService: deps.ConversationService,
		telephonyOpt:        deps.TelephonyOpt,
		createConversation: func(ctx context.Context, auth types.SimplePrinciple, callerNumber string, assistantID, assistantProviderID uint64, direction type_enums.ConversationDirection, source utils.RapidaSource) (uint64, error) {
			conv, err := deps.ConversationService.CreateConversation(ctx, auth, callerNumber, assistantID, assistantProviderID, direction, source)
			if err != nil {
				return 0, err
			}
			return conv.Id, nil
		},
	}
}

// HandleStatusCallback resolves the telephony provider and processes a status callback
// webhook. It builds telemetry (metric + event) from the StatusInfo returned by the provider.
func (d *InboundDispatcher) HandleStatusCallback(c *gin.Context, provider string, auth types.SimplePrinciple, assistantId, conversationId uint64) error {
	tel, err := GetTelephony(Telephony(provider), d.cfg, d.logger, d.telephonyOpt)
	if err != nil {
		return fmt.Errorf("invalid telephony provider %s: %w", provider, err)
	}

	statusInfo, err := tel.StatusCallback(c, auth, assistantId, conversationId)
	if err != nil {
		return fmt.Errorf("status callback failed: %w", err)
	}
	if statusInfo == nil {
		return nil
	}

	d.logger.Infow("Status callback received",
		"provider", provider,
		"event", statusInfo.Event,
		"assistant_id", assistantId,
		"conversation_id", conversationId)

	// Persist callback event as metadata so it's visible in conversation history.
	// Terminal events (completed, failed, busy, no-answer, canceled) also persist a status metric.
	if d.conversationService != nil {
		metadata := []*types.Metadata{
			types.NewMetadata("telephony.callback.event", statusInfo.Event),
			types.NewMetadata("telephony.callback.provider", provider),
		}
		if err := d.conversationService.PersistMetadata(c, auth, assistantId, conversationId, metadata); err != nil {
			d.logger.Warnw("Failed to persist callback metadata", "error", err)
		}

		switch statusInfo.Event {
		case "completed":
			d.conversationService.PersistMetrics(c, auth, assistantId, conversationId, []*types.Metric{
				{Name: "status", Value: "COMPLETED", Description: "call_completed_callback"},
			})
		case "failed", "busy", "no-answer", "canceled", "rejected":
			d.conversationService.PersistMetrics(c, auth, assistantId, conversationId, []*types.Metric{
				{Name: "status", Value: "FAILED", Description: statusInfo.Event},
			})
		}
	}

	return nil
}

// HandleStatusCallbackByContext resolves a call context from Postgres using the contextId and
// processes the status callback. Unlike ResolveCallSessionByContext, this reads the context
// without changing its status, since status callbacks can fire multiple times during a call
// and may arrive asynchronously even after the call has ended (status="completed").
func (d *InboundDispatcher) HandleStatusCallbackByContext(c *gin.Context, contextID string) error {
	cc, err := d.store.Get(c, contextID)
	if err != nil {
		d.logger.Errorf("failed to resolve call context %s for event callback: %v", contextID, err)
		return fmt.Errorf("call context not found or expired: %w", err)
	}

	auth := cc.ToAuth()
	return d.HandleStatusCallback(c, cc.Provider, auth, cc.AssistantID, cc.ConversationID)
}

// ResolveVaultCredential fetches the vault credential for the given assistant.
// This is the only DB round-trip needed — call IDs (assistant, conversation,
// provider) are already in the CallContext from Redis.
func (d *InboundDispatcher) ResolveVaultCredential(ctx context.Context, auth types.SimplePrinciple, assistantId, conversationId uint64) (*protos.VaultCredential, error) {
	assistant, err := d.assistantService.Get(ctx, auth, assistantId, nil, &internal_services.GetAssistantOption{InjectPhoneDeployment: true})
	if err != nil {
		return nil, err
	}
	if !assistant.IsPhoneDeploymentEnable() {
		return nil, fmt.Errorf("phone deployment not enabled for assistant %d", assistantId)
	}
	credentialID, err := assistant.AssistantPhoneDeployment.GetOptions().GetUint64("rapida.credential_id")
	if err != nil {
		return nil, err
	}
	vltC, err := d.vaultClient.GetCredential(ctx, auth, credentialID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve vault credential: %w", err)
	}
	return vltC, nil
}

// ResolveCallSessionByContext resolves a call context and vault credential using
// a contextId stored in Postgres. The call context is atomically claimed by
// transitioning its status from "pending" to "claimed". Only one media connection
// can claim a given context — subsequent callers get an error.
// The context remains in Postgres so that event/status callbacks can still read it.
// Returns the CallContext (which contains all IDs and auth info) plus the vault
// credential needed for the streamer.
func (d *InboundDispatcher) ResolveCallSessionByContext(ctx context.Context, contextID string) (*callcontext.CallContext, *protos.VaultCredential, error) {
	cc, err := d.store.Claim(ctx, contextID)
	if err != nil {
		d.logger.Errorf("failed to resolve call context %s: %v", contextID, err)
		return nil, nil, fmt.Errorf("call context not found or already claimed: %w", err)
	}

	auth := cc.ToAuth()
	vaultCred, err := d.ResolveVaultCredential(ctx, auth, cc.AssistantID, cc.ConversationID)
	if err != nil {
		return nil, nil, err
	}
	return cc, vaultCred, nil
}

// CompleteCallSession marks a call context as claimed (call ended).
// Called when the call/session ends (talker exits).
func (d *InboundDispatcher) CompleteCallSession(ctx context.Context, contextID string) {
	if _, err := d.store.Claim(ctx, contextID); err != nil {
		d.logger.Warnf("failed to claim call context %s: %v", contextID, err)
	}
}

// ReceiveCall parses the provider webhook and returns CallInfo.
func (d *InboundDispatcher) ReceiveCall(c *gin.Context, provider string) (*internal_type.CallInfo, error) {
	tel, err := GetTelephony(Telephony(provider), d.cfg, d.logger, d.telephonyOpt)
	if err != nil {
		return nil, fmt.Errorf("telephony provider %s not connected: %w", provider, err)
	}
	return tel.ReceiveCall(c)
}

// LoadAssistant loads the assistant entity with phone deployment.
func (d *InboundDispatcher) LoadAssistant(ctx context.Context, auth types.SimplePrinciple, assistantID uint64) (*internal_assistant_entity.Assistant, error) {
	return d.assistantService.Get(ctx, auth, assistantID, utils.GetVersionDefinition("latest"), &internal_services.GetAssistantOption{InjectPhoneDeployment: true})
}

// CreateConversation creates a conversation and returns its ID.
func (d *InboundDispatcher) CreateConversation(ctx context.Context, auth types.SimplePrinciple, callerNumber string, assistantID, assistantProviderID uint64, direction string) (uint64, error) {
	dir := type_enums.DIRECTION_INBOUND
	if direction == "outbound" {
		dir = type_enums.DIRECTION_OUTBOUND
	}
	return d.createConversation(ctx, auth, callerNumber, assistantID, assistantProviderID, dir, utils.PhoneCall)
}

// SaveCallContext stores the call context in Postgres and returns the contextID.
func (d *InboundDispatcher) SaveCallContext(ctx context.Context, auth types.SimplePrinciple, assistant *internal_assistant_entity.Assistant, conversationID uint64, callInfo *internal_type.CallInfo, provider string) (string, error) {
	direction := callInfo.Direction
	if direction == "" {
		direction = "inbound"
	}
	cc := &callcontext.CallContext{
		AssistantID:         assistant.Id,
		ConversationID:      conversationID,
		AssistantProviderId: assistant.AssistantProviderId,
		AuthToken:           auth.GetCurrentToken(),
		AuthType:            auth.Type(),
		Direction:           direction,
		CallerNumber:        callInfo.CallerNumber,
		FromNumber:          callInfo.FromNumber,
		Provider:            provider,
		ChannelUUID:         callInfo.ChannelUUID,
	}
	if auth.GetCurrentProjectId() != nil {
		cc.ProjectID = *auth.GetCurrentProjectId()
	}
	if auth.GetCurrentOrganizationId() != nil {
		cc.OrganizationID = *auth.GetCurrentOrganizationId()
	}
	return d.store.Save(ctx, cc)
}

// AnswerProvider instructs the telephony provider to answer the call.
// For providers that make synchronous API calls during InboundCall (e.g. Telnyx
// answer + streaming_start), the vault credential is resolved here and set on
// the gin context so InboundCall can use it without a second DB lookup.
func (d *InboundDispatcher) AnswerProvider(c *gin.Context, auth types.SimplePrinciple, provider string, assistantID uint64, callerNumber string, conversationID uint64) error {
	tel, err := GetTelephony(Telephony(provider), d.cfg, d.logger, d.telephonyOpt)
	if err != nil {
		return fmt.Errorf("telephony provider %s not connected: %w", provider, err)
	}
	if _, exists := c.Get("vault_credential"); !exists {
		vaultCred, err := d.ResolveVaultCredential(c, auth, assistantID, conversationID)
		if err != nil {
			return fmt.Errorf("AnswerProvider: failed to resolve vault credential for assistant %d: %w", assistantID, err)
		}
		c.Set("vault_credential", vaultCred)
	}
	return tel.InboundCall(c, auth, assistantID, callerNumber, conversationID)
}
