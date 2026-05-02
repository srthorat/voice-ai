// Copyright (c) 2023-2025 RapidaAI
// Author: Prashant Srivastav <prashant@rapida.ai>
//
// Licensed under GPL-2.0 with Rapida Additional Terms.
// See LICENSE.md or contact sales@rapida.ai for commercial usage.

package channel_pipeline

import (
	"context"
	"fmt"

	obs "github.com/rapidaai/api/assistant-api/internal/observe"
	"github.com/rapidaai/pkg/types"
)

func (d *Dispatcher) runInboundCall(ctx context.Context, v CallReceivedPipeline) *PipelineResult {
	d.logger.Infow("Pipeline: InboundCall", "provider", v.Provider, "assistant_id", v.AssistantID)

	if d.onReceiveCall == nil {
		return &PipelineResult{Error: ErrCallbackNotConfigured}
	}
	callInfo, err := d.onReceiveCall(ctx, v.Provider, v.GinContext)
	if err != nil {
		return &PipelineResult{Error: err}
	}
	if callInfo == nil {
		return &PipelineResult{}
	}

	if d.onLoadAssistant == nil {
		return &PipelineResult{Error: ErrCallbackNotConfigured}
	}
	assistant, err := d.onLoadAssistant(ctx, v.Auth, v.AssistantID)
	if err != nil {
		return &PipelineResult{Error: err}
	}

	if d.onCreateConversation == nil || d.onSaveCallContext == nil {
		return &PipelineResult{Error: ErrCallbackNotConfigured}
	}
	conversationID, err := d.onCreateConversation(ctx, v.Auth, callInfo.CallerNumber, assistant.Id, assistant.AssistantProviderId, "inbound")
	if err != nil {
		return &PipelineResult{Error: err}
	}

	contextID, err := d.onSaveCallContext(ctx, v.Auth, assistant, conversationID, callInfo, v.Provider)
	if err != nil {
		return &PipelineResult{Error: err}
	}

	var observer *obs.ConversationObserver
	if d.onCreateObserver != nil {
		observer = d.onCreateObserver(ctx, contextID, v.Auth, v.AssistantID, conversationID)
	}

	if observer != nil {
		if len(callInfo.Extra) > 0 {
			metadata := make([]*types.Metadata, 0, len(callInfo.Extra))
			for k, val := range callInfo.Extra {
				metadata = append(metadata, types.NewMetadata(k, val))
			}
			observer.EmitMetadata(ctx, metadata)
		}
		if callInfo.StatusInfo.Event != "" {
			observer.EmitEvent(ctx, obs.ComponentTelephony, map[string]string{
				obs.DataType:     callInfo.StatusInfo.Event,
				obs.DataProvider: v.Provider,
				obs.DataCaller:   callInfo.CallerNumber,
			})
		}
		observer.EmitEvent(ctx, obs.ComponentTelephony, map[string]string{
			obs.DataType:      obs.EventCallReceived,
			obs.DataProvider:  v.Provider,
			obs.DataContextID: contextID,
			"conversation_id": fmt.Sprintf("%d", conversationID),
		})
	}

	if observer != nil {
		observer.Shutdown(ctx)
	}

	v.GinContext.Set("contextId", contextID)
	// Expose call_control_id (and any Extra fields) so provider-specific InboundCall
	// implementations that make API callbacks (e.g. Telnyx answer + streaming_start)
	// can read them without an extra Postgres round-trip.
	if callInfo.ChannelUUID != "" {
		v.GinContext.Set("call_control_id", callInfo.ChannelUUID)
	}
	for k, val := range callInfo.Extra {
		v.GinContext.Set(k, val)
	}
	if d.onAnswerProvider != nil {
		if err := d.onAnswerProvider(ctx, v.GinContext, v.Auth, v.Provider, v.AssistantID, callInfo.CallerNumber, conversationID); err != nil {
			return &PipelineResult{Error: err}
		}
	}

	return &PipelineResult{ContextID: contextID, ConversationID: conversationID}
}
