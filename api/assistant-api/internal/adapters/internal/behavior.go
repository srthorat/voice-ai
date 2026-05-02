// Copyright (c) 2023-2025 RapidaAI
// Author: Prashant Srivastav <prashant@rapida.ai>
//
// Licensed under GPL-2.0 with Rapida Additional Terms.
// See LICENSE.md or contact sales@rapida.ai for commercial usage.
package adapter_internal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	internal_assistant_entity "github.com/rapidaai/api/assistant-api/internal/entity/assistants"
	internal_type "github.com/rapidaai/api/assistant-api/internal/type"
	"github.com/rapidaai/pkg/utils"
	"github.com/rapidaai/protos"
)

var errDeploymentNotEnabled = errors.New("deployment is not enabled for source")

// GetBehavior retrieves the deployment behavior configuration based on the source type.
func (r *genericRequestor) GetBehavior() (*internal_assistant_entity.AssistantDeploymentBehavior, error) {
	if r.assistant == nil {
		return nil, errDeploymentNotEnabled
	}

	switch r.source {
	case utils.PhoneCall:
		if r.assistant.AssistantPhoneDeployment != nil {
			return &r.assistant.AssistantPhoneDeployment.AssistantDeploymentBehavior, nil
		}
	case utils.Whatsapp:
		if r.assistant.AssistantWhatsappDeployment != nil {
			return &r.assistant.AssistantWhatsappDeployment.AssistantDeploymentBehavior, nil
		}
	case utils.SDK:
		if r.assistant.AssistantApiDeployment != nil {
			return &r.assistant.AssistantApiDeployment.AssistantDeploymentBehavior, nil
		}
	case utils.WebPlugin:
		if r.assistant.AssistantWebPluginDeployment != nil {
			return &r.assistant.AssistantWebPluginDeployment.AssistantDeploymentBehavior, nil
		}
	case utils.Debugger:
		if r.assistant.AssistantDebuggerDeployment != nil {
			return &r.assistant.AssistantDebuggerDeployment.AssistantDeploymentBehavior, nil
		}
	}

	return nil, errDeploymentNotEnabled
}

// InitializeBehavior sets up the initial behavior configuration including greeting,
// idle timeout, and max session duration timers.
func (r *genericRequestor) initializeBehavior(ctx context.Context) error {
	behavior, err := r.GetBehavior()
	if err != nil {
		r.logger.Errorf("error while fetching deployment behavior: %v", err)
		return nil
	}
	r.initializeGreeting(ctx, behavior)
	r.initializeIdleTimeout(ctx, behavior)
	r.initializeMaxSessionDuration(ctx, behavior)
	return nil
}

// initializeGreeting sends the greeting message if configured.
func (r *genericRequestor) initializeGreeting(ctx context.Context, behavior *internal_assistant_entity.AssistantDeploymentBehavior) {
	if behavior.Greeting == nil {
		r.logger.Warnf("initializeGreeting: no greeting configured for this deployment (source=%v)", r.source)
		return
	}

	greetingContent := r.templateParser.Parse(*behavior.Greeting, r.GetArgs())
	if strings.TrimSpace(greetingContent) == "" {
		return
	}

	// r.Transition(Interrupted)
	if err := r.OnPacket(ctx,
		internal_type.InjectMessagePacket{ContextID: r.GetID(), Text: greetingContent},
		internal_type.ConversationEventPacket{
			Name: "behavior",
			Data: map[string]string{"type": "greeting", "text_chars": fmt.Sprintf("%d", len(greetingContent))},
			Time: time.Now(),
		},
	); err != nil {
		r.logger.Errorf("error while sending greeting message: %v", err)
	}
}

// initializeIdleTimeout starts the idle timeout timer if configured.
func (r *genericRequestor) initializeIdleTimeout(ctx context.Context, behavior *internal_assistant_entity.AssistantDeploymentBehavior) {
	if behavior.IdleTimeout == nil || *behavior.IdleTimeout <= 0 {
		return
	}
	r.startIdleTimeoutTimer(ctx)
}

// initializeMaxSessionDuration sets up the max session duration timer if configured.
func (r *genericRequestor) initializeMaxSessionDuration(ctx context.Context, behavior *internal_assistant_entity.AssistantDeploymentBehavior) {
	if behavior.MaxSessionDuration == nil || *behavior.MaxSessionDuration <= 0 {
		return
	}

	timeoutDuration := time.Duration(*behavior.MaxSessionDuration) * time.Second
	r.maxSessionTimer = time.AfterFunc(timeoutDuration, func() {
		r.OnPacket(ctx, internal_type.DirectivePacket{
			ContextID: r.GetID(),
			Directive: protos.ConversationDirective_END_CONVERSATION,
			Arguments: map[string]interface{}{
				"reason": "max session duration reached",
			},
		})
	})
}

// OnError handles error scenarios by sending a configured or default error message.
func (r *genericRequestor) OnError(ctx context.Context) error {
	behavior, err := r.GetBehavior()
	if err != nil {
		r.logger.Warnf("no error message configured for assistant")
		return nil
	}

	const defaultMistakeMessage = "Oops! It looks like something went wrong. Let me look into that for you right away. I really appreciate your patience—hang tight while I get this sorted!"

	mistakeContent := defaultMistakeMessage
	if behavior.Mistake != nil {
		mistakeContent = r.templateParser.Parse(*behavior.Mistake, r.GetArgs())
	}

	r.Transition(Interrupted)
	if err := r.OnPacket(ctx,
		internal_type.InjectMessagePacket{ContextID: r.GetID(), Text: mistakeContent},
		internal_type.ConversationEventPacket{
			Name: "behavior",
			Data: map[string]string{"type": "error", "text_chars": fmt.Sprintf("%d", len(mistakeContent))},
			Time: time.Now(),
		},
	); err != nil {
		r.logger.Errorf("error while sending error message: %v", err)
	}

	return nil
}

// OnIdleTimeout handles the behavior when the bot has spoken but the user
// has not responded within the idle timeout duration.
// If configured, it will prompt the user or end the conversation after max retries.
func (r *genericRequestor) onIdleTimeout(ctx context.Context) error {
	behavior, err := r.GetBehavior()
	if err != nil {
		r.logger.Debugf("no idle timeout behavior configured for assistant")
		return nil
	}

	if behavior.IdleTimeout == nil || *behavior.IdleTimeout == 0 {
		return nil
	}

	// Check if max backoff retries reached
	if behavior.IdleTimeoutBackoff != nil && *behavior.IdleTimeoutBackoff > 0 {
		if r.idleTimeoutCount >= *behavior.IdleTimeoutBackoff {
			r.OnPacket(ctx, internal_type.DirectivePacket{
				ContextID: r.GetID(),
				Directive: protos.ConversationDirective_END_CONVERSATION,
				Arguments: map[string]interface{}{
					"reason": "max session duration reached",
				},
			})
			return nil
		}
	}

	r.idleTimeoutCount++
	timeoutContent := r.getIdleTimeoutMessage(behavior)
	if timeoutContent == "" {
		r.logger.Warnf("empty idle timeout message")
		return nil
	}

	maxCount := 0
	if behavior.IdleTimeoutBackoff != nil {
		maxCount = int(*behavior.IdleTimeoutBackoff)
	}
	// Rotate context before injecting the timeout message.
	// InjectMessagePacket routes to outputCh; the context must be rotated
	// before enqueueing so GetID() returns the new context.
	r.Transition(Interrupted)
	// Reinitialize TTS synchronously — the provider enters "completed" state
	// after speaking and needs an interrupt to accept new text. Calling inline
	// guarantees TTS is ready before InjectMessagePacket reaches handleSpeakText.
	if r.textToSpeechTransformer != nil {
		r.textToSpeechTransformer.Transform(ctx, internal_type.InterruptionDetectedPacket{ContextID: r.GetID()})
	}
	if err := r.OnPacket(ctx,
		internal_type.InjectMessagePacket{ContextID: r.GetID(), Text: timeoutContent},
		internal_type.ConversationEventPacket{
			Name: "behavior",
			Data: map[string]string{
				"type":      "idle_timeout",
				"count":     fmt.Sprintf("%d", r.idleTimeoutCount),
				"max_count": fmt.Sprintf("%d", maxCount),
			},
			Time: time.Now(),
		},
	); err != nil {
		r.logger.Errorf("error while sending idle timeout message: %v", err)
	}

	return nil
}

// getIdleTimeoutMessage returns the configured or default idle timeout message.
func (r *genericRequestor) getIdleTimeoutMessage(behavior *internal_assistant_entity.AssistantDeploymentBehavior) string {
	const defaultTimeoutMessage = "Are you still there?"

	if behavior.IdleTimeoutMessage != nil && strings.TrimSpace(*behavior.IdleTimeoutMessage) != "" {
		return r.templateParser.Parse(*behavior.IdleTimeoutMessage, r.GetArgs())
	}

	return defaultTimeoutMessage
}

// StartIdleTimeoutTimer starts a timer that triggers OnIdleTimeout when the bot
// has spoken but the user hasn't responded within the configured duration.
// The inputDuration parameter extends the idle timeout to account for user input time.
func (r *genericRequestor) startIdleTimeoutTimer(ctx context.Context, inputDuration ...time.Duration) {
	if r.idleTimeoutTimer != nil {
		r.idleTimeoutTimer.Stop()
	}

	behavior, err := r.GetBehavior()
	if err != nil {
		return
	}

	if behavior.IdleTimeout == nil || *behavior.IdleTimeout == 0 {
		return
	}

	timeoutDuration := time.Duration(*behavior.IdleTimeout) * time.Second
	if len(inputDuration) > 0 && inputDuration[0] > 0 {
		timeoutDuration += inputDuration[0]
	}

	r.idleTimeoutDeadline = time.Now().Add(timeoutDuration)
	r.idleTimeoutTimer = time.AfterFunc(timeoutDuration, func() {
		if err := r.onIdleTimeout(ctx); err != nil {
			r.logger.Errorf("error while handling idle timeout: %v", err)
		}
	})
}

// extendIdleTimeoutTimer pushes the existing idle timeout further into the future
// by the given duration. Used to account for buffered TTS audio that the client
// is still playing back.
func (r *genericRequestor) extendIdleTimeoutTimer(d time.Duration) {
	if r.idleTimeoutTimer == nil || d <= 0 {
		return
	}
	r.idleTimeoutDeadline = r.idleTimeoutDeadline.Add(d)
	if remaining := time.Until(r.idleTimeoutDeadline); remaining > 0 {
		r.idleTimeoutTimer.Reset(remaining)
	}
}

// stopIdleTimeoutTimerAndResetCount stops the timer AND resets the consecutive
// idle backoff counter. Used when the user actually speaks — signaling active
// engagement and breaking the consecutive idle chain.
func (r *genericRequestor) stopIdleTimeoutTimerAndResetCount() {
	r.stopIdleTimeoutTimer()
	r.idleTimeoutCount = 0
}

// ResetIdleTimeoutTimer resets the idle timeout timer when the user responds,
// indicating they are still engaged in the conversation.
// The inputDuration parameter extends the idle timeout to account for user input time.
func (r *genericRequestor) resetIdleTimeoutTimer(ctx context.Context, inputDuration ...time.Duration) {
	if r.idleTimeoutTimer == nil {
		return
	}
	r.idleTimeoutCount = 0
	r.startIdleTimeoutTimer(ctx, inputDuration...)
}

// stopIdleTimeoutTimer stops the idle timeout timer without resetting the
// retry count. The count is only reset when the user actually speaks
// (handleNormalizedText), not when a system-generated interrupt fires.
func (r *genericRequestor) stopIdleTimeoutTimer() {
	if r.idleTimeoutTimer != nil {
		r.idleTimeoutTimer.Stop()
		r.idleTimeoutTimer = nil
	}
	r.idleTimeoutDeadline = time.Time{}
}
