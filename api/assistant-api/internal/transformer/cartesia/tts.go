// Copyright (c) 2023-2025 RapidaAI
// Author: Prashant Srivastav <prashant@rapida.ai>
//
// Licensed under GPL-2.0 with Rapida Additional Terms.
// See LICENSE.md or contact sales@rapida.ai for commercial usage.

package internal_transformer_cartesia

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	cartesia_internal "github.com/rapidaai/api/assistant-api/internal/transformer/cartesia/internal"
	internal_type "github.com/rapidaai/api/assistant-api/internal/type"
	"github.com/rapidaai/pkg/commons"
	type_enums "github.com/rapidaai/pkg/types/enums"
	"github.com/rapidaai/pkg/utils"
	"github.com/rapidaai/protos"
)

type cartesiaTTS struct {
	*cartesiaOption
	mu        sync.Mutex
	ctx       context.Context
	ctxCancel context.CancelFunc

	contextId      string
	ttsConnectedAt time.Time

	ttsStartedAt  time.Time
	ttsMetricSent bool

	logger     commons.Logger
	connection *websocket.Conn
	onPacket   func(pkt ...internal_type.Packet) error
}

func NewCartesiaTextToSpeech(ctx context.Context, logger commons.Logger, credential *protos.VaultCredential,
	onPacket func(pkt ...internal_type.Packet) error,
	opts utils.Option) (internal_type.TextToSpeechTransformer, error) {
	cartesiaOpts, err := NewCartesiaOption(logger, credential, opts)
	if err != nil {
		logger.Errorf("intializing cartesia failed %+v", err)
		return nil, err
	}

	ct, ctxCancel := context.WithCancel(ctx)
	return &cartesiaTTS{
		cartesiaOption: cartesiaOpts,
		logger:         logger,
		ctx:            ct,
		ctxCancel:      ctxCancel,
		onPacket:       onPacket,
	}, nil
}

// Initialize opens a fresh WebSocket connection to Cartesia and starts the
// read goroutine. Called at session start and after each interruption so the
// connection is warm before the first text delta arrives.
func (ct *cartesiaTTS) Initialize() error {
	start := time.Now()
	conn, _, err := websocket.DefaultDialer.Dial(ct.GetTextToSpeechConnectionString(), nil)
	if err != nil {
		ct.logger.Errorf("cartesia-tts: unable to dial %v", err)
		return err
	}

	ct.mu.Lock()
	ct.connection = conn
	if ct.ttsConnectedAt.IsZero() {
		ct.ttsConnectedAt = time.Now()
	}
	ct.mu.Unlock()

	go ct.readLoop(conn)
	ct.onPacket(internal_type.ConversationEventPacket{
		Name: "tts",
		Data: map[string]string{
			"type":     "initialized",
			"provider": ct.Name(),
			"init_ms":  fmt.Sprintf("%d", time.Since(start).Milliseconds()),
		},
		Time: time.Now(),
	})
	return nil
}

// Name returns the name of this transformer.
func (*cartesiaTTS) Name() string {
	return "cartesia-text-to-speech"
}

// handleFlushComplete is called when Cartesia signals done. It emits
// TextToSpeechEndPacket — correctly ordered after the last audio chunk — and
// closes the per-turn connection.
func (cst *cartesiaTTS) handleFlushComplete(conn *websocket.Conn) {
	cst.mu.Lock()
	ctxId := cst.contextId
	cst.connection = nil // mark before Close so readLoop error handler sees intentional
	cst.mu.Unlock()

	cst.onPacket(
		internal_type.TextToSpeechEndPacket{ContextID: ctxId},
		internal_type.ConversationEventPacket{
			Name: "tts",
			Data: map[string]string{"type": "completed"},
			Time: time.Now(),
		},
	)
	conn.Close()
}

// readLoop owns a single WebSocket connection for the duration of one TTS turn.
// It exits when the connection closes — intentionally (interrupt / flush complete)
// or unexpectedly (network drop).
func (cst *cartesiaTTS) readLoop(conn *websocket.Conn) {
	for {
		select {
		case <-cst.ctx.Done():
			return
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			cst.mu.Lock()
			intentional := cst.connection == nil // set to nil before conn.Close() on intentional paths
			if !intentional {
				cst.connection = nil // unintentional drop: next delta will reconnect
			}
			cst.mu.Unlock()
			if !intentional {
				cst.logger.Errorf("cartesia-tts: connection lost: %v", err)
			}
			return
		}

		var payload cartesia_internal.TextToSpeechOuput
		if err := json.Unmarshal(msg, &payload); err != nil {
			cst.logger.Errorf("cartesia-tts: invalid json from cartesia error : %v", err)
			continue
		}

		if payload.Done {
			cst.handleFlushComplete(conn)
			return
		}

		if payload.Data == "" {
			continue
		}

		decoded, err := base64.StdEncoding.DecodeString(payload.Data)
		if err != nil {
			cst.logger.Errorf("cartesia-tts: failed to decode audio payload error: %v", err)
			continue
		}

		cst.mu.Lock()
		startedAt := cst.ttsStartedAt
		metricSent := cst.ttsMetricSent
		ctxId := cst.contextId
		if !metricSent && !startedAt.IsZero() {
			cst.ttsMetricSent = true
		}
		cst.mu.Unlock()

		if !metricSent && !startedAt.IsZero() {
			_ = cst.onPacket(internal_type.AssistantMessageMetricPacket{
				ContextID: ctxId,
				Metrics: []*protos.Metric{{
					Name:  "tts_latency_ms",
					Value: fmt.Sprintf("%d", time.Since(startedAt).Milliseconds()),
				}},
			})
		}
		_ = cst.onPacket(internal_type.TextToSpeechAudioPacket{ContextID: ctxId, AudioChunk: decoded})
	}
}

func (ct *cartesiaTTS) Transform(ctx context.Context, in internal_type.LLMPacket) error {
	ct.mu.Lock()
	if in.ContextId() != ct.contextId {
		ct.contextId = in.ContextId()
		ct.ttsStartedAt = time.Time{}
		ct.ttsMetricSent = false
	}
	connection := ct.connection
	ct.mu.Unlock()

	switch input := in.(type) {
	case internal_type.InterruptionDetectedPacket:
		ct.mu.Lock()
		ct.contextId = ""
		ct.ttsStartedAt = time.Time{}
		ct.ttsMetricSent = false
		conn := ct.connection
		ct.connection = nil
		ct.mu.Unlock()
		if conn != nil {
			conn.Close()
		}
		ct.onPacket(internal_type.ConversationEventPacket{
			Name: "tts",
			Data: map[string]string{"type": "interrupted"},
			Time: time.Now(),
		})
		if err := ct.Initialize(); err != nil {
			ct.logger.Errorf("cartesia-tts: reconnect after interrupt failed: %v", err)
		}
		return nil

	case internal_type.LLMResponseDeltaPacket:
		// Fallback reconnect: handles Initialize() failure or an unintentional drop.
		if connection == nil {
			if err := ct.Initialize(); err != nil {
				return fmt.Errorf("cartesia-tts: failed to connect: %w", err)
			}
			ct.mu.Lock()
			connection = ct.connection
			if ct.ttsStartedAt.IsZero() {
				ct.ttsStartedAt = time.Now()
			}
			ct.mu.Unlock()
		} else {
			ct.mu.Lock()
			if ct.ttsStartedAt.IsZero() {
				ct.ttsStartedAt = time.Now()
			}
			ct.mu.Unlock()
		}
		ct.mu.Lock()
		ctxId := ct.contextId
		ct.mu.Unlock()
		message := ct.GetTextToSpeechInput(input.Text, map[string]interface{}{"continue": true, "context_id": ctxId, "max_buffer_delay_ms": "0ms"})
		if err := connection.WriteJSON(message); err != nil {
			return err
		}
		ct.onPacket(internal_type.ConversationEventPacket{
			Name: "tts",
			Data: map[string]string{
				"type": "speaking",
				"text": input.Text,
			},
			Time: time.Now(),
		})

	case internal_type.LLMResponseDonePacket:
		// Interrupted before done arrived — nothing to flush.
		if connection == nil {
			return nil
		}
		ct.mu.Lock()
		ctxId := ct.contextId
		ct.mu.Unlock()
		// Signal end of text stream; Cartesia will respond with done:true.
		message := ct.GetTextToSpeechInput("", map[string]interface{}{"continue": false, "flush": true, "context_id": ctxId})
		if err := connection.WriteJSON(message); err != nil {
			return err
		}
		// TextToSpeechEndPacket is emitted by handleFlushComplete once done received.

	case internal_type.TurnChangePacket:
		// Context rotated (new turn) — update tracked context ID so the next
		// LLMResponseDeltaPacket picks up the correct Cartesia context_id.
		ct.mu.Lock()
		ct.contextId = input.ContextID
		ct.ttsStartedAt = time.Time{}
		ct.ttsMetricSent = false
		ct.mu.Unlock()
		return nil

	default:
		return fmt.Errorf("cartesia-tts: unsupported input type %T", in)
	}
	return nil
}

func (ct *cartesiaTTS) Close(ctx context.Context) error {
	ct.ctxCancel()
	ct.mu.Lock()
	ctxID := ct.contextId
	connectedAt := ct.ttsConnectedAt
	ct.ttsConnectedAt = time.Time{}

	if ct.connection != nil {
		conn := ct.connection
		ct.connection = nil // mark before Close so readLoop sees intentional
		_ = conn.Close()
	}
	ct.mu.Unlock()

	if !connectedAt.IsZero() {
		ct.onPacket(
			internal_type.ConversationEventPacket{
				ContextID: ctxID,
				Name:      "tts",
				Data: map[string]string{
					"type":     "closed",
					"provider": ct.Name(),
				},
				Time: time.Now(),
			},
			internal_type.ConversationMetricPacket{
				ContextID: 0,
				Metrics: []*protos.Metric{{
					Name:        type_enums.CONVERSATION_TTS_DURATION.String(),
					Value:       fmt.Sprintf("%d", time.Since(connectedAt).Nanoseconds()),
					Description: "Total TTS connection duration in nanoseconds",
				}},
			},
		)
	}
	return nil
}
