// Copyright (c) 2023-2025 RapidaAI
// Author: RapidaAI Team <team@rapida.ai>
//
// Licensed under GPL-2.0 with Rapida Additional Terms.
// See LICENSE.md or contact sales@rapida.ai for commercial usage.

package internal_telnyx_telephony

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"


	"github.com/gorilla/websocket"
	internal_audio "github.com/rapidaai/api/assistant-api/internal/audio"
	callcontext "github.com/rapidaai/api/assistant-api/internal/callcontext"
	internal_telephony_base "github.com/rapidaai/api/assistant-api/internal/channel/telephony/internal/base"
	internal_type "github.com/rapidaai/api/assistant-api/internal/type"
	"github.com/rapidaai/pkg/commons"
	"github.com/rapidaai/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RAPIDA_AUDIO_CONFIG is the internal Rapida audio format (linear16 16kHz).
var RAPIDA_AUDIO_CONFIG = internal_audio.NewLinear16khzMonoAudioConfig()

// PCMU_8K_AUDIO_CONFIG is the Telnyx-native audio format (PCMU 8kHz).
var PCMU_8K_AUDIO_CONFIG = internal_audio.NewMulaw8khzMonoAudioConfig()

// TelnyxWebSocketEvent represents a Telnyx WebSocket event.
type TelnyxWebSocketEvent struct {
	Event    string                 `json:"event"`
	StreamID string                 `json:"stream_id"`
	Start    *TelnyxStartEvent      `json:"start,omitempty"`
	Media    *TelnyxMediaEvent      `json:"media,omitempty"`
	Stop     *TelnyxStopEvent       `json:"stop,omitempty"`
}

// TelnyxStartEvent contains the start event details.
type TelnyxStartEvent struct {
	CallControlID string                    `json:"call_control_id"`
	MediaFormat   TelnyxMediaFormat         `json:"media_format"`
}

// TelnyxMediaFormat describes the audio format.
type TelnyxMediaFormat struct {
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
	Channels   int    `json:"channels"`
}

// TelnyxMediaEvent contains the media event details.
type TelnyxMediaEvent struct {
	Track   string `json:"track"`
	Payload string `json:"payload"`
}

// TelnyxStopEvent contains the stop event details.
type TelnyxStopEvent struct {
	CallControlID string `json:"call_control_id"`
}

type telnyxWebsocketStreamer struct {
	internal_telephony_base.BaseTelephonyStreamer

	streamID      string
	callControlID string
	connection    *websocket.Conn
	mu            sync.RWMutex
	encoder       *base64.Encoding
	telephony     *telnyxTelephony
}

// NewTelnyxWebsocketStreamer creates a Telnyx WebSocket streamer.
// Telnyx sends PCMU 8kHz (µ-law 8kHz) which needs resampling to linear16 16kHz.
func NewTelnyxWebsocketStreamer(logger commons.Logger, connection *websocket.Conn, cc *callcontext.CallContext, vaultCred *protos.VaultCredential) internal_type.Streamer {
	tws := &telnyxWebsocketStreamer{
		BaseTelephonyStreamer: internal_telephony_base.NewBaseTelephonyStreamer(
			logger, cc, vaultCred,
			internal_telephony_base.WithSourceAudioConfig(internal_audio.NewMulaw8khzMonoAudioConfig()),
		),
		streamID:   "",
		connection: connection,
		encoder:    base64.StdEncoding,
		telephony: &telnyxTelephony{
			logger: logger,
		},
	}
	go tws.runWebSocketReader()
	return tws
}

// runWebSocketReader reads messages from the WebSocket connection.
func (tws *telnyxWebsocketStreamer) runWebSocketReader() {
	tws.mu.RLock()
	conn := tws.connection
	tws.mu.RUnlock()

	if conn == nil {
		return
	}

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			tws.Logger.Errorf("WebSocket read error: %v", err)
			tws.PushDisconnection(protos.ConversationDisconnection_DISCONNECTION_TYPE_USER)
			tws.BaseStreamer.Cancel()
			return
		}

		// Telnyx sends JSON text messages (unlike Vonage which sends binary)
		if messageType != websocket.TextMessage {
			tws.Logger.Warnf("Unexpected message type: %d", messageType)
			continue
		}

		var event TelnyxWebSocketEvent
		if err := json.Unmarshal(message, &event); err != nil {
			tws.Logger.Errorf("Failed to unmarshal Telnyx event: %v", err)
			continue
		}

		if event.Event != "media" {
			tws.Logger.Debugf("Telnyx WebSocket event: %s", event.Event)
		}

		switch event.Event {
		case "start":
			tws.handleStartEvent(event)
			tws.PushInput(tws.CreateConnectionRequest())
			tws.PushInputLow(&protos.ConversationEvent{
				Name: "channel",
				Data: map[string]string{
					"type":            "stream_started",
					"provider":        "telnyx",
					"stream_id":       tws.streamID,
					"call_control_id": tws.callControlID,
				},
				Time: timestamppb.Now(),
			})

		case "media":
			msg, err := tws.handleMediaEvent(event)
			if err != nil {
				tws.Logger.Errorf("Failed to handle media event: %v", err)
			}
			if msg != nil {
				tws.PushInput(msg)
			}

		case "stop":
			tws.Logger.Info("Telnyx stream stopped")
			tws.PushDisconnection(protos.ConversationDisconnection_DISCONNECTION_TYPE_USER)
			tws.Cancel()
			return

		case "dtmf":
			// Handle DTMF events if needed
			tws.Logger.Debugf("DTMF event received: %+v", event)

		case "connected":
			tws.Logger.Debug("Telnyx WebSocket connected")
		default:
			tws.Logger.Warnf("Unhandled Telnyx event: %s | Full event: %+v", event.Event, event)
		}


	}
}

// handleStartEvent processes the start event from Telnyx.
func (tws *telnyxWebsocketStreamer) handleStartEvent(event TelnyxWebSocketEvent) {
	if event.Start == nil {
		return
	}

	// Capture locals first so the log call below does not need the lock.
	streamID := event.StreamID
	callControlID := event.Start.CallControlID
	encoding := event.Start.MediaFormat.Encoding
	sampleRate := event.Start.MediaFormat.SampleRate

	tws.mu.Lock()
	tws.streamID = streamID
	tws.callControlID = callControlID
	tws.ChannelUUID = callControlID
	tws.mu.Unlock()

	tws.Logger.Debugf("Telnyx stream started | stream_id: %s, call_control_id: %s, format: %s %dHz",
		streamID, callControlID, encoding, sampleRate)

	// Flush any audio that was buffered before start event arrived.
	// Collect chunks inside the lock, then send after releasing it so
	// the output-buffer lock is not held across network I/O.
	var flushChunks [][]byte
	tws.WithOutputBuffer(func(buf *bytes.Buffer) {
		for buf.Len() >= tws.OutputFrameSize() {
			chunk := make([]byte, tws.OutputFrameSize())
			copy(chunk, buf.Next(tws.OutputFrameSize()))
			flushChunks = append(flushChunks, chunk)
		}
	})
	for _, chunk := range flushChunks {
		if err := tws.sendMedia(chunk); err != nil {
			tws.Logger.Errorf("Failed to flush audio chunk: %v", err)
			break
		}
	}
}


// handleMediaEvent processes incoming media events from Telnyx.
func (tws *telnyxWebsocketStreamer) handleMediaEvent(event TelnyxWebSocketEvent) (*protos.ConversationUserMessage, error) {
	if event.Media == nil {
		return nil, nil
	}

	if event.Media.Track != "" && event.Media.Track != "inbound_track" && event.Media.Track != "inbound" {
		// Ignore outbound audio or other tracks to prevent echo
		return nil, nil
	}

	payload := event.Media.Payload
	if payload == "" {
		// Log the entire event to see if we missed the field name
		tws.Logger.Debugf("Empty payload! Event: %+v", event.Media)
		return nil, nil
	}


	// Decode base64 payload
	payloadBytes, err := tws.encoder.DecodeString(payload)
	if err != nil {
		tws.Logger.Warnf("Failed to decode media payload: %v", err)
		return nil, nil
	}

	var audioRequest *protos.ConversationUserMessage
	tws.WithInputBuffer(func(buf *bytes.Buffer) {
		buf.Write(payloadBytes)
		if buf.Len() >= tws.InputBufferThreshold() {
			audioRequest = tws.CreateVoiceRequest(buf.Bytes())
			buf.Reset()
		}
	})

	return audioRequest, nil
}

// Send sends audio or control messages to Telnyx.
func (tws *telnyxWebsocketStreamer) Send(response internal_type.Stream) error {
	tws.mu.RLock()
	conn := tws.connection
	tws.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("telnyx websocket connection is nil")
	}

	switch data := response.(type) {
	case *protos.ConversationAssistantMessage:
		switch content := data.Message.(type) {
		case *protos.ConversationAssistantMessage_Audio:
			// Resample from internal format (linear16 16kHz) to Telnyx format (PCMU 8kHz)
			audioData, err := tws.Resampler().Resample(content.Audio, RAPIDA_AUDIO_CONFIG, PCMU_8K_AUDIO_CONFIG)
			if err != nil {
				tws.Logger.Warnw("Failed to resample output audio to PCMU 8kHz, forwarding raw bytes",
					"error", err.Error(),
				)
				audioData = content.Audio
			}

			// Collect chunks under the output-buffer lock; send after releasing it
			// so network I/O (and any per-chunk pacing) never holds the lock.
			var chunks [][]byte
			tws.WithOutputBuffer(func(buf *bytes.Buffer) {
				buf.Write(audioData)

				tws.mu.RLock()
				hasStream := tws.streamID != ""
				tws.mu.RUnlock()

				if !hasStream {
					// Buffer the audio until streamID is set by the 'start' event.
					return
				}

				for buf.Len() >= tws.OutputFrameSize() {
					chunk := make([]byte, tws.OutputFrameSize())
					copy(chunk, buf.Next(tws.OutputFrameSize()))
					chunks = append(chunks, chunk)
				}
				// Flush remaining audio when response is marked complete.
				if data.GetCompleted() && buf.Len() > 0 {
					remaining := make([]byte, buf.Len())
					copy(remaining, buf.Bytes())
					chunks = append(chunks, remaining)
					buf.Reset()
				}
			})

			for _, chunk := range chunks {
				if err := tws.sendMedia(chunk); err != nil {
					tws.Logger.Errorf("Failed to send audio chunk: %v", err)
					return err
				}
			}
			return nil
		}

	case *protos.ConversationInterruption:
		if data.Type == protos.ConversationInterruption_INTERRUPTION_TYPE_WORD {
			tws.ResetOutputBuffer()
			if err := tws.sendClear(); err != nil {
				tws.Logger.Errorf("Error sending clear command: %v", err)
			}
		}

	case *protos.ConversationDirective:
		if data.GetType() == protos.ConversationDirective_END_CONVERSATION {
			if tws.callControlID != "" {
				// Use Call Control API to hang up
				if err := tws.telephony.HangupCall(tws.callControlID, tws.VaultCredential()); err != nil {
					tws.Logger.Errorf("Error ending Telnyx call: %v", err)
				}
			}
			if err := tws.Cancel(); err != nil {
				tws.Logger.Errorf("Error disconnecting: %v", err)
			}
		}
	}

	return nil
}

// sendMedia encodes and sends an audio chunk to Telnyx.
func (tws *telnyxWebsocketStreamer) sendMedia(audioData []byte) error {
	tws.mu.RLock()
	conn := tws.connection
	streamID := tws.streamID
	tws.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("telnyx connection is nil")
	}
	if streamID == "" {
		return nil
	}

	const chunkSize = 160 // 20ms of PCMU at 8kHz
	for i := 0; i < len(audioData); i += chunkSize {
		end := i + chunkSize
		if end > len(audioData) {
			end = len(audioData)
		}
		chunk := audioData[i:end]

		message := map[string]interface{}{
			"event":     "media",
			"stream_id": streamID,
			"media": map[string]interface{}{
				"payload": tws.encoder.EncodeToString(chunk),
				"track":   "outbound_track",
			},
		}

		messageJSON, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal media message: %w", err)
		}

		tws.mu.Lock()
		if tws.connection == nil {
			tws.mu.Unlock()
			return fmt.Errorf("connection closed")
		}
		err = tws.connection.WriteMessage(websocket.TextMessage, messageJSON)
		tws.mu.Unlock()

		if err != nil {
			return err
		}

		tws.Logger.Debugf("Sent media chunk: %d bytes | StreamID: %s", len(chunk), streamID)
	}


	return nil
}



func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}



// sendClear sends a clear command to Telnyx to interrupt audio.
func (tws *telnyxWebsocketStreamer) sendClear() error {
	tws.mu.RLock()
	conn := tws.connection
	streamID := tws.streamID
	tws.mu.RUnlock()

	if conn == nil || streamID == "" {
		return nil
	}

	message := map[string]interface{}{
		"event":     "clear",
		"stream_id": streamID,
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal clear message: %w", err)
	}

	tws.mu.Lock()
	defer tws.mu.Unlock()
	// Re-check: Cancel() may have nulled tws.connection between RUnlock and Lock.
	if tws.connection == nil {
		return nil
	}
	return tws.connection.WriteMessage(websocket.TextMessage, messageJSON)
}

// sendDTMF sends DTMF digits to Telnyx.
func (tws *telnyxWebsocketStreamer) sendDTMF(digit string) error {
	tws.mu.RLock()
	conn := tws.connection
	streamID := tws.streamID
	tws.mu.RUnlock()

	if conn == nil || streamID == "" {
		return nil
	}

	message := map[string]interface{}{
		"event":     "dtmf",
		"stream_id": streamID,
		"dtmf": map[string]interface{}{
			"digit": digit,
		},
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal dtmf message: %w", err)
	}

	tws.mu.Lock()
	defer tws.mu.Unlock()
	// Re-check: Cancel() may have nulled tws.connection between RUnlock and Lock.
	if tws.connection == nil {
		return nil
	}
	return tws.connection.WriteMessage(websocket.TextMessage, messageJSON)
}

// GetConversationUuid returns the call control ID.
func (tws *telnyxWebsocketStreamer) GetConversationUuid() string {
	return tws.ChannelUUID
}

// Cancel closes the WebSocket connection.
func (tws *telnyxWebsocketStreamer) Cancel() error {
	tws.mu.Lock()
	conn := tws.connection
	tws.connection = nil
	tws.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
	tws.BaseStreamer.Cancel()
	return nil
}

// NotifyMode is a no-op for telephony providers.
func (tws *telnyxWebsocketStreamer) NotifyMode(mode protos.StreamMode) {
	// No-op for telephony
}
