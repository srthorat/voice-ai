// Copyright (c) 2023-2025 RapidaAI
// Author: RapidaAI Team <team@rapida.ai>
//
// Licensed under GPL-2.0 with Rapida Additional Terms.
// See LICENSE.md or contact sales@rapida.ai for commercial usage.

package internal_telnyx_telephony

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rapidaai/api/assistant-api/config"
	internal_type "github.com/rapidaai/api/assistant-api/internal/type"
	"github.com/rapidaai/pkg/commons"
	"github.com/rapidaai/pkg/types"
	"github.com/rapidaai/pkg/utils"
	"github.com/rapidaai/protos"
)

const telnyxProvider = "telnyx"

const (
	// Telnyx API base URL
	telnyxAPIBaseURL = "https://api.telnyx.com/v2"
)

type telnyxTelephony struct {
	appCfg *config.AssistantConfig
	logger commons.Logger
}

func NewTelnyxTelephony(config *config.AssistantConfig, logger commons.Logger) (internal_type.Telephony, error) {
	return &telnyxTelephony{
		appCfg: config,
		logger: logger,
	}, nil
}

// CatchAllStatusCallback handles catch-all event callbacks.
func (tpc *telnyxTelephony) CatchAllStatusCallback(ctx *gin.Context) (*internal_type.StatusInfo, error) {
	return nil, nil
}

// StatusCallback handles a status/event callback for a conversation.
// Telnyx sends webhooks for call events like call.answered, call.hangup, etc.
func (tpc *telnyxTelephony) StatusCallback(c *gin.Context, auth types.SimplePrinciple, assistantId uint64, assistantConversationId uint64) (*internal_type.StatusInfo, error) {
	body, err := c.GetRawData()
	if err != nil {
		tpc.logger.Errorf("failed to read request body with error %+v", err)
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		tpc.logger.Errorf("failed to parse request body: %+v", err)
		return nil, fmt.Errorf("failed to parse request body: %w", err)
	}

	// Telnyx webhook format: {"data": {"event_type": "call.answered", ...}}
	data, ok := payload["data"].(map[string]interface{})
	if !ok {
		tpc.logger.Errorf("data field not found or invalid in payload")
		return nil, fmt.Errorf("data field not found in payload")
	}

	eventType, ok := data["event_type"].(string)
	if !ok {
		tpc.logger.Errorf("event_type not found or invalid in payload")
		return nil, fmt.Errorf("event_type not found in payload")
	}

	tpc.logger.Debugf("event processed | event_type: %s, payload: %+v", eventType, payload)
	return &internal_type.StatusInfo{Event: eventType, Payload: payload}, nil
}

// ReceiveCall processes an incoming call webhook and returns structured call info.
// Telnyx sends a webhook with call.answered event when an inbound call is received.
func (tpc *telnyxTelephony) ReceiveCall(c *gin.Context) (*internal_type.CallInfo, error) {
	tpc.logger.Debugf("Telnyx ReceiveCall called")
	// Parse query parameters
	queryParams := make(map[string]string)
	for key, values := range c.Request.URL.Query() {
		if len(values) > 0 {
			queryParams[key] = values[0]
		}
	}
	tpc.logger.Debugf("Telnyx query params: %+v", queryParams)

	// Telnyx sends from/to in query params or in the webhook payload
	clientNumber := queryParams["from"]
	if clientNumber == "" {
		clientNumber = queryParams["caller_id"]
	}
	// bodyCallControlID holds the call_control_id found in the JSON body (if any).
	var bodyCallControlID string
	var eventType string

	// Read body once and restore it immediately so downstream handlers (InboundCall)
	// can still access Request.Body on the same *gin.Context.
	body, err := c.GetRawData()
	if err != nil {
		tpc.logger.Warnf("failed to read request body for caller number: %v", err)
	} else {
		// Restore for downstream handlers.
		c.Request.Body = io.NopCloser(bytes.NewReader(body))

		var payload map[string]interface{}
		if err := json.Unmarshal(body, &payload); err == nil {
			if data, ok := payload["data"].(map[string]interface{}); ok {
				eventType, _ = data["event_type"].(string)
				// Log for debugging
				tpc.logger.Debugf("Telnyx webhook data: %+v", data)
				// Extract from and call_control_id directly from data object, not nested payload
				if from, ok := data["from"].(string); ok && clientNumber == "" {
					clientNumber = from
					tpc.logger.Debugf("Extracted from from data: %s", from)
				}
				if ccid, ok := data["call_control_id"].(string); ok {
					bodyCallControlID = ccid
					tpc.logger.Debugf("Extracted call_control_id from data: %s", ccid)
				}
				// Fallback to payload.nested fields for backward compatibility
				if payloadData, ok := data["payload"].(map[string]interface{}); ok {
					if from, ok := payloadData["from"].(string); ok && clientNumber == "" {
						clientNumber = from
						tpc.logger.Debugf("Extracted from from nested payload: %s", from)
					}
					if ccid, ok := payloadData["call_control_id"].(string); ok && bodyCallControlID == "" {
						bodyCallControlID = ccid
						tpc.logger.Debugf("Extracted call_control_id from nested payload: %s", ccid)
					}
				}
			} else {
				// v1 format: event_type sits at the top level (no 'data' wrapper).
				// e.g. {"event_type": "call_answered", "payload": {...}}
				if topType, ok := payload["event_type"].(string); ok {
					eventType = topType
				}
				if topPayload, ok := payload["payload"].(map[string]interface{}); ok {
					if from, ok := topPayload["from"].(string); ok && clientNumber == "" {
						clientNumber = from
					}
					if ccid, ok := topPayload["call_control_id"].(string); ok && bodyCallControlID == "" {
						bodyCallControlID = ccid
					}
				}
			}
		} else {
			tpc.logger.Debugf("Failed to unmarshal Telnyx webhook body: %v", err)
		}
	}

	// Only process call-initiation events — ignore answered, hangup, streaming_started, etc.
	// Telnyx v2 uses dot notation ("call.initiated"); v1 uses underscores ("call_initiated").
	allowedEvents := map[string]bool{
		"call.initiated": true,
		"call_initiated": true,
	}
	if eventType != "" && !allowedEvents[eventType] {
		tpc.logger.Debugf("ReceiveCall: ignoring non-initiation event %s", eventType)
		// Acknowledge the webhook so Telnyx does not retry indefinitely.
		c.JSON(http.StatusOK, gin.H{"received": true})
		return nil, nil
	}

	if clientNumber == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing caller number"})
		return nil, fmt.Errorf("missing or empty 'from' query parameter")
	}


	info := &internal_type.CallInfo{
		CallerNumber: clientNumber,
		Provider:     telnyxProvider,
		Status:       "SUCCESS",
		StatusInfo:   internal_type.StatusInfo{Event: "webhook", Payload: queryParams},
		Extra:        make(map[string]string),
	}

	// Prefer call_control_id from query params; fall back to the value from the JSON body.
	if v, ok := queryParams["call_control_id"]; ok && v != "" {
		info.ChannelUUID = v
		info.Extra["call_control_id"] = v
	} else if bodyCallControlID != "" {
		info.ChannelUUID = bodyCallControlID
		info.Extra["call_control_id"] = bodyCallControlID
	}

	return info, nil
}

// OutboundCall places an outbound call using Telnyx Call Control API.
// POST to /v2/calls with connection_id, to, from, and stream_url.
func (tpc *telnyxTelephony) OutboundCall(
	auth types.SimplePrinciple,
	toPhone string,
	fromPhone string,
	assistantId, assistantConversationId uint64,
	vaultCredential *protos.VaultCredential,
	opts utils.Option,
) (*internal_type.CallInfo, error) {
	info := &internal_type.CallInfo{Provider: telnyxProvider}

	// Get credentials from vault
	apiKey, connectionID, err := tpc.getCredentials(vaultCredential)
	if err != nil {
		info.Status = "FAILED"
		info.ErrorMessage = fmt.Sprintf("authentication error: %s", err.Error())
		return info, err
	}

	contextID, _ := opts.GetString("rapida.context_id")

	// Build the WebSocket stream URL for bidirectional audio
	streamURL := fmt.Sprintf("wss://%s/%s",
		tpc.appCfg.PublicAssistantHost,
		internal_type.GetContextAnswerPath(telnyxProvider, contextID))

	// Build the request body for Telnyx Call Control API
	callRequest := map[string]interface{}{
		"connection_id":              connectionID,
		"to":                         toPhone,
		"from":                       fromPhone,
		"stream_url":                 streamURL,
		"stream_track":               "both_tracks",
		"stream_bidirectional_mode":  "rtp",
		"stream_bidirectional_codec": "PCMU",
	}

	requestBody, err := json.Marshal(callRequest)
	if err != nil {
		info.Status = "FAILED"
		info.ErrorMessage = fmt.Sprintf("failed to marshal request: %s", err.Error())
		return info, err
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", telnyxAPIBaseURL+"/calls", bytes.NewReader(requestBody))
	if err != nil {
		info.Status = "FAILED"
		info.ErrorMessage = fmt.Sprintf("failed to create request: %s", err.Error())
		return info, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	// Send the request with timeout
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		info.Status = "FAILED"
		info.ErrorMessage = fmt.Sprintf("API error: %s", err.Error())
		return info, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		info.Status = "FAILED"
		info.ErrorMessage = fmt.Sprintf("failed to read response: %s", err.Error())
		return info, err
	}

	// Parse the response
	var callResponse map[string]interface{}
	if err := json.Unmarshal(respBody, &callResponse); err != nil {
		info.Status = "FAILED"
		info.ErrorMessage = fmt.Sprintf("failed to parse response: %s", err.Error())
		return info, err
	}

	// Check for errors
	if resp.StatusCode >= 400 {
		errMsg := "unknown error"
		if errors, ok := callResponse["errors"].([]interface{}); ok && len(errors) > 0 {
			if errMap, ok := errors[0].(map[string]interface{}); ok {
				if detail, ok := errMap["detail"].(string); ok {
					errMsg = detail
				}
			}
		}
		info.Status = "FAILED"
		info.ErrorMessage = fmt.Sprintf("API error: %s", errMsg)
		return info, fmt.Errorf("API error: %s", errMsg)
	}

	// Extract call information from response
	var callData map[string]interface{}
	if data, ok := callResponse["data"].(map[string]interface{}); ok {
		callData = data
	} else {
		callData = callResponse
	}

	// Get call_control_id
	callControlID := ""
	if id, ok := callData["call_control_id"].(string); ok {
		callControlID = id
	} else if id, ok := callData["id"].(string); ok {
		callControlID = id
	}

	info.ChannelUUID = callControlID
	info.Status = "SUCCESS"

	// Extract call session ID if present
	callSessionID := ""
	if sid, ok := callData["call_session_id"].(string); ok {
		callSessionID = sid
	}

	info.Extra = map[string]string{
		"call_control_id": callControlID,
		"call_session_id": callSessionID,
	}

	// Determine event name
	eventName := "initiated"
	if callStatus, ok := callData["status"].(string); ok {
		eventName = callStatus
	}

	info.StatusInfo = internal_type.StatusInfo{Event: eventName, Payload: callResponse}

	return info, nil
}

// InboundCall instructs Telnyx to answer and start streaming via Call Control API.
// For Call Control apps the HTTP response body is ignored; we must:
// 1. POST /v2/calls/{call_control_id}/actions/answer   — to pick up the call
// 2. POST /v2/calls/{call_control_id}/actions/streaming_start — to open the WebSocket
// We acknowledge the webhook immediately (200) and do the API calls asynchronously
// so Telnyx doesn't time out waiting for our response.
func (tpc *telnyxTelephony) InboundCall(c *gin.Context, auth types.SimplePrinciple, assistantId uint64, clientNumber string, assistantConversationId uint64) error {
	contextID, _ := c.Get("contextId")
	ctxID := fmt.Sprintf("%v", contextID)

	streamURL := fmt.Sprintf("wss://%s/%s",
		tpc.appCfg.PublicAssistantHost,
		internal_type.GetContextAnswerPath("telnyx", ctxID))

	ccID := ""
	if val, exists := c.Get("call_control_id"); exists {
		ccID = fmt.Sprintf("%v", val)
	}

	// Fallback: parse call_control_id directly from the request body.
	// ReceiveCall already restores the body via io.NopCloser so we can read it again.
	// This handles cases where the pipeline's context-set path is skipped (empty ChannelUUID).
	if ccID == "" || ccID == "<nil>" {
		if rawBody, err := c.GetRawData(); err == nil && len(rawBody) > 0 {
			c.Request.Body = io.NopCloser(bytes.NewReader(rawBody))
			var wbPayload map[string]interface{}
			if json.Unmarshal(rawBody, &wbPayload) == nil {
				// v2/v3 format: {"data": {"payload": {"call_control_id": "..."}}}
				if d, ok := wbPayload["data"].(map[string]interface{}); ok {
					if p, ok := d["payload"].(map[string]interface{}); ok {
						if id, ok := p["call_control_id"].(string); ok {
							ccID = id
						}
					}
					// also try top-level data.call_control_id
					if ccID == "" {
						if id, ok := d["call_control_id"].(string); ok {
							ccID = id
						}
					}
				}
				// v1 format: {"payload": {"call_control_id": "..."}}
				if ccID == "" {
					if p, ok := wbPayload["payload"].(map[string]interface{}); ok {
						if id, ok := p["call_control_id"].(string); ok {
							ccID = id
						}
					}
				}
			}
			tpc.logger.Debugf("InboundCall: extracted call_control_id from body: %s", ccID)
		}
	}

	vaultCredVal, _ := c.Get("vault_credential")
	vc, hasVault := vaultCredVal.(*protos.VaultCredential)

	// Acknowledge webhook immediately — Telnyx will drop the call if we take >10s.
	c.JSON(http.StatusOK, gin.H{"received": true})

	if ccID == "" || ccID == "<nil>" {
		tpc.logger.Warnf("InboundCall: call_control_id not found in context or body — cannot answer or stream")
		return nil
	}
	if !hasVault {
		tpc.logger.Warnf("InboundCall: vault_credential not in context — cannot answer or stream")
		return nil
	}

	apiKey, _, err := tpc.getCredentials(vc)
	if err != nil {
		tpc.logger.Warnf("InboundCall: failed to get credentials: %v", err)
		return nil
	}

	// Perform answer + streaming_start in a goroutine so we don't block the webhook handler.
	go func() {
		client := &http.Client{Timeout: 10 * time.Second}
		base := fmt.Sprintf("%s/calls/%s/actions", telnyxAPIBaseURL, ccID)

		// Step 1: answer the call
		answerBody, _ := json.Marshal(map[string]interface{}{})
		req, err := http.NewRequest("POST", base+"/answer", bytes.NewReader(answerBody))
		if err != nil {
			tpc.logger.Warnf("InboundCall: failed to build answer request: %v", err)
			return
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			tpc.logger.Warnf("InboundCall: answer API call failed: %v", err)
			return
		}
		resp.Body.Close()
		tpc.logger.Infof("InboundCall: answer sent | call_control_id=%s status=%d", ccID, resp.StatusCode)

		if resp.StatusCode >= 300 {
			tpc.logger.Warnf("InboundCall: answer returned non-2xx (%d), skipping streaming_start", resp.StatusCode)
			return
		}

		// Wait for the call to be fully answered on Telnyx side before starting the stream.
		time.Sleep(500 * time.Millisecond)


		// Step 2: start streaming
		streamBody, _ := json.Marshal(map[string]interface{}{
			"stream_url":                 streamURL,
			"stream_track":               "both_tracks",
			"stream_bidirectional_mode":  "rtp",
			"stream_bidirectional_codec": "PCMU",
		})




		req2, err := http.NewRequest("POST", base+"/streaming_start", bytes.NewReader(streamBody))
		if err != nil {
			tpc.logger.Warnf("InboundCall: failed to build streaming_start request: %v", err)
			return
		}
		req2.Header.Set("Authorization", "Bearer "+apiKey)
		req2.Header.Set("Content-Type", "application/json")
		resp2, err := client.Do(req2)
		if err != nil {
			tpc.logger.Warnf("InboundCall: streaming_start API call failed: %v", err)
			return
		}
		resp2.Body.Close()
		tpc.logger.Infof("InboundCall: streaming_start sent | call_control_id=%s stream_url=%s status=%d",
			ccID, streamURL, resp2.StatusCode)
	}()

	return nil
}



// Auth extracts the API key from vault credential.
func (tpc *telnyxTelephony) Auth(vaultCredential *protos.VaultCredential) (string, error) {
	apiKey, _, err := tpc.getCredentials(vaultCredential)
	return apiKey, err
}

// getCredentials extracts API key and connection ID from vault credential.
func (tpc *telnyxTelephony) getCredentials(vaultCredential *protos.VaultCredential) (string, string, error) {
	if vaultCredential == nil {
		return "", "", fmt.Errorf("vault credential is nil")
	}

	credMap := vaultCredential.GetValue().AsMap()

	apiKey, ok := credMap["api_key"]
	if !ok {
		return "", "", fmt.Errorf("api_key not found in vault credential")
	}

	connectionID, ok := credMap["connection_id"]
	if !ok {
		return "", "", fmt.Errorf("connection_id not found in vault credential")
	}

	return fmt.Sprintf("%v", apiKey), fmt.Sprintf("%v", connectionID), nil
}

// HangupCall hangs up a call using Telnyx Call Control API.
// Transfer moves the call to a new destination.
func (tpc *telnyxTelephony) Transfer(ctx context.Context, conversationID string, to string, vaultCredential *protos.VaultCredential) error {
	apiKey, _, err := tpc.getCredentials(vaultCredential)
	if err != nil {
		return err
	}

	tpc.logger.Infof("Transfer: transferring call %s to %s", conversationID, to)
	base := tpc.getBaseURL(conversationID)
	client := &http.Client{Timeout: 10 * time.Second}

	body, _ := json.Marshal(map[string]interface{}{
		"to": to,
	})

	req, err := http.NewRequestWithContext(ctx, "POST", base+"/transfer", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("telnyx transfer failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (tpc *telnyxTelephony) HangupCall(callControlID string, vaultCredential *protos.VaultCredential) error {
	apiKey, _, err := tpc.getCredentials(vaultCredential)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST",
		fmt.Sprintf("%s/calls/%s/actions/hangup", telnyxAPIBaseURL, url.PathEscape(callControlID)),
		nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("hangup failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
 
 func (tpc *telnyxTelephony) getBaseURL(callControlID string) string {
 	return "https://api.telnyx.com/v2/calls/" + callControlID + "/actions"
 }

