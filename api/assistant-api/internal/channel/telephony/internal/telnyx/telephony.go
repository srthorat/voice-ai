// Copyright (c) 2023-2025 RapidaAI
// Author: RapidaAI Team <team@rapida.ai>
//
// Licensed under GPL-2.0 with Rapida Additional Terms.
// See LICENSE.md or contact sales@rapida.ai for commercial usage.

package internal_telnyx_telephony

import (
	"bytes"
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
	// Parse query parameters
	queryParams := make(map[string]string)
	for key, values := range c.Request.URL.Query() {
		if len(values) > 0 {
			queryParams[key] = values[0]
		}
	}

	// Telnyx sends from/to in query params or in the webhook payload
	clientNumber := queryParams["from"]
	if clientNumber == "" {
		clientNumber = queryParams["caller_id"]
	}
	// bodyCallControlID holds the call_control_id found in the JSON body (if any).
	var bodyCallControlID string
	if clientNumber == "" {
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
					if payloadData, ok := data["payload"].(map[string]interface{}); ok {
						if from, ok := payloadData["from"].(string); ok {
							clientNumber = from
						}
						if ccid, ok := payloadData["call_control_id"].(string); ok {
							bodyCallControlID = ccid
						}
					}
				}
			}
		}
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
		"connection_id": connectionID,
		"to":            toPhone,
		"from":          fromPhone,
		"stream_url":    streamURL,
		"stream_track":  "both_tracks", // Stream both inbound and outbound audio
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

// InboundCall instructs Telnyx to answer the inbound call and connect to our WebSocket.
// Returns JSON response for Telnyx to execute streaming.start command.
func (tpc *telnyxTelephony) InboundCall(c *gin.Context, auth types.SimplePrinciple, assistantId uint64, clientNumber string, assistantConversationId uint64) error {
	contextID, _ := c.Get("contextId")
	ctxID := fmt.Sprintf("%v", contextID)

	// Return JSON to tell Telnyx to start streaming
	c.JSON(http.StatusOK, gin.H{
		"result": "streaming.start",
		"params": gin.H{
			"stream_url": fmt.Sprintf("wss://%s/%s",
				tpc.appCfg.PublicAssistantHost,
				internal_type.GetContextAnswerPath("telnyx", ctxID)),
			"stream_track": "both_tracks",
		},
	})

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
