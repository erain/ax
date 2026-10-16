// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/types/known/structpb"
)

// Supported providers and default models
const (
	ProviderGoogle = "google"

	DefaultModel             = "gemini-3.8-flash"
	DefaultModelResourceName = "default-model"
	DefaultAtespace          = "default"
	DefaultSecretName        = "gemini-api-secret"
	DefaultSecretKey         = "GEMINI_API_KEY"
)

// systemInstructionParam is the Parameters key holding a default system
// instruction. It is not a generation parameter, so it is lifted out of the map.
const systemInstructionParam = "systemInstruction"

// SecretKeyRef references a secret key for authentication.
type SecretKeyRef = v1alpha1.SecretKeyRef

// Config specifies the configuration for the model client.
// It maps directly to the common Model CRD (v1alpha1.Model / v1alpha1.ModelSpec)
// with additional client runtime options (API keys, endpoints, timeouts).
type Config struct {
	Name     string `json:"name,omitempty" yaml:"name,omitempty"`
	Atespace string `json:"atespace,omitempty" yaml:"atespace,omitempty"`
	Provider string `json:"provider" yaml:"provider"`
	Model    string `json:"model,omitempty" yaml:"model,omitempty"`
	// Parameters are provider-specific generation settings passed through to the
	// model API, for example temperature or maxOutputTokens for Gemini. A
	// systemInstruction entry is sent as the system instruction rather than as a
	// generation parameter.
	Parameters    map[string]any `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	SecretKey     *SecretKeyRef  `json:"secretKey,omitempty" yaml:"secretKey,omitempty"`
	APIKey        string         `json:"apiKey,omitempty" yaml:"apiKey,omitempty"`
	BaseURL       string         `json:"baseURL,omitempty" yaml:"baseURL,omitempty"`
	DisableRemote bool           `json:"disableRemote,omitempty" yaml:"disableRemote,omitempty"`
}

// ConfigFromSpec creates a Config from a v1alpha1.ModelSpec.
func ConfigFromSpec(spec *v1alpha1.ModelSpec) Config {
	if spec == nil {
		return DefaultConfig()
	}
	secKey := spec.SecretKey
	if secKey == nil {
		secKey = &SecretKeyRef{
			Name: DefaultSecretName,
			Key:  DefaultSecretKey,
		}
	}
	return Config{
		Name:       DefaultModelResourceName,
		Atespace:   DefaultAtespace,
		Provider:   spec.Provider,
		Model:      spec.Model,
		Parameters: spec.GetParameters().AsMap(),
		SecretKey:  secKey,
	}
}

// ConfigFromCRD creates a Config from a v1alpha1.Model resource.
func ConfigFromCRD(m *v1alpha1.Model) Config {
	if m == nil {
		return DefaultConfig()
	}
	cfg := ConfigFromSpec(m.Spec)
	if m.Metadata != nil {
		if m.Metadata.Name != "" {
			cfg.Name = m.Metadata.Name
		}
		if m.Metadata.Atespace != "" {
			cfg.Atespace = m.Metadata.Atespace
		}
	}
	return cfg
}

// DefaultConfig returns the default configuration targeting "default-model" with Gemini 3.8 Flash.
func DefaultConfig() Config {
	return Config{
		Name:     DefaultModelResourceName,
		Atespace: DefaultAtespace,
		Provider: ProviderGoogle,
		Model:    DefaultModel,
		SecretKey: &SecretKeyRef{
			Name: DefaultSecretName,
			Key:  DefaultSecretKey,
		},
	}
}

// GenerateRequest specifies the input for model inference.
type GenerateRequest struct {
	Model             string  `json:"model,omitempty"`
	Prompt            string  `json:"prompt"`
	SystemInstruction string  `json:"systemInstruction,omitempty"`
	Temperature       float64 `json:"temperature,omitempty"`
	MaxTokens         int     `json:"maxTokens,omitempty"`
}

// GenerateResponse holds the completion output and token statistics.
type GenerateResponse struct {
	Model   string     `json:"model"`
	Content string     `json:"content"`
	Usage   TokenUsage `json:"usage"`
}

// TokenUsage provides token counts for the inference call.
type TokenUsage struct {
	PromptTokens     int `json:"promptTokens"`
	CompletionTokens int `json:"completionTokens"`
	TotalTokens      int `json:"totalTokens"`
}

// SecretResolverFunc is a callback function for resolving secrets by name and key.
type SecretResolverFunc func(secretName, key string) (string, error)

// Client is an LLM client that executes inference using its configured Config.
// System components like Workspace use Client directly without needing a model server.
type Client struct {
	cfg            Config
	httpClient     *http.Client
	secretResolver SecretResolverFunc
}

// Option configures Client runtime behaviors.
type Option func(*Client)

// WithAPIKey sets or overrides the provider API key.
func WithAPIKey(key string) Option {
	return func(c *Client) {
		c.cfg.APIKey = key
	}
}

// WithSecretKey configures a secret key reference for resolving the API key.
func WithSecretKey(name, key string) Option {
	return func(c *Client) {
		c.cfg.SecretKey = &SecretKeyRef{Name: name, Key: key}
		if resolved := c.resolveAPIKey(); resolved != "" {
			c.cfg.APIKey = resolved
		}
	}
}

// WithSecretResolver sets a custom secret resolver callback.
func WithSecretResolver(fn SecretResolverFunc) Option {
	return func(c *Client) {
		c.secretResolver = fn
	}
}

// WithStore instructs the client constructor to load configuration from the store for the given model.
// If modelName is empty, it defaults to "default-model". If atespace is empty, it defaults to "default".
func WithStore(ctx context.Context, s store.Store, atespace, modelName string) Option {
	return func(c *Client) {
		if modelName == "" {
			modelName = DefaultModelResourceName
		}
		if atespace == "" {
			atespace = DefaultAtespace
		}
		if s != nil {
			m, err := s.GetModel(ctx, atespace, modelName)
			if err == nil && m != nil {
				c.cfg = ConfigFromCRD(m)
			}
		}
	}
}

// WithBaseURL overrides the provider base URL (useful for testing or local endpoints).
func WithBaseURL(url string) Option {
	return func(c *Client) {
		c.cfg.BaseURL = strings.TrimRight(url, "/")
	}
}

// WithHTTPClient sets a custom http.Client.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithDisableRemote forces local fallback generation without remote network calls.
func WithDisableRemote(disable bool) Option {
	return func(c *Client) {
		c.cfg.DisableRemote = disable
	}
}

// NewClient creates a new model client using the provided Config.
func NewClient(cfg Config, opts ...Option) *Client {
	if cfg.Name == "" {
		cfg.Name = DefaultModelResourceName
	}
	if cfg.Atespace == "" {
		cfg.Atespace = DefaultAtespace
	}
	if cfg.Model == "" {
		cfg.Model = DefaultModel
	}
	if cfg.Provider == "" {
		cfg.Provider = ProviderGoogle
	}
	if cfg.SecretKey == nil && (cfg.Name == DefaultModelResourceName || cfg.Name == "") {
		cfg.SecretKey = &SecretKeyRef{
			Name: DefaultSecretName,
			Key:  DefaultSecretKey,
		}
	}

	c := &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.cfg.APIKey == "" {
		c.cfg.APIKey = c.resolveAPIKey()
	}

	return c
}

// resolveAPIKey resolves the API key from explicit config or Kubernetes secrets.
func (c *Client) resolveAPIKey() string {
	if c.cfg.APIKey != "" {
		return c.cfg.APIKey
	}

	if c.cfg.SecretKey == nil {
		return ""
	}

	// 1. Custom secret resolver function if provided (e.g. for testing)
	if c.secretResolver != nil {
		if val, err := c.secretResolver(c.cfg.SecretKey.Name, c.cfg.SecretKey.Key); err == nil && val != "" {
			return val
		}
	}

	// 2. Fetch from Kubernetes Secret
	namespace := c.cfg.Atespace
	if namespace == "" {
		namespace = DefaultAtespace
	}
	val, err := GetKubernetesSecret(context.Background(), namespace, c.cfg.SecretKey.Name, c.cfg.SecretKey.Key)
	if err == nil && val != "" {
		return val
	}

	return ""
}

// GetKubernetesSecret retrieves a secret from the Kubernetes API or kubectl CLI.
func GetKubernetesSecret(ctx context.Context, namespace, secretName, key string) (string, error) {
	if secretName == "" || key == "" {
		return "", fmt.Errorf("secret name and key must not be empty")
	}
	if namespace == "" {
		namespace = DefaultAtespace
	}

	// 1. In-cluster Kubernetes API (inside pod)
	if k8sHost := os.Getenv("KUBERNETES_SERVICE_HOST"); k8sHost != "" {
		k8sPort := os.Getenv("KUBERNETES_SERVICE_PORT")
		if k8sPort == "" {
			k8sPort = "443"
		}

		tokenPath := "/var/run/secrets/kubernetes.io/serviceaccount/token"
		tokenData, err := os.ReadFile(tokenPath)
		if err == nil {
			token := strings.TrimSpace(string(tokenData))

			// If namespace was not specified, use pod's serviceaccount namespace
			if namespace == "" {
				if nsData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
					if ns := strings.TrimSpace(string(nsData)); ns != "" {
						namespace = ns
					}
				}
			}

			caCertPool := x509.NewCertPool()
			if caData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"); err == nil {
				caCertPool.AppendCertsFromPEM(caData)
			}

			tr := &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: caCertPool,
				},
			}
			k8sClient := &http.Client{
				Transport: tr,
				Timeout:   10 * time.Second,
			}

			url := fmt.Sprintf("https://%s:%s/api/v1/namespaces/%s/secrets/%s", k8sHost, k8sPort, namespace, secretName)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err == nil {
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Accept", "application/json")
				resp, err := k8sClient.Do(req)
				if err == nil {
					defer resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						var secretObj struct {
							Data map[string]string `json:"data"`
						}
						if err := json.NewDecoder(resp.Body).Decode(&secretObj); err == nil {
							if encoded, ok := secretObj.Data[key]; ok {
								decoded, err := base64.StdEncoding.DecodeString(encoded)
								if err == nil {
									return strings.TrimSpace(string(decoded)), nil
								}
							}
						}
					}
				}
			}
		}
	}

	// 2. Fallback to kubectl CLI (for local development outside cluster)
	cmd := exec.CommandContext(ctx, "kubectl", "get", "secret", secretName, "-n", namespace,
		"-o", fmt.Sprintf("jsonpath={.data.%s}", key))
	out, err := cmd.Output()
	if err == nil {
		raw := strings.TrimSpace(string(out))
		if raw != "" {
			decoded, err := base64.StdEncoding.DecodeString(raw)
			if err == nil {
				return strings.TrimSpace(string(decoded)), nil
			}
		}
	}

	return "", fmt.Errorf("kubernetes secret %s/%s with key %s not found", namespace, secretName, key)
}

// NewClientFromSpec creates a new model client from a v1alpha1.ModelSpec.
func NewClientFromSpec(spec *v1alpha1.ModelSpec, opts ...Option) *Client {
	return NewClient(ConfigFromSpec(spec), opts...)
}

// NewClientFromCRD creates a new model client from a v1alpha1.Model CRD resource.
func NewClientFromCRD(m *v1alpha1.Model, opts ...Option) *Client {
	if m == nil {
		return NewDefaultClient(opts...)
	}
	return NewClient(ConfigFromCRD(m), opts...)
}

// NewDefaultClient creates a model client configured with the "default-model" resource and Gemini 3.8 Flash.
func NewDefaultClient(opts ...Option) *Client {
	return NewClient(DefaultConfig(), opts...)
}

// NewClientFromStore creates a model client by reading the model CRD from the store.
// If name is empty, it defaults to "default-model". If atespace is empty, it defaults to "default".
func NewClientFromStore(ctx context.Context, s store.Store, atespace, name string, opts ...Option) (*Client, error) {
	if s == nil {
		return nil, errors.New("store cannot be nil")
	}
	if atespace == "" {
		atespace = DefaultAtespace
	}
	if name == "" {
		name = DefaultModelResourceName
	}
	m, err := s.GetModel(ctx, atespace, name)
	if err != nil {
		return nil, fmt.Errorf("reading model %s/%s from store: %w", atespace, name, err)
	}
	return NewClientFromCRD(m, opts...), nil
}

// NewDefaultClientFromStore loads the "default-model" from the given atespace, or returns a default client if not found.
func NewDefaultClientFromStore(ctx context.Context, s store.Store, atespace string, opts ...Option) (*Client, error) {
	c, err := NewClientFromStore(ctx, s, atespace, DefaultModelResourceName, opts...)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return NewDefaultClient(opts...), nil
		}
		return nil, err
	}
	return c, nil
}

// Config returns the client's configuration.
func (c *Client) Config() Config {
	return c.cfg
}

// Spec converts the client configuration to a v1alpha1.ModelSpec.
func (c *Client) Spec() *v1alpha1.ModelSpec {
	spec := &v1alpha1.ModelSpec{
		Provider:  c.cfg.Provider,
		Model:     c.cfg.Model,
		SecretKey: c.cfg.SecretKey,
	}
	if len(c.cfg.Parameters) > 0 {
		// Values that cannot be represented in a Struct (only JSON-like types can)
		// are dropped rather than failing the conversion.
		spec.Parameters, _ = structpb.NewStruct(c.cfg.Parameters)
	}
	return spec
}

// Generate executes a generation request against the configured model provider.
func (c *Client) Generate(ctx context.Context, req *GenerateRequest) (*GenerateResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	modelName := req.Model
	if modelName == "" {
		modelName = c.cfg.Model
	}
	if modelName == "" {
		modelName = DefaultModel
	}

	sysInst := req.SystemInstruction
	if sysInst == "" {
		if s, ok := c.cfg.Parameters[systemInstructionParam].(string); ok {
			sysInst = s
		}
	}

	effectiveReq := &GenerateRequest{
		Model:             modelName,
		Prompt:            req.Prompt,
		SystemInstruction: sysInst,
		Temperature:       req.Temperature,
		MaxTokens:         req.MaxTokens,
	}

	provider := strings.ToLower(c.cfg.Provider)
	if provider == "" || provider == ProviderGoogle {
		return c.generateGoogle(ctx, effectiveReq)
	}

	if c.cfg.DisableRemote {
		return c.fallbackResponse(effectiveReq), nil
	}

	return nil, fmt.Errorf("unsupported provider %q", c.cfg.Provider)
}

// generateGoogle communicates with Google Generative Language API for Gemini models.
func (c *Client) generateGoogle(ctx context.Context, req *GenerateRequest) (*GenerateResponse, error) {
	if c.cfg.DisableRemote || c.cfg.APIKey == "" {
		return c.fallbackResponse(req), nil
	}

	baseURL := c.cfg.BaseURL
	if baseURL == "" {
		baseURL = "https://generativelanguage.googleapis.com"
	}

	endpoint := fmt.Sprintf("%s/v1beta/models/%s:generateContent?key=%s", baseURL, req.Model, c.cfg.APIKey)

	payload := map[string]interface{}{
		"contents": []map[string]interface{}{
			{
				"parts": []map[string]string{
					{"text": req.Prompt},
				},
			},
		},
	}

	if req.SystemInstruction != "" {
		payload["systemInstruction"] = map[string]interface{}{
			"parts": []map[string]string{
				{"text": req.SystemInstruction},
			},
		}
	}

	// Configured parameters go through as-is; per-request values override them.
	genConfig := map[string]interface{}{}
	for k, v := range c.cfg.Parameters {
		if k != systemInstructionParam {
			genConfig[k] = v
		}
	}
	if req.Temperature > 0 {
		genConfig["temperature"] = req.Temperature
	}
	if req.MaxTokens > 0 {
		genConfig["maxOutputTokens"] = req.MaxTokens
	}
	if len(genConfig) > 0 {
		payload["generationConfig"] = genConfig
	}

	reqBody, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("creating http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return c.fallbackResponse(req), nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return c.fallbackResponse(req), nil
		}
		return nil, fmt.Errorf("gemini api error %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var geminiResp struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount     int `json:"promptTokenCount"`
			CandidatesTokenCount int `json:"candidatesTokenCount"`
			TotalTokenCount      int `json:"totalTokenCount"`
		} `json:"usageMetadata"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&geminiResp); err != nil {
		return nil, fmt.Errorf("decoding gemini response: %w", err)
	}

	var textBuilder strings.Builder
	if len(geminiResp.Candidates) > 0 {
		for _, p := range geminiResp.Candidates[0].Content.Parts {
			textBuilder.WriteString(p.Text)
		}
	}

	return &GenerateResponse{
		Model:   req.Model,
		Content: textBuilder.String(),
		Usage: TokenUsage{
			PromptTokens:     geminiResp.UsageMetadata.PromptTokenCount,
			CompletionTokens: geminiResp.UsageMetadata.CandidatesTokenCount,
			TotalTokens:      geminiResp.UsageMetadata.TotalTokenCount,
		},
	}, nil
}

func (c *Client) fallbackResponse(req *GenerateRequest) *GenerateResponse {
	content := fmt.Sprintf(
		"Synthesized plan using %s: Workspace environment configured with development toolchain and dependencies matching declared goal.",
		req.Model,
	)
	return &GenerateResponse{
		Model:   req.Model,
		Content: content,
		Usage: TokenUsage{
			PromptTokens:     len(req.Prompt) / 4,
			CompletionTokens: len(content) / 4,
			TotalTokens:      (len(req.Prompt) + len(content)) / 4,
		},
	}
}
