// Package githubapp provides GitHub App authentication and token management.
package githubapp

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/alecthomas/errors"
)

type Config struct {
	AppID             string `hcl:"app-id" help:"GitHub App ID"`
	PrivateKeyPath    string `hcl:"private-key-path" help:"Path to GitHub App private key (PEM format)"`
	InstallationsJSON string `hcl:"installations-json" help:"JSON string mapping org names to installation IDs"`

	// Populated from InstallationsJSON during Initialize(). Not exposed in HCL.
	Installations map[string]string
}

type TokenCacheConfig struct {
	RefreshBuffer time.Duration // How early to refresh before expiration
	JWTExpiration time.Duration // GitHub allows max 10 minutes
}

func DefaultTokenCacheConfig() TokenCacheConfig {
	return TokenCacheConfig{
		RefreshBuffer: 5 * time.Minute,
		JWTExpiration: 10 * time.Minute,
	}
}

// Initialize must be called after loading config to parse InstallationsJSON into Installations map.
func (c *Config) Initialize(logger *slog.Logger) error {
	if c.InstallationsJSON == "" {
		return errors.New("installations-json is required")
	}

	var installations map[string]string
	if err := json.Unmarshal([]byte(c.InstallationsJSON), &installations); err != nil {
		logger.Error("Failed to parse installations-json",
			"error", err,
			"installations_json", c.InstallationsJSON)
		return errors.Wrap(err, "parse installations-json")
	}

	if len(installations) == 0 {
		return errors.New("installations-json must contain at least one organization")
	}

	c.Installations = installations

	logger.Info("GitHub App config initialized",
		"app_id", c.AppID,
		"private_key_path", c.PrivateKeyPath,
		"installations_json", c.InstallationsJSON)

	return nil
}

func (c *Config) IsConfigured() bool {
	return c.AppID != "" && c.PrivateKeyPath != "" && len(c.Installations) > 0
}

func (c *Config) GetInstallationID(org string) string {
	if c.Installations == nil {
		return ""
	}
	return c.Installations[org]
}
