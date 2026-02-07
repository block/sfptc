// Package gitclone provides reusable git clone management with lifecycle control,
// concurrency management, and large repository optimizations.
package gitclone

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"
)

func gitCommand(ctx context.Context, url string, credentialProvider CredentialProvider, args ...string) (*exec.Cmd, error) {
	modifiedURL := url
	if credentialProvider != nil && strings.Contains(url, "github.com") {
		token, err := credentialProvider.GetTokenForURL(ctx, url)
		if err == nil && token != "" {
			modifiedURL = injectTokenIntoURL(url, token)
		}
		// If error getting token, fall back to original URL (system credentials)
	}

	configArgs, err := getInsteadOfDisableArgsForURL(ctx, url)
	if err != nil {
		return nil, errors.Wrap(err, "get insteadOf disable args")
	}

	var allArgs []string
	if len(configArgs) > 0 {
		allArgs = append(allArgs, configArgs...)
	}
	allArgs = append(allArgs, args...)

	// Replace URL in args if it was modified for authentication
	if modifiedURL != url {
		for i, arg := range allArgs {
			if arg == url {
				allArgs[i] = modifiedURL
			}
		}
	}

	return exec.CommandContext(ctx, "git", allArgs...), nil
}

// Converts https://github.com/org/repo to https://x-access-token:TOKEN@github.com/org/repo
func injectTokenIntoURL(url, token string) string {
	if token == "" {
		return url
	}

	if strings.HasPrefix(url, "https://github.com/") {
		return strings.Replace(url, "https://github.com/", fmt.Sprintf("https://x-access-token:%s@github.com/", token), 1)
	}

	// Upgrade http to https when adding token for security
	if strings.HasPrefix(url, "http://github.com/") {
		return strings.Replace(url, "http://github.com/", fmt.Sprintf("https://x-access-token:%s@github.com/", token), 1)
	}

	return url
}

func getInsteadOfDisableArgsForURL(ctx context.Context, targetURL string) ([]string, error) {
	if targetURL == "" {
		return nil, nil
	}

	cmd := exec.CommandContext(ctx, "git", "config", "--get-regexp", "^url\\..*\\.(insteadof|pushinsteadof)$")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return []string{}, nil //nolint:nilerr
	}

	var args []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			configKey := parts[0]
			pattern := parts[1]

			if strings.HasPrefix(targetURL, pattern) {
				args = append(args, "-c", configKey+"=")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "scan insteadOf output")
	}

	return args, nil
}

func ParseGitRefs(output []byte) map[string]string {
	refs := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			sha := parts[0]
			ref := parts[1]
			refs[ref] = sha
		}
	}
	return refs
}
