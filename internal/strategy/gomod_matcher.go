package strategy

import (
	"path"
	"strings"
)

type modulePathMatcher struct {
	patterns []string
}

func newModulePathMatcher(patterns []string) *modulePathMatcher {
	return &modulePathMatcher{patterns: patterns}
}

func (m *modulePathMatcher) isPrivate(modulePath string) bool {
	for _, pattern := range m.patterns {
		if matched, _ := path.Match(pattern, modulePath); matched {
			return true
		}

		if strings.HasPrefix(modulePath, pattern+"/") || modulePath == pattern {
			return true
		}
	}

	return false
}
