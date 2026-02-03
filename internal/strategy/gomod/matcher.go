package gomod

import (
	"path"
	"strings"
)

type ModulePathMatcher struct {
	patterns []string
}

func NewModulePathMatcher(patterns []string) *ModulePathMatcher {
	return &ModulePathMatcher{patterns: patterns}
}

func (m *ModulePathMatcher) IsPrivate(modulePath string) bool {
	for _, pattern := range m.patterns {
		matched, err := path.Match(pattern, modulePath)
		if err == nil && matched {
			return true
		}

		if strings.HasPrefix(modulePath, pattern+"/") || modulePath == pattern {
			return true
		}
	}

	return false
}
