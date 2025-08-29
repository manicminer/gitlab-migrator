package main

import (
	"sync"

	"github.com/google/go-github/v74/github"
	"github.com/xanzy/go-gitlab"
)

const (
	githubBranchesCacheType uint8 = iota
	githubPullRequestCacheType
	githubSearchResultsCacheType
	githubUserCacheType
	gitlabUserCacheType
)

type objectCache struct {
	mutex *sync.RWMutex
	store map[uint8]map[string]any
}

func newObjectCache() *objectCache {
	store := make(map[uint8]map[string]any)
	store[githubBranchesCacheType] = make(map[string]any)
	store[githubPullRequestCacheType] = make(map[string]any)
	store[githubSearchResultsCacheType] = make(map[string]any)
	store[githubUserCacheType] = make(map[string]any)
	store[gitlabUserCacheType] = make(map[string]any)

	return &objectCache{
		mutex: new(sync.RWMutex),
		store: store,
	}
}

func (c objectCache) getGithubBranches(query string) []*github.Branch {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if v, ok := c.store[githubBranchesCacheType][query]; ok {
		return v.([]*github.Branch)
	}
	return nil
}

func (c objectCache) setGithubBranches(query string, result []*github.Branch) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.store[githubBranchesCacheType][query] = result
}

func (c objectCache) getGithubPullRequest(query string) *github.PullRequest {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if v, ok := c.store[githubPullRequestCacheType][query]; ok {
		return pointer(v.(github.PullRequest))
	}
	return nil
}

func (c objectCache) setGithubPullRequest(query string, result github.PullRequest) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.store[githubPullRequestCacheType][query] = result
}

func (c objectCache) getGithubSearchResults(query string) *github.IssuesSearchResult {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if v, ok := c.store[githubSearchResultsCacheType][query]; ok {
		return pointer(v.(github.IssuesSearchResult))
	}
	return nil
}

func (c objectCache) setGithubSearchResults(query string, result github.IssuesSearchResult) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.store[githubSearchResultsCacheType][query] = result
}

func (c objectCache) getGithubUser(username string) *github.User {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if v, ok := c.store[gitlabUserCacheType][username]; ok {
		return pointer(v.(github.User))
	}
	return nil
}

func (c objectCache) setGithubUser(username string, user github.User) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.store[gitlabUserCacheType][username] = user
}

func (c objectCache) getGitlabUser(username string) *gitlab.User {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if v, ok := c.store[githubUserCacheType][username]; ok {
		return pointer(v.(gitlab.User))
	}
	return nil
}

func (c objectCache) setGitlabUser(username string, user gitlab.User) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.store[githubUserCacheType][username] = user
}
