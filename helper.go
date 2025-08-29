package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/go-github/v74/github"
	"github.com/xanzy/go-gitlab"
)

func getGithubBranches(ctx context.Context, owner, repo string) ([]*github.Branch, error) {
	var err error
	cacheToken := fmt.Sprintf("%s/%s", owner, repo)
	result := cache.getGithubBranches(cacheToken)
	if result == nil {
		logger.Debug("listing branches")
		result, _, err = gh.Repositories.ListBranches(ctx, owner, repo, nil)
		if err != nil {
			return nil, fmt.Errorf("listing branches: %v", err)
		}

		if result == nil {
			return nil, fmt.Errorf("nil result was returned when listing branches for repo: %s/%s", owner, repo)
		}

		logger.Trace("caching GitHub branches", "repo", fmt.Sprintf("%s/%s", owner, repo))
		cache.setGithubBranches(cacheToken, result)
	}

	return result, nil
}

func getGithubPullRequest(ctx context.Context, org, repo string, prNumber int) (*github.PullRequest, error) {
	var err error
	cacheToken := fmt.Sprintf("%s/%s/%d", org, repo, prNumber)
	pullRequest := cache.getGithubPullRequest(cacheToken)
	if pullRequest == nil {
		logger.Debug("retrieving pull request details", "owner", org, "repo", repo, "pr_number", prNumber)
		pullRequest, _, err = gh.PullRequests.Get(ctx, org, repo, prNumber)
		if err != nil {
			return nil, fmt.Errorf("retrieving pull request: %v", err)
		}

		if pullRequest == nil {
			return nil, fmt.Errorf("nil pull request was returned: %d", prNumber)
		}

		logger.Trace("caching pull request details", "owner", org, "repo", repo, "pr_number", prNumber)
		cache.setGithubPullRequest(cacheToken, *pullRequest)
	}

	return pullRequest, nil
}

func getGithubSearchResults(ctx context.Context, query string) (*github.IssuesSearchResult, error) {
	var err error
	result := cache.getGithubSearchResults(query)
	if result == nil {
		logger.Debug("performing search", "query", query)
		result, _, err = gh.Search.Issues(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("performing issue search: %v", err)
		}

		if result == nil {
			return nil, fmt.Errorf("nil search result was returned for query: %s", query)
		}

		logger.Trace("caching GitHub search result", "query", query)
		cache.setGithubSearchResults(query, *result)
	}

	return result, nil
}

func getGithubUser(ctx context.Context, username string) (*github.User, error) {
	var err error
	user := cache.getGithubUser(username)
	if user == nil {
		logger.Debug("retrieving user details", "username", username)
		if user, _, err = gh.Users.Get(ctx, username); err != nil {
			return nil, err
		}

		if user == nil {
			return nil, fmt.Errorf("nil user was returned: %s", username)
		}

		logger.Trace("caching GitHub user", "username", username)
		cache.setGithubUser(username, *user)
	}

	if user.Type == nil {
		return nil, fmt.Errorf("unable to determine whether owner is a user or organisatition: %s", username)
	}

	return user, nil
}

func getGitlabUser(username string) (*gitlab.User, error) {
	user := cache.getGitlabUser(username)
	if user == nil {
		logger.Debug("retrieving user details", "username", username)
		users, _, err := gl.Users.ListUsers(&gitlab.ListUsersOptions{Username: &username})
		if err != nil {
			return nil, err
		}

		for _, user = range users {
			if user != nil && user.Username == username {
				logger.Trace("caching GitLab user", "username", username)
				cache.setGitlabUser(username, *user)

				return user, nil
			}
		}

		return nil, fmt.Errorf("GitLab user not found: %s", username)
	}

	return user, nil
}

func pointer[T any](v T) *T {
	return &v
}

func parseProjectSlugs(slugs []string) ([]string, []string, error) {
	if len(slugs) != 2 {
		return nil, nil, fmt.Errorf("too many fields")
	}

	delimPosition := strings.LastIndex(slugs[0], "/")
	gitlabPath := []string{
		slugs[0][:delimPosition],
		slugs[0][delimPosition+1:],
	}
	githubPath := strings.Split(slugs[1], "/")

	if len(gitlabPath) != 2 {
		return nil, nil, fmt.Errorf("invalid GitLab project: %s", slugs[0])
	}
	if len(githubPath) != 2 {
		return nil, nil, fmt.Errorf("invalid GitHub project: %s", slugs[1])
	}

	return gitlabPath, githubPath, nil
}

func roundDuration(d, r time.Duration) time.Duration {
	if r <= 0 {
		return d
	}
	neg := d < 0
	if neg {
		d = -d
	}
	if m := d % r; m+m < r {
		d = d - m
	} else {
		d = d + r - m
	}
	if neg {
		return -d
	}
	return d
}

func sendErr(err error) {
	errCount++
	logger.Error(err.Error())
}

func unmarshalResp(resp *http.Response, model interface{}) error {
	if resp == nil {
		return nil
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("parsing response body: %+v", err)
	}
	_ = resp.Body.Close()

	// Trim away a BOM if present
	respBody = bytes.TrimPrefix(respBody, []byte("\xef\xbb\xbf"))

	// In some cases the respBody is empty, but not nil, so don't attempt to unmarshal this
	if len(respBody) == 0 {
		return nil
	}

	// Unmarshal into provided model
	if err := json.Unmarshal(respBody, model); err != nil {
		return fmt.Errorf("unmarshaling response body: %+v", err)
	}

	// Reassign the response body as downstream code may expect it
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))

	return nil
}
