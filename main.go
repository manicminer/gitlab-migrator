package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/gofri/go-github-pagination/githubpagination"
	"github.com/google/go-github/v74/github"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/xanzy/go-gitlab"
)

const (
	dateFormat          = "Mon, 2 Jan 2006"
	defaultGithubDomain = "github.com"
	defaultGitlabDomain = "gitlab.com"
)

var loop, report bool
var deleteExistingRepos, enablePullRequests, renameMasterToMain bool
var githubDomain, githubRepo, githubToken, githubUser, gitlabDomain, gitlabProject, gitlabToken, projectsCsvPath string

var (
	cache          *objectCache
	errCount       int
	logger         hclog.Logger
	gh             *github.Client
	gl             *gitlab.Client
	maxConcurrency int
)

type Project = []string

type Report struct {
	GroupName          string
	ProjectName        string
	MergeRequestsCount int
}

type GitHubError struct {
	Message          string
	DocumentationURL string `json:"documentation_url"`
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
	resp.Body.Close()

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

func main() {
	var err error

	// Bypass pre-emptive rate limit checks in the GitHub client, as we will handle these via go-retryablehttp
	valueCtx := context.WithValue(context.Background(), github.BypassRateLimitCheck, true)

	// Assign a Done channel so we can abort on Ctrl-c
	ctx, cancel := context.WithCancel(valueCtx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	logger = hclog.New(&hclog.LoggerOptions{
		Name:  "gitlab-migrator",
		Level: hclog.LevelFromString(os.Getenv("LOG_LEVEL")),
	})

	cache = newObjectCache()

	githubToken = os.Getenv("GITHUB_TOKEN")
	if githubToken == "" {
		logger.Error("missing environment variable", "name", "GITHUB_TOKEN")
		os.Exit(1)
	}

	gitlabToken = os.Getenv("GITLAB_TOKEN")
	if gitlabToken == "" {
		logger.Error("missing environment variable", "name", "GITLAB_TOKEN")
		os.Exit(1)
	}

	flag.BoolVar(&loop, "loop", false, "continue migrating until canceled")
	flag.BoolVar(&report, "report", false, "report on primitives to be migrated instead of beginning migration")

	flag.BoolVar(&deleteExistingRepos, "delete-existing-repos", false, "whether existing repositories should be deleted before migrating")
	flag.BoolVar(&enablePullRequests, "migrate-pull-requests", false, "whether pull requests should be migrated")
	flag.BoolVar(&renameMasterToMain, "rename-master-to-main", false, "rename master branch to main and update pull requests")

	flag.StringVar(&githubDomain, "github-domain", defaultGithubDomain, "specifies the GitHub domain to use")
	flag.StringVar(&githubRepo, "github-repo", "", "the GitHub repository to migrate to")
	flag.StringVar(&githubUser, "github-user", "", "specifies the GitHub user to use, who will author any migrated PRs (required)")
	flag.StringVar(&gitlabDomain, "gitlab-domain", defaultGitlabDomain, "specifies the GitLab domain to use")
	flag.StringVar(&gitlabProject, "gitlab-project", "", "the GitLab project to migrate")
	flag.StringVar(&projectsCsvPath, "projects-csv", "", "specifies the path to a CSV file describing projects to migrate (incompatible with -gitlab-project and -github-repo)")

	flag.IntVar(&maxConcurrency, "max-concurrency", 4, "how many projects to migrate in parallel")

	flag.Parse()

	if githubUser == "" {
		githubUser = os.Getenv("GITHUB_USER")
	}

	if githubUser == "" {
		logger.Error("must specify GitHub user")
		os.Exit(1)
	}

	repoSpecifiedInline := githubRepo != "" && gitlabProject != ""
	if repoSpecifiedInline && projectsCsvPath != "" {
		logger.Error("cannot specify -projects-csv and either -github-repo or -gitlab-project at the same time")
		os.Exit(1)
	}
	if !repoSpecifiedInline && projectsCsvPath == "" {
		logger.Error("must specify either -projects-csv or both of -github-repo and -gitlab-project")
		os.Exit(1)
	}

	retryClient := &retryablehttp.Client{
		HTTPClient:   cleanhttp.DefaultPooledClient(),
		Logger:       nil,
		RetryMax:     2,
		RetryWaitMin: 30 * time.Second,
		RetryWaitMax: 300 * time.Second,
	}

	retryClient.Backoff = func(min, max time.Duration, attemptNum int, resp *http.Response) (sleep time.Duration) {
		requestMethod := "unknown"
		requestUrl := "unknown"

		if req := resp.Request; req != nil {
			requestMethod = req.Method
			if req.URL != nil {
				requestUrl = req.URL.String()
			}
		}

		defer func() {
			logger.Trace("waiting before retrying failed API request", "method", requestMethod, "url", requestUrl, "status", resp.StatusCode, "sleep", sleep, "attempt", attemptNum, "max_attempts", retryClient.RetryMax)
		}()

		if resp != nil {
			// Check the Retry-After header
			if s, ok := resp.Header["Retry-After"]; ok {
				if retryAfter, err := strconv.ParseInt(s[0], 10, 64); err == nil {
					sleep = time.Second * time.Duration(retryAfter)
					return
				}
			}

			// Reference:
			// - https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28
			// - https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api?apiVersion=2022-11-28
			if v, ok := resp.Header["X-Ratelimit-Remaining"]; ok {
				if remaining, err := strconv.ParseInt(v[0], 10, 64); err == nil && remaining == 0 {

					// If x-ratelimit-reset is present, this indicates the UTC timestamp when we can retry
					if w, ok := resp.Header["X-Ratelimit-Reset"]; ok {
						if recoveryEpoch, err := strconv.ParseInt(w[0], 10, 64); err == nil {
							// Add 30 seconds to recovery timestamp for clock differences
							sleep = roundDuration(time.Until(time.Unix(recoveryEpoch+30, 0)), time.Second)
							return
						}
					}

					// Otherwise, wait for 60 seconds
					sleep = 60 * time.Second
					return
				}
			}
		}

		// Exponential backoff
		mult := math.Pow(2, float64(attemptNum)) * float64(min)
		wait := time.Duration(mult)
		if float64(wait) != mult || wait > max {
			wait = max
		}

		sleep = wait
		return
	}

	retryClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		if err != nil {
			return false, err
		}

		// Potential connection reset
		if resp == nil {
			return true, nil
		}

		errResp := GitHubError{}
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			if err = unmarshalResp(resp, &errResp); err != nil {
				return false, err
			}
		}

		// Token not authorized for org
		if resp.StatusCode == http.StatusForbidden {
			if match, err := regexp.MatchString("SAML enforcement", errResp.Message); err != nil {
				return false, fmt.Errorf("matching 403 response: %v", err)
			} else if match {
				msg := errResp.Message
				if errResp.DocumentationURL != "" {
					msg += fmt.Sprintf(" - %s", errResp.DocumentationURL)
				}
				return false, fmt.Errorf("received 403 with response: %v", msg)
			}
		}

		retryableStatuses := []int{
			http.StatusTooManyRequests, // rate-limiting
			http.StatusForbidden,       // rate-limiting

			http.StatusRequestTimeout,
			http.StatusFailedDependency,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout,
		}

		requestMethod := "unknown"
		requestUrl := "unknown"

		if req := resp.Request; req != nil {
			requestMethod = req.Method
			if req.URL != nil {
				requestUrl = req.URL.String()
			}
		}

		for _, status := range retryableStatuses {
			if resp.StatusCode == status {
				logger.Trace("retrying failed API request", "method", requestMethod, "url", requestUrl, "status", resp.StatusCode, "message", errResp.Message)
				return true, nil
			}
		}

		return false, nil
	}

	transport := &gitHubAdvancedSearchModder{
		base: &retryablehttp.RoundTripper{Client: retryClient},
	}
	client := githubpagination.NewClient(transport, githubpagination.WithPerPage(100))

	if githubDomain == defaultGithubDomain {
		gh = github.NewClient(client).WithAuthToken(githubToken)
	} else {
		githubUrl := fmt.Sprintf("https://%s", githubDomain)
		if gh, err = github.NewClient(client).WithAuthToken(githubToken).WithEnterpriseURLs(githubUrl, githubUrl); err != nil {
			sendErr(err)
			os.Exit(1)
		}
	}

	gitlabOpts := make([]gitlab.ClientOptionFunc, 0)
	if gitlabDomain != defaultGitlabDomain {
		gitlabUrl := fmt.Sprintf("https://%s", gitlabDomain)
		gitlabOpts = append(gitlabOpts, gitlab.WithBaseURL(gitlabUrl))
	}
	if gl, err = gitlab.NewClient(gitlabToken, gitlabOpts...); err != nil {
		sendErr(err)
		os.Exit(1)
	}

	projects := make([]Project, 0)
	if projectsCsvPath != "" {
		data, err := os.ReadFile(projectsCsvPath)
		if err != nil {
			sendErr(err)
			os.Exit(1)
		}

		// Trim a UTF-8 BOM, if present
		data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))

		if projects, err = csv.NewReader(bytes.NewBuffer(data)).ReadAll(); err != nil {
			sendErr(err)
			os.Exit(1)
		}
	} else {
		projects = []Project{{gitlabProject, githubRepo}}
	}

	if report {
		printReport(ctx, projects)
	} else {
		if err = performMigration(ctx, projects); err != nil {
			sendErr(err)
			os.Exit(1)
		} else if errCount > 0 {
			logger.Warn(fmt.Sprintf("encountered %d errors during migration, review log output for details", errCount))
			os.Exit(1)
		}
	}
}

func printReport(ctx context.Context, projects []Project) {
	logger.Debug("building report")

	results := make([]Report, 0)

	for _, proj := range projects {
		if err := ctx.Err(); err != nil {
			return
		}

		result, err := reportProject(ctx, proj)
		if err != nil {
			errCount++
			sendErr(err)
		}

		if result != nil {
			results = append(results, *result)
		}
	}

	fmt.Println()

	totalMergeRequests := 0
	for _, result := range results {
		totalMergeRequests += result.MergeRequestsCount
		fmt.Printf("%#v\n", result)
	}

	fmt.Println()
	fmt.Printf("Total merge requests: %d\n", totalMergeRequests)
	fmt.Println()
}

func reportProject(ctx context.Context, proj []string) (*Report, error) {
	gitlabPath := strings.Split(proj[0], "/")
	//githubPath := strings.Split(proj[1], "/")

	logger.Debug("searching for GitLab project", "name", gitlabPath[1], "group", gitlabPath[0])
	searchTerm := gitlabPath[1]
	projectResult, _, err := gl.Projects.ListProjects(&gitlab.ListProjectsOptions{Search: &searchTerm})
	if err != nil {
		return nil, fmt.Errorf("listing projects: %v", err)
	}

	var project *gitlab.Project
	for _, item := range projectResult {
		if item == nil {
			continue
		}

		if item.PathWithNamespace == proj[0] {
			logger.Debug("found GitLab project", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", item.ID)
			project = item
		}
	}

	if project == nil {
		return nil, fmt.Errorf("no matching GitLab project found: %s", proj[0])
	}

	var mergeRequests []*gitlab.MergeRequest

	opts := &gitlab.ListProjectMergeRequestsOptions{
		OrderBy: pointer("created_at"),
		Sort:    pointer("asc"),
	}

	logger.Debug("retrieving GitLab merge requests", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID)
	for {
		result, resp, err := gl.MergeRequests.ListProjectMergeRequests(project.ID, opts)
		if err != nil {
			return nil, fmt.Errorf("retrieving gitlab merge requests: %v", err)
		}

		mergeRequests = append(mergeRequests, result...)

		if resp.NextPage == 0 {
			break
		}

		opts.Page = resp.NextPage
	}

	return &Report{
		GroupName:          gitlabPath[0],
		ProjectName:        gitlabPath[1],
		MergeRequestsCount: len(mergeRequests),
	}, nil
}

func performMigration(ctx context.Context, projects []Project) error {
	concurrency := maxConcurrency
	if len(projects) < maxConcurrency {
		concurrency = len(projects)
	}

	logger.Info(fmt.Sprintf("processing %d project(s) with %d workers", len(projects), concurrency))

	var wg sync.WaitGroup
	queue := make(chan Project, concurrency*2)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for proj := range queue {
				if err := ctx.Err(); err != nil {
					break
				}

				if err := migrateProject(ctx, proj); err != nil {
					errCount++
					sendErr(err)
				}
			}
		}()
	}

	queueProjects := func() {
		for _, proj := range projects {
			if err := ctx.Err(); err != nil {
				break
			}

			queue <- proj
		}
	}

	if loop {
		logger.Info(fmt.Sprintf("looping migration until canceled"))
		for {
			if err := ctx.Err(); err != nil {
				break
			}

			queueProjects()
		}
	} else {
		queueProjects()
		close(queue)
	}

	wg.Wait()

	return nil
}

func migrateProject(ctx context.Context, proj []string) error {
	gitlabPath := strings.Split(proj[0], "/")
	githubPath := strings.Split(proj[1], "/")

	logger.Info("searching for GitLab project", "name", gitlabPath[1], "group", gitlabPath[0])
	searchTerm := gitlabPath[1]
	projectResult, _, err := gl.Projects.ListProjects(&gitlab.ListProjectsOptions{Search: &searchTerm})
	if err != nil {
		return fmt.Errorf("listing projects: %v", err)
	}

	var project *gitlab.Project
	for _, item := range projectResult {
		if item == nil {
			continue
		}

		if item.PathWithNamespace == proj[0] {
			logger.Debug("found GitLab project", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", item.ID)
			project = item
		}
	}

	if project == nil {
		return fmt.Errorf("no matching GitLab project found: %s", proj[0])
	}

	cloneUrl, err := url.Parse(project.HTTPURLToRepo)
	if err != nil {
		return fmt.Errorf("parsing clone URL: %v", err)
	}

	logger.Info("mirroring repository from GitLab to GitHub", "name", gitlabPath[1], "group", gitlabPath[0], "github_org", githubPath[0], "github_repo", githubPath[1])

	user, err := getGithubUser(ctx, githubPath[0])
	if err != nil {
		return fmt.Errorf("retrieving github user: %v", err)
	}

	var org string
	if strings.EqualFold(*user.Type, "organization") {
		org = githubPath[0]
	} else if !strings.EqualFold(*user.Type, "user") || !strings.EqualFold(*user.Login, githubPath[0]) {
		return fmt.Errorf("configured owner is neither an organization nor the current user: %s", githubPath[0])
	}

	logger.Debug("checking for existing repository on GitHub", "owner", githubPath[0], "repo", githubPath[1])
	_, _, err = gh.Repositories.Get(ctx, githubPath[0], githubPath[1])

	var githubError *github.ErrorResponse
	if err != nil && (!errors.As(err, &githubError) || githubError == nil || githubError.Response == nil || githubError.Response.StatusCode != http.StatusNotFound) {
		return fmt.Errorf("retrieving github repo: %v", err)
	}

	var createRepo, repoDeleted bool
	if err != nil {
		createRepo = true
	} else if deleteExistingRepos {
		logger.Warn("existing repository was found on GitHub, proceeding to delete", "owner", githubPath[0], "repo", githubPath[1])
		if _, err = gh.Repositories.Delete(ctx, githubPath[0], githubPath[1]); err != nil {
			return fmt.Errorf("deleting existing github repo: %v", err)
		}

		createRepo = true
		repoDeleted = true
	}

	defaultBranch := "main"
	if !renameMasterToMain && project.DefaultBranch != "" {
		defaultBranch = project.DefaultBranch
	}

	homepage := fmt.Sprintf("https://%s/%s/%s", gitlabDomain, gitlabPath[0], gitlabPath[1])

	if createRepo {
		if repoDeleted {
			logger.Warn("recreating GitHub repository", "owner", githubPath[0], "repo", githubPath[1])
		} else {
			logger.Debug("repository not found on GitHub, proceeding to create", "owner", githubPath[0], "repo", githubPath[1])
		}
		newRepo := github.Repository{
			Name:          pointer(githubPath[1]),
			Description:   &project.Description,
			Homepage:      &homepage,
			DefaultBranch: &defaultBranch,
			Private:       pointer(true),
			HasIssues:     pointer(true),
			HasProjects:   pointer(true),
			HasWiki:       pointer(true),
		}
		if _, _, err = gh.Repositories.Create(ctx, org, &newRepo); err != nil {
			return fmt.Errorf("creating github repo: %v", err)
		}
	}

	logger.Debug("updating repository settings", "owner", githubPath[0], "repo", githubPath[1])
	description := regexp.MustCompile("\r|\n").ReplaceAllString(project.Description, " ")
	updateRepo := github.Repository{
		Name:              pointer(githubPath[1]),
		Description:       &description,
		Homepage:          &homepage,
		AllowAutoMerge:    pointer(true),
		AllowMergeCommit:  pointer(true),
		AllowRebaseMerge:  pointer(true),
		AllowSquashMerge:  pointer(true),
		AllowUpdateBranch: pointer(true),
	}
	if _, _, err = gh.Repositories.Edit(ctx, githubPath[0], githubPath[1], &updateRepo); err != nil {
		return fmt.Errorf("updating github repo: %v", err)
	}

	cloneUrl.User = url.UserPassword("oauth2", gitlabToken)
	cloneUrlWithCredentials := cloneUrl.String()

	// In-memory filesystem for worktree operations
	fs := memfs.New()

	logger.Debug("cloning repository", "name", gitlabPath[1], "group", gitlabPath[0], "url", project.HTTPURLToRepo)
	repo, err := git.CloneContext(ctx, memory.NewStorage(), fs, &git.CloneOptions{
		URL:        cloneUrlWithCredentials,
		Auth:       nil,
		RemoteName: "gitlab",
		Mirror:     true,
	})
	if err != nil {
		return fmt.Errorf("cloning gitlab repo: %v", err)
	}

	if renameMasterToMain {
		if masterBranch, err := repo.Reference(plumbing.NewBranchReferenceName("master"), false); err == nil {
			logger.Info("renaming master branch to main prior to push", "name", gitlabPath[1], "group", gitlabPath[0], "sha", masterBranch.Hash())

			logger.Debug("creating main branch", "name", gitlabPath[1], "group", gitlabPath[0], "sha", masterBranch.Hash())
			mainBranch := plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), masterBranch.Hash())
			if err = repo.Storer.SetReference(mainBranch); err != nil {
				return fmt.Errorf("creating main branch: %v", err)
			}

			logger.Debug("deleting master branch", "name", gitlabPath[1], "group", gitlabPath[0], "sha", masterBranch.Hash())
			if err = repo.Storer.RemoveReference(masterBranch.Name()); err != nil {
				return fmt.Errorf("deleting master branch: %v", err)
			}
		}
	}

	githubUrl := fmt.Sprintf("https://%s/%s/%s", githubDomain, githubPath[0], githubPath[1])
	githubUrlWithCredentials := fmt.Sprintf("https://%s:%s@%s/%s/%s", githubUser, githubToken, githubDomain, githubPath[0], githubPath[1])

	logger.Debug("adding remote for GitHub repository", "name", gitlabPath[1], "group", gitlabPath[0], "url", githubUrl)
	if _, err = repo.CreateRemote(&config.RemoteConfig{
		Name:   "github",
		URLs:   []string{githubUrlWithCredentials},
		Mirror: true,
	}); err != nil {
		return fmt.Errorf("adding github remote: %v", err)
	}

	logger.Debug("force-pushing to GitHub repository", "name", gitlabPath[1], "group", gitlabPath[0], "url", githubUrl)
	if err = repo.PushContext(ctx, &git.PushOptions{
		RemoteName: "github",
		Force:      true,
		//Prune:      true, // causes error, attempts to delete main branch
	}); err != nil {
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			logger.Debug("repository already up-to-date on GitHub", "name", gitlabPath[1], "group", gitlabPath[0], "url", githubUrl)
		} else {
			return fmt.Errorf("pushing to github repo: %v", err)
		}
	}

	logger.Debug("pushing tags to GitHub repository", "name", gitlabPath[1], "group", gitlabPath[0], "url", githubUrl)
	if err = repo.PushContext(ctx, &git.PushOptions{
		RemoteName: "github",
		Force:      true,
		RefSpecs:   []config.RefSpec{"refs/tags/*:refs/tags/*"},
	}); err != nil {
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			logger.Debug("repository already up-to-date on GitHub", "name", gitlabPath[1], "group", gitlabPath[0], "url", githubUrl)
		} else {
			return fmt.Errorf("pushing to github repo: %v", err)
		}
	}

	logger.Debug("setting default repository branch", "owner", githubPath[0], "repo", githubPath[1], "branch_name", defaultBranch)
	updateRepo = github.Repository{
		DefaultBranch: &defaultBranch,
	}
	if _, _, err = gh.Repositories.Edit(ctx, githubPath[0], githubPath[1], &updateRepo); err != nil {
		return fmt.Errorf("setting default branch: %v", err)
	}

	if enablePullRequests {
		migratePullRequests(ctx, githubPath, gitlabPath, project, repo)
	}

	return nil
}

func migratePullRequests(ctx context.Context, githubPath, gitlabPath []string, project *gitlab.Project, repo *git.Repository) {
	var mergeRequests []*gitlab.MergeRequest

	opts := &gitlab.ListProjectMergeRequestsOptions{
		OrderBy: pointer("created_at"),
		Sort:    pointer("asc"),
	}

	logger.Debug("retrieving GitLab merge requests", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID)
	for {
		result, resp, err := gl.MergeRequests.ListProjectMergeRequests(project.ID, opts)
		if err != nil {
			sendErr(fmt.Errorf("retrieving gitlab merge requests: %v", err))
			return
		}

		mergeRequests = append(mergeRequests, result...)

		if resp.NextPage == 0 {
			break
		}

		opts.Page = resp.NextPage
	}

	var successCount, failureCount int
	totalCount := len(mergeRequests)
	logger.Info("migrating merge requests from GitLab to GitHub", "name", gitlabPath[1], "group", gitlabPath[0], "count", totalCount)
	for _, mergeRequest := range mergeRequests {
		if mergeRequest == nil {
			continue
		}

		// Check for context cancellation
		if err := ctx.Err(); err != nil {
			sendErr(fmt.Errorf("preparing to list pull requests: %v", err))
			break
		}

		sourceBranchForClosedMergeRequest := fmt.Sprintf("migration-source-%d/%s", mergeRequest.IID, mergeRequest.SourceBranch)
		targetBranchForClosedMergeRequest := fmt.Sprintf("migration-target-%d/%s", mergeRequest.IID, mergeRequest.TargetBranch)

		var cleanUpBranch bool
		var pullRequest *github.PullRequest

		logger.Debug("searching for any existing pull request", "owner", githubPath[0], "repo", githubPath[1], "merge_request_id", mergeRequest.IID)
		sourceBranches := []string{mergeRequest.SourceBranch, sourceBranchForClosedMergeRequest}
		branchQuery := fmt.Sprintf("head:%s", strings.Join(sourceBranches, " OR head:"))
		query := fmt.Sprintf("repo:%s/%s AND is:pr AND (%s)", githubPath[0], githubPath[1], branchQuery)
		searchResult, err := getGithubSearchResults(ctx, query)
		if err != nil {
			sendErr(fmt.Errorf("listing pull requests: %v", err))
			continue
		}

		// Look for an existing GitHub pull request
		skip := false
		for _, issue := range searchResult.Issues {
			if issue == nil {
				continue
			}

			// Check for context cancellation
			if err := ctx.Err(); err != nil {
				sendErr(fmt.Errorf("preparing to retrieve pull request: %v", err))
				break
			}

			if issue.IsPullRequest() {
				// Extract the PR number from the URL
				prUrl, err := url.Parse(*issue.PullRequestLinks.URL)
				if err != nil {
					sendErr(fmt.Errorf("parsing pull request url: %v", err))
					skip = true
					break
				}

				if m := regexp.MustCompile(".+/([0-9]+)$").FindStringSubmatch(prUrl.Path); len(m) == 2 {
					prNumber, _ := strconv.Atoi(m[1])
					pr, err := getGithubPullRequest(ctx, githubPath[0], githubPath[1], prNumber)
					if err != nil {
						sendErr(fmt.Errorf("retrieving pull request: %v", err))
						skip = true
						break
					}

					if strings.Contains(pr.GetBody(), fmt.Sprintf("**GitLab MR Number** | %d", mergeRequest.IID)) ||
						strings.Contains(pr.GetBody(), fmt.Sprintf("**GitLab MR Number** | [%d]", mergeRequest.IID)) {
						logger.Debug("found existing pull request", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pr.GetNumber())
						pullRequest = pr
						break
					}
				}
			}
		}
		if skip {
			continue
		}

		// Proceed to create temporary branches when migrating a merged/closed merge request that doesn't yet have a counterpart PR in GitHub (can't create one without a branch)
		if pullRequest == nil && !strings.EqualFold(mergeRequest.State, "opened") {
			logger.Trace("searching for existing branch for closed/merged merge request", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID, "source_branch", mergeRequest.SourceBranch)

			// Create a worktree
			worktree, err := repo.Worktree()
			if err != nil {
				sendErr(fmt.Errorf("creating worktree: %v", err))
				failureCount++
				continue
			}

			// Generate temporary branch names
			mergeRequest.SourceBranch = sourceBranchForClosedMergeRequest
			mergeRequest.TargetBranch = targetBranchForClosedMergeRequest

			logger.Trace("retrieving commits for merge request", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID)
			mergeRequestCommits, _, err := gl.MergeRequests.GetMergeRequestCommits(project.ID, mergeRequest.IID, &gitlab.GetMergeRequestCommitsOptions{OrderBy: "created_at", Sort: "asc"})
			if err != nil {
				sendErr(fmt.Errorf("retrieving merge request commits: %v", err))
				failureCount++
				continue
			}

			// Some merge requests have no commits, disregard these
			if len(mergeRequestCommits) == 0 {
				continue
			}

			// API is buggy, ordering is not respected, so we'll reorder by commit datestamp
			sort.Slice(mergeRequestCommits, func(i, j int) bool {
				return mergeRequestCommits[i].CommittedDate.Before(*mergeRequestCommits[j].CommittedDate)
			})

			if mergeRequestCommits[0] == nil {
				sendErr(fmt.Errorf("start commit for merge request %d is nil", mergeRequest.IID))
				failureCount++
				continue
			}
			if mergeRequestCommits[len(mergeRequestCommits)-1] == nil {
				sendErr(fmt.Errorf("end commit for merge request %d is nil", mergeRequest.IID))
				failureCount++
				continue
			}

			logger.Trace("inspecting start commit", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID, "sha", mergeRequestCommits[0].ShortID)
			startCommit, err := object.GetCommit(repo.Storer, plumbing.NewHash(mergeRequestCommits[0].ID))
			if err != nil {
				sendErr(fmt.Errorf("loading start commit: %v", err))
				failureCount++
				continue
			}

			if startCommit.NumParents() == 0 {
				// Orphaned commit, start with an empty branch
				// TODO: this isn't working as hoped, try to figure this out. in the meantime, we'll skip MRs from orphaned branches
				//if err = repo.Storer.SetReference(plumbing.NewSymbolicReference("HEAD", plumbing.ReferenceName(fmt.Sprintf("refs/heads/%s", mergeRequest.TargetBranch)))); err != nil {
				//	return fmt.Errorf("creating empty branch: %s", err)
				//}
				sendErr(fmt.Errorf("start commit %s for merge request %d has no parents", mergeRequestCommits[0].ShortID, mergeRequest.IID))
				continue
			} else {
				// Sometimes we will be starting from a merge commit, so look for a suitable parent commit to branch out from
				var startCommitParent *object.Commit
				for i := 0; i < startCommit.NumParents(); i++ {
					logger.Trace("inspecting start commit parent", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID, "sha", mergeRequestCommits[0].ShortID)
					startCommitParent, err = startCommit.Parent(0)
					if err != nil {
						sendErr(fmt.Errorf("loading parent commit: %s", err))
					}

					continue
				}

				if startCommitParent == nil {
					sendErr(fmt.Errorf("identifying suitable parent of start commit %s for merge request %d", mergeRequestCommits[0].ShortID, mergeRequest.IID))
					failureCount++
				}

				logger.Trace("creating target branch for merged/closed merge request", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID, "branch", mergeRequest.TargetBranch, "sha", startCommitParent.Hash)
				if err = worktree.Checkout(&git.CheckoutOptions{
					Create: true,
					Force:  true,
					Branch: plumbing.NewBranchReferenceName(mergeRequest.TargetBranch),
					Hash:   startCommitParent.Hash,
				}); err != nil {
					sendErr(fmt.Errorf("checking out temporary target branch: %v", err))
					failureCount++
					continue
				}
			}

			endHash := plumbing.NewHash(mergeRequestCommits[len(mergeRequestCommits)-1].ID)
			logger.Trace("creating source branch for merged/closed merge request", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID, "branch", mergeRequest.SourceBranch, "sha", endHash)
			if err = worktree.Checkout(&git.CheckoutOptions{
				Create: true,
				Force:  true,
				Branch: plumbing.NewBranchReferenceName(mergeRequest.SourceBranch),
				Hash:   endHash,
			}); err != nil {
				sendErr(fmt.Errorf("checking out temporary source branch: %v", err))
				failureCount++
				continue
			}

			logger.Debug("pushing branches for merged/closed merge request", "owner", githubPath[0], "repo", githubPath[1], "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
			if err = repo.PushContext(ctx, &git.PushOptions{
				RemoteName: "github",
				RefSpecs: []config.RefSpec{
					config.RefSpec(fmt.Sprintf("refs/heads/%[1]s:refs/heads/%[1]s", mergeRequest.SourceBranch)),
					config.RefSpec(fmt.Sprintf("refs/heads/%[1]s:refs/heads/%[1]s", mergeRequest.TargetBranch)),
				},
				Force: true,
			}); err != nil {
				if errors.Is(err, git.NoErrAlreadyUpToDate) {
					logger.Trace("branch already exists and is up-to-date on GitHub", "owner", githubPath[0], "repo", githubPath[1], "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
				} else {
					sendErr(fmt.Errorf("pushing temporary branches to github: %v", err))
					failureCount++
					continue
				}
			}

			// We will clean up these temporary branches after configuring and closing the pull request
			cleanUpBranch = true
		}

		if renameMasterToMain && mergeRequest.TargetBranch == "master" {
			logger.Trace("changing target branch from master to main", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID)
			mergeRequest.TargetBranch = "main"
		}

		githubAuthorName := mergeRequest.Author.Name

		author, err := getGitlabUser(mergeRequest.Author.Username)
		if err != nil {
			sendErr(fmt.Errorf("retrieving gitlab user: %v", err))
			failureCount++
			continue
		}
		if author.WebsiteURL != "" {
			githubAuthorName = "@" + strings.TrimPrefix(strings.ToLower(author.WebsiteURL), "https://github.com/")
		}

		originalState := ""
		if !strings.EqualFold(mergeRequest.State, "opened") {
			originalState = fmt.Sprintf("> This merge request was originally **%s** on GitLab", mergeRequest.State)
		}

		logger.Debug("determining merge request approvers", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID)
		approvers := make([]string, 0)
		awards, _, err := gl.AwardEmoji.ListMergeRequestAwardEmoji(project.ID, mergeRequest.IID, &gitlab.ListAwardEmojiOptions{PerPage: 100})
		if err != nil {
			sendErr(fmt.Errorf("listing merge request awards: %v", err))
		} else {
			for _, award := range awards {
				if award.Name == "thumbsup" {
					approver := award.User.Name

					approverUser, err := getGitlabUser(award.User.Username)
					if err != nil {
						sendErr(fmt.Errorf("retrieving gitlab user: %v", err))
						continue
					}
					if approverUser.WebsiteURL != "" {
						approver = "@" + strings.TrimPrefix(strings.ToLower(approverUser.WebsiteURL), "https://github.com/")
					}

					approvers = append(approvers, approver)
				}
			}
		}

		description := mergeRequest.Description
		if strings.TrimSpace(description) == "" {
			description = "_No description_"
		}

		slices.Sort(approvers)
		approval := strings.Join(approvers, ", ")
		if approval == "" {
			approval = "_No approvers_"
		}

		closeDate := ""
		if mergeRequest.State == "closed" && mergeRequest.ClosedAt != nil {
			closeDate = fmt.Sprintf("\n> | **Date Originally Closed** | %s |", mergeRequest.ClosedAt.Format(dateFormat))
		} else if mergeRequest.State == "merged" && mergeRequest.MergedAt != nil {
			closeDate = fmt.Sprintf("\n> | **Date Originally Merged** | %s |", mergeRequest.MergedAt.Format(dateFormat))
		}

		mergeRequestTitle := mergeRequest.Title
		if len(mergeRequestTitle) > 40 {
			mergeRequestTitle = mergeRequestTitle[:40] + "..."
		}

		body := fmt.Sprintf(`> [!NOTE]
> This pull request was migrated from GitLab
>
> |      |      |
> | ---- | ---- |
> | **Original Author** | %[1]s |
> | **GitLab Project** | [%[4]s/%[5]s](https://%[10]s/%[4]s/%[5]s) |
> | **GitLab Merge Request** | [%[11]s](https://%[10]s/%[4]s/%[5]s/merge_requests/%[2]d) |
> | **GitLab MR Number** | [%[2]d](https://%[10]s/%[4]s/%[5]s/merge_requests/%[2]d) |
> | **Date Originally Opened** | %[6]s |%[7]s
> | **Approved on GitLab by** | %[8]s |
> |      |      |
>
%[9]s

## Original Description

%[3]s`, githubAuthorName, mergeRequest.IID, description, gitlabPath[0], gitlabPath[1], mergeRequest.CreatedAt.Format(dateFormat), closeDate, approval, originalState, gitlabDomain, mergeRequestTitle)

		if pullRequest == nil {
			logger.Info("creating pull request", "owner", githubPath[0], "repo", githubPath[1], "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
			newPullRequest := github.NewPullRequest{
				Title:               &mergeRequest.Title,
				Head:                &mergeRequest.SourceBranch,
				Base:                &mergeRequest.TargetBranch,
				Body:                &body,
				MaintainerCanModify: pointer(true),
				Draft:               &mergeRequest.Draft,
			}
			if pullRequest, _, err = gh.PullRequests.Create(ctx, githubPath[0], githubPath[1], &newPullRequest); err != nil {
				sendErr(fmt.Errorf("creating pull request: %v", err))
				failureCount++
				continue
			}

			if mergeRequest.State == "closed" || mergeRequest.State == "merged" {
				logger.Debug("closing pull request", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber())

				pullRequest.State = pointer("closed")
				if pullRequest, _, err = gh.PullRequests.Edit(ctx, githubPath[0], githubPath[1], pullRequest.GetNumber(), pullRequest); err != nil {
					sendErr(fmt.Errorf("updating pull request: %v", err))
					failureCount++
					continue
				}
			}

		} else {
			var newState *string
			switch mergeRequest.State {
			case "opened":
				newState = pointer("open")
			case "closed", "merged":
				newState = pointer("closed")
			}

			if pullRequest.State != nil && newState != nil && *pullRequest.State != *newState {
				pullRequestState := &github.PullRequest{
					Number: pullRequest.Number,
					State:  newState,
				}

				if pullRequest, _, err = gh.PullRequests.Edit(ctx, githubPath[0], githubPath[1], pullRequestState.GetNumber(), pullRequestState); err != nil {
					sendErr(fmt.Errorf("updating pull request state: %v", err))
					failureCount++
					continue
				}
			}

			if (newState != nil && (pullRequest.State == nil || *pullRequest.State != *newState)) ||
				(pullRequest.Title == nil || *pullRequest.Title != mergeRequest.Title) ||
				(pullRequest.Body == nil || *pullRequest.Body != body) ||
				(pullRequest.Draft == nil || *pullRequest.Draft != mergeRequest.Draft) {
				logger.Info("updating pull request", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber())

				pullRequest.Title = &mergeRequest.Title
				pullRequest.Body = &body
				pullRequest.Draft = &mergeRequest.Draft
				pullRequest.MaintainerCanModify = nil
				if pullRequest, _, err = gh.PullRequests.Edit(ctx, githubPath[0], githubPath[1], pullRequest.GetNumber(), pullRequest); err != nil {
					sendErr(fmt.Errorf("updating pull request: %v", err))
					failureCount++
					continue
				}
			} else {
				logger.Trace("existing pull request is up-to-date", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber())
			}
		}

		if cleanUpBranch {
			logger.Debug("deleting temporary branches for closed pull request", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber(), "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
			if err = repo.PushContext(ctx, &git.PushOptions{
				RemoteName: "github",
				RefSpecs: []config.RefSpec{
					config.RefSpec(fmt.Sprintf(":refs/heads/%s", mergeRequest.SourceBranch)),
					config.RefSpec(fmt.Sprintf(":refs/heads/%s", mergeRequest.TargetBranch)),
				},
				Force: true,
			}); err != nil {
				if errors.Is(err, git.NoErrAlreadyUpToDate) {
					logger.Trace("branches already deleted on GitHub", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber(), "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
				} else {
					sendErr(fmt.Errorf("pushing branch deletions to github: %v", err))
					failureCount++
					continue
				}
			}
		}

		var comments []*gitlab.Note
		skipComments := false
		opts := &gitlab.ListMergeRequestNotesOptions{
			OrderBy: pointer("created_at"),
			Sort:    pointer("asc"),
		}

		logger.Debug("retrieving GitLab merge request comments", "name", gitlabPath[1], "group", gitlabPath[0], "project_id", project.ID, "merge_request_id", mergeRequest.IID)
		for {
			result, resp, err := gl.Notes.ListMergeRequestNotes(project.ID, mergeRequest.IID, opts)
			if err != nil {
				sendErr(fmt.Errorf("listing merge request notes: %v", err))
				skipComments = true
				break
			}

			comments = append(comments, result...)

			if resp.NextPage == 0 {
				break
			}

			opts.Page = resp.NextPage
		}

		if skipComments {
			failureCount++
		} else {
			logger.Debug("retrieving GitHub pull request comments", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber())
			prComments, _, err := gh.Issues.ListComments(ctx, githubPath[0], githubPath[1], pullRequest.GetNumber(), &github.IssueListCommentsOptions{Sort: pointer("created"), Direction: pointer("asc")})
			if err != nil {
				sendErr(fmt.Errorf("listing pull request comments: %v", err))
			} else {
				logger.Info("migrating merge request comments from GitLab to GitHub", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber(), "count", len(comments))

				for _, comment := range comments {
					if comment == nil || comment.System {
						continue
					}

					githubCommentAuthorName := comment.Author.Name

					commentAuthor, err := getGitlabUser(comment.Author.Username)
					if err != nil {
						sendErr(fmt.Errorf("retrieving gitlab user: %v", err))
						failureCount++
						break
					}
					if commentAuthor.WebsiteURL != "" {
						githubCommentAuthorName = "@" + strings.TrimPrefix(strings.ToLower(commentAuthor.WebsiteURL), "https://github.com/")
					}

					commentBody := fmt.Sprintf(`> [!NOTE]
> This comment was migrated from GitLab
>
> |      |      |
> | ---- | ---- |
> | **Original Author** | %[1]s |
> | **Note ID** | %[2]d |
> | **Date Originally Created** | %[3]s |
> |      |      |
>

## Original Comment

%[4]s`, githubCommentAuthorName, comment.ID, comment.CreatedAt.Format("Mon, 2 Jan 2006"), comment.Body)

					foundExistingComment := false
					for _, prComment := range prComments {
						if prComment == nil {
							continue
						}

						if strings.Contains(prComment.GetBody(), fmt.Sprintf("**Note ID** | %d", comment.ID)) {
							foundExistingComment = true

							if prComment.Body == nil || *prComment.Body != commentBody {
								logger.Debug("updating pull request comment", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber(), "comment_id", prComment.GetID())
								prComment.Body = &commentBody
								if _, _, err = gh.Issues.EditComment(ctx, githubPath[0], githubPath[1], prComment.GetID(), prComment); err != nil {
									sendErr(fmt.Errorf("updating pull request comments: %v", err))
									failureCount++
									break
								}
							}
						} else {
							logger.Trace("existing pull request comment is up-to-date", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber(), "comment_id", prComment.GetID())
						}
					}

					if !foundExistingComment {
						logger.Debug("creating pull request comment", "owner", githubPath[0], "repo", githubPath[1], "pr_number", pullRequest.GetNumber())
						newComment := github.IssueComment{
							Body: &commentBody,
						}
						if _, _, err = gh.Issues.CreateComment(ctx, githubPath[0], githubPath[1], pullRequest.GetNumber(), &newComment); err != nil {
							sendErr(fmt.Errorf("creating pull request comment: %v", err))
							failureCount++
							break
						}
					}
				}
			}

			successCount++
		}
	}

	skippedCount := totalCount - successCount - failureCount

	logger.Info("migrated merge requests from GitLab to GitHub", "name", gitlabPath[1], "group", gitlabPath[0], "successful", successCount, "failed", failureCount, "skipped", skippedCount)
}
