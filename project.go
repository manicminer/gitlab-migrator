package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/google/go-github/v74/github"
	"github.com/xanzy/go-gitlab"
)

func newProject(slugs []string) (*project, error) {
	var err error
	p := &project{}

	p.gitlabPath, p.githubPath, err = parseProjectSlugs(slugs)
	if err != nil {
		return nil, fmt.Errorf("parsing project slugs: %v", err)
	}

	logger.Info("searching for GitLab project", "name", p.gitlabPath[1], "group", p.gitlabPath[0])
	searchTerm := p.gitlabPath[1]
	projectResult, _, err := gl.Projects.ListProjects(&gitlab.ListProjectsOptions{Search: &searchTerm})
	if err != nil {
		return nil, fmt.Errorf("listing projects: %v", err)
	}

	for _, item := range projectResult {
		if item == nil {
			continue
		}

		if item.PathWithNamespace == slugs[0] {
			logger.Debug("found GitLab project", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", item.ID)
			p.project = item
		}
	}

	if p.project == nil {
		return nil, fmt.Errorf("no matching GitLab project found: %s", slugs[0])
	}

	p.defaultBranch = "main"
	if renameTrunkBranch != "" {
		p.defaultBranch = renameTrunkBranch
	} else if !renameMasterToMain && p.project.DefaultBranch != "" {
		p.defaultBranch = p.project.DefaultBranch
	}

	return p, nil
}

type project struct {
	project       *gitlab.Project
	repo          *git.Repository
	defaultBranch string
	gitlabPath    []string
	githubPath    []string
}

func (p *project) createRepo(ctx context.Context, homepage string, repoDeleted bool) error {
	if repoDeleted {
		logger.Warn("recreating GitHub repository", "owner", p.githubPath[0], "repo", p.githubPath[1])
	} else {
		logger.Debug("repository not found on GitHub, proceeding to create", "owner", p.githubPath[0], "repo", p.githubPath[1])
	}
	newRepo := github.Repository{
		Name:          pointer(p.githubPath[1]),
		Description:   &p.project.Description,
		Homepage:      &homepage,
		DefaultBranch: &p.defaultBranch,
		Private:       pointer(true),
		HasIssues:     pointer(true),
		HasProjects:   pointer(true),
		HasWiki:       pointer(true),
	}
	if _, _, err := gh.Repositories.Create(ctx, p.githubPath[0], &newRepo); err != nil {
		return fmt.Errorf("creating github repo: %v", err)
	}

	return nil
}

func (p *project) migrate(ctx context.Context) error {
	cloneUrl, err := url.Parse(p.project.HTTPURLToRepo)
	if err != nil {
		return fmt.Errorf("parsing clone URL: %v", err)
	}

	logger.Info("mirroring repository from GitLab to GitHub", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "github_org", p.githubPath[0], "github_repo", p.githubPath[1])

	logger.Debug("checking for existing repository on GitHub", "owner", p.githubPath[0], "repo", p.githubPath[1])
	_, _, err = gh.Repositories.Get(ctx, p.githubPath[0], p.githubPath[1])

	var githubError *github.ErrorResponse
	if err != nil && (!errors.As(err, &githubError) || githubError == nil || githubError.Response == nil || githubError.Response.StatusCode != http.StatusNotFound) {
		return fmt.Errorf("retrieving github repo: %v", err)
	}

	homepage := fmt.Sprintf("https://%s/%s/%s", gitlabDomain, p.gitlabPath[0], p.gitlabPath[1])

	if err != nil {
		// Repository not found
		if err = p.createRepo(ctx, homepage, false); err != nil {
			return err
		}
	} else if deleteExistingRepos {
		logger.Warn("existing repository was found on GitHub, proceeding to delete", "owner", p.githubPath[0], "repo", p.githubPath[1])
		if _, err = gh.Repositories.Delete(ctx, p.githubPath[0], p.githubPath[1]); err != nil {
			return fmt.Errorf("deleting existing github repo: %v", err)
		}

		if err = p.createRepo(ctx, homepage, true); err != nil {
			return err
		}
	}

	logger.Debug("updating repository settings", "owner", p.githubPath[0], "repo", p.githubPath[1])
	description := regexp.MustCompile("\r|\n").ReplaceAllString(p.project.Description, " ")
	updateRepo := github.Repository{
		Name:              pointer(p.githubPath[1]),
		Description:       &description,
		Homepage:          &homepage,
		AllowAutoMerge:    pointer(true),
		AllowMergeCommit:  pointer(true),
		AllowRebaseMerge:  pointer(true),
		AllowSquashMerge:  pointer(true),
		AllowUpdateBranch: pointer(true),
	}
	if _, _, err = gh.Repositories.Edit(ctx, p.githubPath[0], p.githubPath[1], &updateRepo); err != nil {
		return fmt.Errorf("updating github repo: %v", err)
	}

	cloneUrl.User = url.UserPassword("oauth2", gitlabToken)
	cloneUrlWithCredentials := cloneUrl.String()

	// In-memory filesystem for worktree operations
	fs := memfs.New()

	logger.Debug("cloning repository", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "url", p.project.HTTPURLToRepo)
	p.repo, err = git.CloneContext(ctx, memory.NewStorage(), fs, &git.CloneOptions{
		URL:        cloneUrlWithCredentials,
		Auth:       nil,
		RemoteName: "gitlab",
		Mirror:     true,
	})
	if err != nil {
		return fmt.Errorf("cloning gitlab repo: %v", err)
	}

	if p.defaultBranch != p.project.DefaultBranch {
		if gitlabTrunk, err := p.repo.Reference(plumbing.NewBranchReferenceName(p.project.DefaultBranch), false); err == nil {
			logger.Info("renaming trunk branch prior to push", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "gitlab_trunk", p.project.DefaultBranch, "github_trunk", p.defaultBranch, "sha", gitlabTrunk.Hash())

			logger.Debug("creating new trunk branch", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "github_trunk", p.defaultBranch, "sha", gitlabTrunk.Hash())
			githubTrunk := plumbing.NewHashReference(plumbing.NewBranchReferenceName(p.defaultBranch), gitlabTrunk.Hash())
			if err = p.repo.Storer.SetReference(githubTrunk); err != nil {
				return fmt.Errorf("creating trunk branch: %v", err)
			}

			logger.Debug("deleting old trunk branch", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "gitlab_trunk", p.project.DefaultBranch, "sha", gitlabTrunk.Hash())
			if err = p.repo.Storer.RemoveReference(gitlabTrunk.Name()); err != nil {
				return fmt.Errorf("deleting old trunk branch: %v", err)
			}
		}
	}

	githubUrl := fmt.Sprintf("https://%s/%s/%s", githubDomain, p.githubPath[0], p.githubPath[1])
	githubUrlWithCredentials := fmt.Sprintf("https://%s:%s@%s/%s/%s", githubUser, githubToken, githubDomain, p.githubPath[0], p.githubPath[1])

	logger.Debug("adding remote for GitHub repository", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "url", githubUrl)
	if _, err = p.repo.CreateRemote(&config.RemoteConfig{
		Name:   "github",
		URLs:   []string{githubUrlWithCredentials},
		Mirror: true,
	}); err != nil {
		return fmt.Errorf("adding github remote: %v", err)
	}

	logger.Debug("determining branches to push", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "url", githubUrl)
	branches, err := p.repo.Branches()
	if err != nil {
		return fmt.Errorf("retrieving branches: %v", err)
	}

	refSpecs := make([]config.RefSpec, 0)
	if err = branches.ForEach(func(ref *plumbing.Reference) error {
		refSpecs = append(refSpecs, config.RefSpec(fmt.Sprintf("%[1]s:%[1]s", ref.Name())))
		return nil
	}); err != nil {
		return fmt.Errorf("parsing branches: %v", err)
	}

	logger.Debug("force-pushing branches to GitHub repository", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "url", githubUrl, "count", len(refSpecs))
	if err = p.repo.PushContext(ctx, &git.PushOptions{
		RemoteName: "github",
		Force:      true,
		RefSpecs:   refSpecs,
		//Prune:      true, // causes error, attempts to delete main branch
	}); err != nil {
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			logger.Debug("repository already up-to-date on GitHub", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "url", githubUrl)
		} else {
			return fmt.Errorf("pushing to github repo: %v", err)
		}
	}

	logger.Debug("force-pushing tags to GitHub repository", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "url", githubUrl)
	if err = p.repo.PushContext(ctx, &git.PushOptions{
		RemoteName: "github",
		Force:      true,
		RefSpecs:   []config.RefSpec{"refs/tags/*:refs/tags/*"},
	}); err != nil {
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			logger.Debug("repository already up-to-date on GitHub", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "url", githubUrl)
		} else {
			return fmt.Errorf("pushing to github repo: %v", err)
		}
	}

	logger.Debug("setting default repository branch", "owner", p.githubPath[0], "repo", p.githubPath[1], "branch_name", p.defaultBranch)
	updateRepo = github.Repository{
		DefaultBranch: &p.defaultBranch,
	}
	if _, _, err = gh.Repositories.Edit(ctx, p.githubPath[0], p.githubPath[1], &updateRepo); err != nil {
		return fmt.Errorf("setting default branch: %v", err)
	}

	if enablePullRequests {
		p.migrateMergeRequests(ctx)
	}

	return nil
}

func (p *project) migrateMergeRequests(ctx context.Context) {
	var mergeRequests []*gitlab.MergeRequest

	opts := &gitlab.ListProjectMergeRequestsOptions{
		OrderBy: pointer("created_at"),
		Sort:    pointer("asc"),
	}

	logger.Debug("retrieving GitLab merge requests", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID)
	for {
		result, resp, err := gl.MergeRequests.ListProjectMergeRequests(p.project.ID, opts)
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
	logger.Info("migrating merge requests from GitLab to GitHub", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "count", totalCount)
	for _, mergeRequest := range mergeRequests {
		if mergeRequest == nil {
			continue
		}

		if err := p.migrateMergeRequest(ctx, mergeRequest); err != nil {
			sendErr(err)
			failureCount++
		} else {
			successCount++
		}
		// call out to migrateMergeRequest and increment counters
	}

	skippedCount := totalCount - successCount - failureCount

	logger.Info("migrated merge requests from GitLab to GitHub", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "successful", successCount, "failed", failureCount, "skipped", skippedCount)
}

func (p *project) migrateMergeRequest(ctx context.Context, mergeRequest *gitlab.MergeRequest) error {
	// Check for context cancellation
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("preparing to list pull requests: %v", err)
	}

	sourceBranchForClosedMergeRequest := fmt.Sprintf("migration-source-%d/%s", mergeRequest.IID, mergeRequest.SourceBranch)
	targetBranchForClosedMergeRequest := fmt.Sprintf("migration-target-%d/%s", mergeRequest.IID, mergeRequest.TargetBranch)

	var pullRequest *github.PullRequest

	logger.Debug("searching for any existing pull request", "owner", p.githubPath[0], "repo", p.githubPath[1], "merge_request_id", mergeRequest.IID)
	sourceBranches := []string{mergeRequest.SourceBranch, sourceBranchForClosedMergeRequest}
	branchQuery := fmt.Sprintf("head:%s", strings.Join(sourceBranches, " OR head:"))
	query := fmt.Sprintf("repo:%s/%s AND is:pr AND (%s)", p.githubPath[0], p.githubPath[1], branchQuery)
	searchResult, err := getGithubSearchResults(ctx, query)
	if err != nil {
		return fmt.Errorf("listing pull requests: %v", err)
	}

	// Look for an existing GitHub pull request
	for _, issue := range searchResult.Issues {
		if issue == nil {
			continue
		}

		// Check for context cancellation
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("preparing to retrieve pull request: %v", err)
		}

		if issue.IsPullRequest() {
			// Extract the PR number from the URL
			prUrl, err := url.Parse(*issue.PullRequestLinks.URL)
			if err != nil {
				return fmt.Errorf("parsing pull request url: %v", err)
			}

			if m := regexp.MustCompile(".+/([0-9]+)$").FindStringSubmatch(prUrl.Path); len(m) == 2 {
				prNumber, _ := strconv.Atoi(m[1])
				pr, err := getGithubPullRequest(ctx, p.githubPath[0], p.githubPath[1], prNumber)
				if err != nil {
					return fmt.Errorf("retrieving pull request: %v", err)
				}

				if strings.Contains(pr.GetBody(), fmt.Sprintf("**GitLab MR Number** | %d", mergeRequest.IID)) ||
					strings.Contains(pr.GetBody(), fmt.Sprintf("**GitLab MR Number** | [%d]", mergeRequest.IID)) {
					logger.Debug("found existing pull request", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pr.GetNumber())
					pullRequest = pr
					break
				}
			}
		}
	}

	// Proceed to create temporary branches when migrating a merged/closed merge request that doesn't yet have a counterpart PR in GitHub (can't create one without a branch)
	if pullRequest == nil && !strings.EqualFold(mergeRequest.State, "opened") {
		logger.Trace("searching for existing branch for closed/merged merge request", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID, "source_branch", mergeRequest.SourceBranch)

		// Create a worktree
		worktree, err := p.repo.Worktree()
		if err != nil {
			return fmt.Errorf("creating worktree: %v", err)
		}

		// Generate temporary branch names
		mergeRequest.SourceBranch = sourceBranchForClosedMergeRequest
		mergeRequest.TargetBranch = targetBranchForClosedMergeRequest

		logger.Trace("retrieving commits for merge request", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID)
		mergeRequestCommits, _, err := gl.MergeRequests.GetMergeRequestCommits(p.project.ID, mergeRequest.IID, &gitlab.GetMergeRequestCommitsOptions{OrderBy: "created_at", Sort: "asc"})
		if err != nil {
			return fmt.Errorf("retrieving merge request commits: %v", err)
		}

		// Some merge requests have no commits, disregard these
		if len(mergeRequestCommits) == 0 {
			return nil
		}

		// API is buggy, ordering is not respected, so we'll reorder by commit datestamp
		sort.Slice(mergeRequestCommits, func(i, j int) bool {
			return mergeRequestCommits[i].CommittedDate.Before(*mergeRequestCommits[j].CommittedDate)
		})

		if mergeRequestCommits[0] == nil {
			return fmt.Errorf("start commit for merge request %d is nil", mergeRequest.IID)
		}
		if mergeRequestCommits[len(mergeRequestCommits)-1] == nil {
			return fmt.Errorf("end commit for merge request %d is nil", mergeRequest.IID)
		}

		logger.Trace("inspecting start commit", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID, "sha", mergeRequestCommits[0].ShortID)
		startCommit, err := object.GetCommit(p.repo.Storer, plumbing.NewHash(mergeRequestCommits[0].ID))
		if err != nil {
			return fmt.Errorf("loading start commit: %v", err)
		}

		if startCommit.NumParents() == 0 {
			// Orphaned commit, start with an empty branch
			// TODO: this isn't working as hoped, try to figure this out. in the meantime, we'll skip MRs from orphaned branches
			//if err = repo.Storer.SetReference(plumbing.NewSymbolicReference("HEAD", plumbing.ReferenceName(fmt.Sprintf("refs/heads/%s", mergeRequest.TargetBranch)))); err != nil {
			//	return fmt.Errorf("creating empty branch: %s", err)
			//}
			return fmt.Errorf("start commit %s for merge request %d has no parents", mergeRequestCommits[0].ShortID, mergeRequest.IID)
		} else {
			// Sometimes we will be starting from a merge commit, so look for a suitable parent commit to branch out from
			var startCommitParent *object.Commit
			for i := 0; i < startCommit.NumParents(); i++ {
				logger.Trace("inspecting start commit parent", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID, "sha", mergeRequestCommits[0].ShortID)
				startCommitParent, err = startCommit.Parent(0)
				if err != nil {
					// Don't return as we will keep trying to find a parent
					sendErr(fmt.Errorf("loading parent commit: %s", err))
				}

				continue
			}

			if startCommitParent == nil {
				return fmt.Errorf("identifying suitable parent of start commit %s for merge request %d", mergeRequestCommits[0].ShortID, mergeRequest.IID)
			}

			logger.Trace("creating target branch for merged/closed merge request", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID, "branch", mergeRequest.TargetBranch, "sha", startCommitParent.Hash)
			if err = worktree.Checkout(&git.CheckoutOptions{
				Create: true,
				Force:  true,
				Branch: plumbing.NewBranchReferenceName(mergeRequest.TargetBranch),
				Hash:   startCommitParent.Hash,
			}); err != nil {
				return fmt.Errorf("checking out temporary target branch: %v", err)
			}
		}

		endHash := plumbing.NewHash(mergeRequestCommits[len(mergeRequestCommits)-1].ID)
		logger.Trace("creating source branch for merged/closed merge request", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID, "branch", mergeRequest.SourceBranch, "sha", endHash)
		if err = worktree.Checkout(&git.CheckoutOptions{
			Create: true,
			Force:  true,
			Branch: plumbing.NewBranchReferenceName(mergeRequest.SourceBranch),
			Hash:   endHash,
		}); err != nil {
			return fmt.Errorf("checking out temporary source branch: %v", err)
		}

		logger.Debug("pushing branches for merged/closed merge request", "owner", p.githubPath[0], "repo", p.githubPath[1], "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
		if err = p.repo.PushContext(ctx, &git.PushOptions{
			RemoteName: "github",
			RefSpecs: []config.RefSpec{
				config.RefSpec(fmt.Sprintf("refs/heads/%[1]s:refs/heads/%[1]s", mergeRequest.SourceBranch)),
				config.RefSpec(fmt.Sprintf("refs/heads/%[1]s:refs/heads/%[1]s", mergeRequest.TargetBranch)),
			},
			Force: true,
		}); err != nil {
			if errors.Is(err, git.NoErrAlreadyUpToDate) {
				logger.Trace("branch already exists and is up-to-date on GitHub", "owner", p.githubPath[0], "repo", p.githubPath[1], "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
			} else {
				return fmt.Errorf("pushing temporary branches to github: %v", err)
			}
		}

		// We will clean up these temporary branches after configuring and closing the pull request
		defer func() {
			logger.Debug("deleting temporary branches for closed pull request", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber(), "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
			if err := p.repo.PushContext(ctx, &git.PushOptions{
				RemoteName: "github",
				RefSpecs: []config.RefSpec{
					config.RefSpec(fmt.Sprintf(":refs/heads/%s", mergeRequest.SourceBranch)),
					config.RefSpec(fmt.Sprintf(":refs/heads/%s", mergeRequest.TargetBranch)),
				},
				Force: true,
			}); err != nil {
				if errors.Is(err, git.NoErrAlreadyUpToDate) {
					logger.Trace("branches already deleted on GitHub", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber(), "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
				} else {
					sendErr(fmt.Errorf("pushing branch deletions to github: %v", err))
				}
			}

		}()
	}

	if p.defaultBranch != p.project.DefaultBranch && mergeRequest.TargetBranch == p.project.DefaultBranch {
		logger.Trace("changing target trunk branch", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID, "old_trunk", p.project.DefaultBranch, "new_trunk", p.defaultBranch)
		mergeRequest.TargetBranch = p.defaultBranch
	}

	githubAuthorName := mergeRequest.Author.Name

	author, err := getGitlabUser(mergeRequest.Author.Username)
	if err != nil {
		return fmt.Errorf("retrieving gitlab user: %v", err)
	}
	if author.WebsiteURL != "" {
		githubAuthorName = "@" + strings.TrimPrefix(strings.ToLower(author.WebsiteURL), "https://github.com/")
	}

	originalState := ""
	if !strings.EqualFold(mergeRequest.State, "opened") {
		originalState = fmt.Sprintf("> This merge request was originally **%s** on GitLab", mergeRequest.State)
	}

	logger.Debug("determining merge request approvers", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID)
	approvers := make([]string, 0)
	awards, _, err := gl.AwardEmoji.ListMergeRequestAwardEmoji(p.project.ID, mergeRequest.IID, &gitlab.ListAwardEmojiOptions{PerPage: 100})
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

%[3]s`, githubAuthorName, mergeRequest.IID, description, p.gitlabPath[0], p.gitlabPath[1], mergeRequest.CreatedAt.Format(dateFormat), closeDate, approval, originalState, gitlabDomain, mergeRequestTitle)

	if pullRequest == nil {
		logger.Info("creating pull request", "owner", p.githubPath[0], "repo", p.githubPath[1], "source_branch", mergeRequest.SourceBranch, "target_branch", mergeRequest.TargetBranch)
		newPullRequest := github.NewPullRequest{
			Title:               &mergeRequest.Title,
			Head:                &mergeRequest.SourceBranch,
			Base:                &mergeRequest.TargetBranch,
			Body:                &body,
			MaintainerCanModify: pointer(true),
			Draft:               &mergeRequest.Draft,
		}
		if pullRequest, _, err = gh.PullRequests.Create(ctx, p.githubPath[0], p.githubPath[1], &newPullRequest); err != nil {
			return fmt.Errorf("creating pull request: %v", err)
		}

		if mergeRequest.State == "closed" || mergeRequest.State == "merged" {
			logger.Debug("closing pull request", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber())

			pullRequest.State = pointer("closed")
			if pullRequest, _, err = gh.PullRequests.Edit(ctx, p.githubPath[0], p.githubPath[1], pullRequest.GetNumber(), pullRequest); err != nil {
				return fmt.Errorf("updating pull request: %v", err)
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

			if pullRequest, _, err = gh.PullRequests.Edit(ctx, p.githubPath[0], p.githubPath[1], pullRequestState.GetNumber(), pullRequestState); err != nil {
				return fmt.Errorf("updating pull request state: %v", err)
			}
		}

		if (newState != nil && (pullRequest.State == nil || *pullRequest.State != *newState)) ||
			(pullRequest.Title == nil || *pullRequest.Title != mergeRequest.Title) ||
			(pullRequest.Body == nil || *pullRequest.Body != body) ||
			(pullRequest.Draft == nil || *pullRequest.Draft != mergeRequest.Draft) {
			logger.Info("updating pull request", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber())

			pullRequest.Title = &mergeRequest.Title
			pullRequest.Body = &body
			pullRequest.Draft = &mergeRequest.Draft
			pullRequest.MaintainerCanModify = nil
			if pullRequest, _, err = gh.PullRequests.Edit(ctx, p.githubPath[0], p.githubPath[1], pullRequest.GetNumber(), pullRequest); err != nil {
				return fmt.Errorf("updating pull request: %v", err)
			}
		} else {
			logger.Trace("existing pull request is up-to-date", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber())
		}
	}

	var comments []*gitlab.Note
	opts := &gitlab.ListMergeRequestNotesOptions{
		OrderBy: pointer("created_at"),
		Sort:    pointer("asc"),
	}

	logger.Debug("retrieving GitLab merge request comments", "name", p.gitlabPath[1], "group", p.gitlabPath[0], "project_id", p.project.ID, "merge_request_id", mergeRequest.IID)
	for {
		result, resp, err := gl.Notes.ListMergeRequestNotes(p.project.ID, mergeRequest.IID, opts)
		if err != nil {
			return fmt.Errorf("listing merge request notes: %v", err)
		}

		comments = append(comments, result...)

		if resp.NextPage == 0 {
			break
		}

		opts.Page = resp.NextPage
	}

	logger.Debug("retrieving GitHub pull request comments", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber())
	prComments, _, err := gh.Issues.ListComments(ctx, p.githubPath[0], p.githubPath[1], pullRequest.GetNumber(), &github.IssueListCommentsOptions{Sort: pointer("created"), Direction: pointer("asc")})
	if err != nil {
		sendErr(fmt.Errorf("listing pull request comments: %v", err))
	} else {
		logger.Info("migrating merge request comments from GitLab to GitHub", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber(), "count", len(comments))

		for _, comment := range comments {
			if comment == nil || comment.System {
				continue
			}

			githubCommentAuthorName := comment.Author.Name

			commentAuthor, err := getGitlabUser(comment.Author.Username)
			if err != nil {
				return fmt.Errorf("retrieving gitlab user: %v", err)
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
						logger.Debug("updating pull request comment", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber(), "comment_id", prComment.GetID())
						prComment.Body = &commentBody
						if _, _, err = gh.Issues.EditComment(ctx, p.githubPath[0], p.githubPath[1], prComment.GetID(), prComment); err != nil {
							return fmt.Errorf("updating pull request comments: %v", err)
						}
					}
				} else {
					logger.Trace("existing pull request comment is up-to-date", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber(), "comment_id", prComment.GetID())
				}
			}

			if !foundExistingComment {
				logger.Debug("creating pull request comment", "owner", p.githubPath[0], "repo", p.githubPath[1], "pr_number", pullRequest.GetNumber())
				newComment := github.IssueComment{
					Body: &commentBody,
				}
				if _, _, err = gh.Issues.CreateComment(ctx, p.githubPath[0], p.githubPath[1], pullRequest.GetNumber(), &newComment); err != nil {
					return fmt.Errorf("creating pull request comment: %v", err)
				}
			}
		}
	}

	return nil
}
