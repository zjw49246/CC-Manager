const GITHUB_HOST = 'github.com';
const REPO_PART = /^[A-Za-z0-9_.-]+$/;

function repoParts(repoFullName: string): [string, string] | null {
  const parts = repoFullName.split('/');
  if (
    parts.length !== 2
    || parts.some((part) => !REPO_PART.test(part) || part === '.' || part === '..')
  ) return null;
  return [parts[0], parts[1]];
}

function safeGitHubUrl(raw: string): URL | null {
  try {
    const authority = /^https:\/\/([^/?#]+)/i.exec(raw)?.[1];
    // URL normalisation erases an explicit default port and can obscure
    // authority tricks. Require the raw authority itself to be the host.
    if (!authority || authority.toLowerCase() !== GITHUB_HOST) return null;
    const url = new URL(raw);
    if (
      url.protocol !== 'https:'
      || url.hostname.toLowerCase() !== GITHUB_HOST
      || url.port
      || url.username
      || url.password
      || url.search
    ) return null;
    return url;
  } catch {
    return null;
  }
}

/** Return a canonical GitHub PR URL only when it matches the projected subject. */
export function canonicalGitHubPRUrl(
  raw: string | null | undefined,
  repoFullName: string,
  prNumber: number,
): string | null {
  if (!raw || !Number.isSafeInteger(prNumber) || prNumber <= 0) return null;
  const repo = repoParts(repoFullName);
  const url = safeGitHubUrl(raw);
  if (!repo || !url || url.hash) return null;
  const expectedPath = `/${repo[0]}/${repo[1]}/pull/${prNumber}`;
  if (url.pathname.toLowerCase() !== expectedPath.toLowerCase()) return null;
  return `https://${GITHUB_HOST}${expectedPath}`;
}

/** Return a canonical exact Review anchor only when repo, PR, and Review all match. */
export function canonicalGitHubReviewUrl(
  raw: string | null | undefined,
  repoFullName: string,
  prNumber: number,
  githubReviewId: number | null | undefined,
): string | null {
  if (!raw || !Number.isSafeInteger(githubReviewId) || Number(githubReviewId) <= 0) return null;
  const repo = repoParts(repoFullName);
  const url = safeGitHubUrl(raw);
  if (!repo || !url) return null;
  const expectedPath = `/${repo[0]}/${repo[1]}/pull/${prNumber}`;
  const expectedHash = `#pullrequestreview-${githubReviewId}`;
  if (
    url.pathname.toLowerCase() !== expectedPath.toLowerCase()
    || url.hash !== expectedHash
  ) return null;
  return `https://${GITHUB_HOST}${expectedPath}${expectedHash}`;
}

/**
 * Return a canonical Finding-thread URL bound to the projected repo, PR, and
 * GitHub comment id. Findings may be published either as an inline review
 * comment or as an issue-comment fallback, so only those two exact anchors
 * are accepted.
 */
export function canonicalGitHubFindingCommentUrl(
  raw: string | null | undefined,
  repoFullName: string,
  prNumber: number,
  githubCommentId: number | null | undefined,
): string | null {
  if (
    !raw
    || !Number.isSafeInteger(prNumber)
    || prNumber <= 0
    || !Number.isSafeInteger(githubCommentId)
    || Number(githubCommentId) <= 0
  ) return null;
  const repo = repoParts(repoFullName);
  const url = safeGitHubUrl(raw);
  if (!repo || !url) return null;
  const expectedPath = `/${repo[0]}/${repo[1]}/pull/${prNumber}`;
  if (url.pathname.toLowerCase() !== expectedPath.toLowerCase()) return null;
  const commentId = Number(githubCommentId);
  const allowedHashes = new Set([
    `#discussion_r${commentId}`,
    `#issuecomment-${commentId}`,
  ]);
  if (!allowedHashes.has(url.hash)) return null;
  return `https://${GITHUB_HOST}${expectedPath}${url.hash}`;
}
