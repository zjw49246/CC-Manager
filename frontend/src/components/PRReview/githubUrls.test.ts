import { describe, expect, it } from 'vitest';
import { canonicalGitHubFindingCommentUrl } from './githubUrls';

describe('canonicalGitHubFindingCommentUrl', () => {
  it.each([
    [
      'inline review comment',
      'https://github.com/acme/widgets/pull/42#discussion_r100',
    ],
    [
      'issue-comment fallback',
      'https://github.com/acme/widgets/pull/42#issuecomment-100',
    ],
  ])('accepts the exact %s anchor', (_label, url) => {
    expect(canonicalGitHubFindingCommentUrl(url, 'acme/widgets', 42, 100)).toBe(url);
  });

  it.each([
    'https://github.com/acme/widgets/pull/42?notification=1#discussion_r100',
    'https://user@github.com/acme/widgets/pull/42#discussion_r100',
    'https://github.com:443/acme/widgets/pull/42#discussion_r100',
    'https://github.com/acme/widgets/pull/42#discussion_r101',
    'https://github.com/acme/widgets/pull/43#discussion_r100',
  ])('rejects non-canonical evidence URL %s', (url) => {
    expect(canonicalGitHubFindingCommentUrl(url, 'acme/widgets', 42, 100)).toBeNull();
  });
});
