import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { PRReviewResult } from '../../api/client';
import { PRReviewResultCard } from './PRReviewResultCard';

function resultFixture(overrides: Partial<PRReviewResult> = {}): PRReviewResult {
  return {
    result_key: 'run:14',
    run_id: 14,
    repo_id: 3,
    repo_full_name: 'acme/widget',
    pr_number: 133,
    pr_title: 'Keep exact-head review evidence',
    pr_url: 'https://github.com/acme/widget/pull/133',
    review_id: 113,
    base_ref: 'main',
    base_sha: 'b'.repeat(40),
    head_sha: 'a'.repeat(40),
    verdict_state: 'complete',
    aggregate_verdict: 'changes_required',
    publication_state: 'not_applicable',
    lifecycle_state: 'merged',
    failure_stage: 'lifecycle',
    error_category: null,
    error_measured: null,
    error_limit: null,
    error_unit: null,
    display_status: 'Changes required · PR merged',
    display_summary: 'The PR was merged while CCM was reviewing the exact head, so the result was not published.',
    published_actor: null,
    published_at: null,
    github_review_id: null,
    github_review_url: null,
    github_state: null,
    github_event: null,
    created_at: '2026-08-16T00:00:00Z',
    updated_at: '2026-08-16T00:02:00Z',
    completed_at: '2026-08-16T00:02:00Z',
    can_rerun: false,
    ...overrides,
  };
}

function renderCard(result: PRReviewResult = resultFixture()) {
  const onOpenDetail = vi.fn();
  const onCreateFollowUp = vi.fn();
  const onRerun = vi.fn().mockResolvedValue(undefined);
  render(
    <PRReviewResultCard
      result={result}
      onOpenDetail={onOpenDetail}
      onCreateFollowUp={onCreateFollowUp}
      onRerun={onRerun}
    />,
  );
  return { onOpenDetail, onCreateFollowUp, onRerun };
}

describe('PRReviewResultCard', () => {
  it('can render the aggregate projection without review mutation controls', () => {
    render(
      <PRReviewResultCard
        result={resultFixture()}
        readOnly
      />,
    );

    expect(screen.getByRole('article', { name: /PR Review Result acme\/widget #133/ })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Open review details|Create follow-up|Re-run exact head/i })).not.toBeInTheDocument();
    expect(screen.queryByRole('link', { name: /Open GitHub PR|Open published comment/i })).not.toBeInTheDocument();
  });

  it('keeps a complete code verdict separate from a stale publication and merged lifecycle', () => {
    renderCard();

    const card = screen.getByRole('article', { name: /PR Review Result acme\/widget #133/ });
    expect(within(card).getByText('Code verdict: Changes required')).toBeInTheDocument();
    expect(within(card).getByText('GitHub publication not applicable')).toBeInTheDocument();
    expect(within(card).getByText('PR merged')).toBeInTheDocument();
    expect(within(card).queryByText(/Infrastructure error/i)).not.toBeInTheDocument();
    expect(within(card).queryByText(/No code verdict/i)).not.toBeInTheDocument();
  });

  it('describes a COMMENT publication as a comment and shows the backend publisher evidence', () => {
    renderCard(resultFixture({
      aggregate_verdict: 'pass',
      publication_state: 'published',
      lifecycle_state: 'reviewing',
      failure_stage: null,
      published_actor: 'youchengsong',
      published_at: '2026-08-16T01:02:03Z',
      github_review_id: 987,
      github_review_url: 'https://github.com/acme/widget/pull/133#pullrequestreview-987',
      github_state: 'COMMENTED',
    }));

    expect(screen.getByText('Code verdict: Pass')).toBeInTheDocument();
    expect(screen.getByText('GitHub comment published')).toBeInTheDocument();
    expect(screen.getByText(/Published by CCM as youchengsong/)).toBeInTheDocument();
    expect(screen.getByText(/GitHub COMMENTED/)).toBeInTheDocument();
    expect(screen.queryByText(/Approved/i)).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open published comment' })).toHaveAttribute(
      'href',
      'https://github.com/acme/widget/pull/133#pullrequestreview-987',
    );
  });

  it('shows a deterministic input-size rejection instead of a generic infrastructure error', () => {
    renderCard(resultFixture({
      verdict_state: 'unavailable',
      aggregate_verdict: null,
      publication_state: 'not_applicable',
      lifecycle_state: 'reviewing',
      failure_stage: 'reviewer',
      error_category: 'unsupported_input_size',
      error_measured: 120_001,
      error_limit: 120_000,
      error_unit: 'characters',
      display_status: 'Review input too large',
      display_summary: 'The exact PR review input exceeded the configured safe model limit.',
    }));

    expect(screen.getByText('Code review not started: input too large')).toBeInTheDocument();
    expect(screen.getByText('GitHub publication not applicable')).toBeInTheDocument();
    expect(screen.getByText(/Review input: 120,001 characters; safe limit: 120,000 characters/)).toBeInTheDocument();
    expect(screen.getByText('Failure stage: review input admission')).toHaveClass('text-amber-300');
    const verdict = screen.getByText('Code review not started: input too large').closest('.rounded');
    expect(verdict).toHaveClass('border-amber-500/30', 'bg-amber-500/10', 'text-amber-300');
    expect(verdict).not.toHaveClass('border-red-500/30', 'bg-red-500/10', 'text-red-300');
    expect(screen.getByLabelText('Review input exceeds the safe limit')).toBeInTheDocument();
    expect(screen.queryByText(/Infrastructure error/i)).not.toBeInTheDocument();
  });

  it.each([
    ['a script URL', 'javascript:alert(1)'],
    ['a lookalike host', 'https://github.com.evil/acme/widget/pull/133'],
    ['a different repository', 'https://github.com/acme/other/pull/133'],
    ['a different PR', 'https://github.com/acme/widget/pull/134'],
  ])('does not render the GitHub PR link for %s', (_label, prUrl) => {
    renderCard(resultFixture({ pr_url: prUrl }));

    expect(screen.queryByRole('link', { name: 'Open GitHub PR' })).not.toBeInTheDocument();
  });

  it.each([
    ['a lookalike host', 'https://github.com.evil/acme/widget/pull/133#pullrequestreview-987'],
    ['a different repository', 'https://github.com/acme/other/pull/133#pullrequestreview-987'],
    ['a different PR', 'https://github.com/acme/widget/pull/134#pullrequestreview-987'],
    ['a different Review', 'https://github.com/acme/widget/pull/133#pullrequestreview-988'],
  ])('does not render the published Review link for %s', (_label, reviewUrl) => {
    renderCard(resultFixture({
      publication_state: 'published',
      github_review_id: 987,
      github_review_url: reviewUrl,
    }));

    expect(screen.queryByRole('link', { name: 'Open published comment' })).not.toBeInTheDocument();
  });

  it('offers only result-safe actions and exact-head rerun', async () => {
    const result = resultFixture({ can_rerun: true, lifecycle_state: 'closed' });
    const callbacks = renderCard(result);

    await userEvent.click(screen.getByRole('button', { name: 'Open review details' }));
    await userEvent.click(screen.getByRole('button', { name: /Create follow-up Task/ }));
    await userEvent.click(screen.getByRole('button', { name: /Re-run exact head/ }));

    expect(callbacks.onOpenDetail).toHaveBeenCalledWith(result);
    expect(callbacks.onCreateFollowUp).toHaveBeenCalledWith(result);
    expect(callbacks.onRerun).toHaveBeenCalledWith(result);
    expect(screen.queryByRole('button', { name: /Chat|Interrupt|Share|Copy prompt|Retry Task/i })).not.toBeInTheDocument();
    expect(document.body).not.toHaveTextContent('Reviewer Task');
    expect(document.body).not.toHaveTextContent('session');
    expect(document.body).not.toHaveTextContent('nonce');
  });
});
