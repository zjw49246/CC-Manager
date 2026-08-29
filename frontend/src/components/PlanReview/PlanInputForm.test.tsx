import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { api, type PlanInputRequest, type PlanRun } from '../../api/client';
import { PlanInputForm } from './PlanInputForm';

vi.mock('../../api/client', () => ({
  isApiRequestError: () => false,
  api: { answerPlanInput: vi.fn().mockResolvedValue({}) },
}));

const run = {
  id: 71,
  generation: 9,
} as PlanRun;

function requestWithQuestions(count: number, id = 81): PlanInputRequest {
  return {
    id,
    plan_id: 61,
    run_id: run.id,
    source_step_id: 91,
    requested_by: 'planner',
    reason: 'Every answer is required to continue safely.',
    questions: Array.from({ length: count }, (_, index) => ({
      id: `question_${index}`,
      header: `Q${index + 1}`,
      question: `Required value ${index + 1}`,
      response_type: 'text' as const,
      options: [],
      required: true,
    })),
    status: 'open',
    answers: null,
    response_text: null,
    attachments: null,
    answered_by: null,
    opened_at: '2026-08-02T08:00:00Z',
    answered_at: null,
    created_at: '2026-08-02T08:00:00Z',
  };
}

describe('PlanInputForm', () => {
  beforeEach(() => vi.clearAllMocks());

  it('renders and submits every question without pagination or truncation', async () => {
    const request = requestWithQuestions(8);
    const onAnswered = vi.fn();
    render(<PlanInputForm run={run} request={request} onAnswered={onAnswered} />);

    expect(screen.getByText('8 questions')).toBeInTheDocument();
    for (let index = 1; index <= 8; index += 1) {
      expect(screen.getByText(`Required value ${index}`)).toBeInTheDocument();
    }

    const fields = screen.getAllByRole('textbox').slice(0, 8);
    for (let index = 0; index < fields.length; index += 1) {
      await userEvent.type(fields[index], `answer-${index + 1}`);
    }
    await userEvent.click(screen.getByRole('button', { name: 'Submit answers' }));

    await waitFor(() => expect(api.answerPlanInput).toHaveBeenCalledWith(
      71,
      81,
      expect.objectContaining({
        expected_run_generation: 9,
        answers: Array.from({ length: 8 }, (_, index) => ({
          question_id: `question_${index}`,
          value: `answer-${index + 1}`,
        })),
      }),
    ));
    expect(onAnswered).toHaveBeenCalledTimes(1);
  });

  it('reuses one idempotency key when a submission is retried', async () => {
    vi.mocked(api.answerPlanInput)
      .mockRejectedValueOnce(new Error('temporary disconnect'))
      .mockResolvedValueOnce({} as never);
    const request = requestWithQuestions(1);
    render(<PlanInputForm run={run} request={request} onAnswered={vi.fn()} />);

    await userEvent.type(screen.getAllByRole('textbox')[0], 'same answer');
    await userEvent.click(screen.getByRole('button', { name: 'Submit answers' }));
    await screen.findByText('temporary disconnect');
    await userEvent.click(screen.getByRole('button', { name: 'Submit answers' }));

    await waitFor(() => expect(api.answerPlanInput).toHaveBeenCalledTimes(2));
    const firstKey = vi.mocked(api.answerPlanInput).mock.calls[0][2].idempotency_key;
    const secondKey = vi.mocked(api.answerPlanInput).mock.calls[1][2].idempotency_key;
    expect(secondKey).toBe(firstKey);
  });

  it('uses a light-theme-readable text color for a selected choice', async () => {
    const request = requestWithQuestions(1);
    request.questions[0] = {
      ...request.questions[0],
      response_type: 'single_choice',
      options: [{ label: 'Fix a bug', value: 'bug' }],
    };
    render(<PlanInputForm run={run} request={request} onAnswered={vi.fn()} />);

    const option = screen.getByText('Fix a bug').closest('label');
    expect(option).not.toBeNull();
    await userEvent.click(option!);

    expect(option).toHaveClass('text-indigo-300');
    expect(option).not.toHaveClass('text-indigo-100');
  });

  it('submits free-form context when no required single-choice option fits', async () => {
    const request = requestWithQuestions(1);
    request.questions[0] = {
      ...request.questions[0],
      response_type: 'single_choice',
      options: [
        { label: 'Blue-green', value: 'blue_green' },
        { label: 'Rolling', value: 'rolling' },
      ],
    };
    const onAnswered = vi.fn();
    render(<PlanInputForm run={run} request={request} onAnswered={onAnswered} />);

    expect(screen.getByRole('button', { name: 'Submit answers' })).toBeDisabled();
    await userEvent.click(screen.getByText('Blue-green'));
    await userEvent.click(screen.getByRole('button', {
      name: 'None of these options fit — answer in additional context',
    }));
    expect(screen.getByLabelText('Blue-green')).not.toBeChecked();
    await userEvent.type(
      screen.getByLabelText('Additional context'),
      'Use a canary rollout with a manual gate instead.',
    );
    await userEvent.click(screen.getByRole('button', { name: 'Submit answers' }));

    await waitFor(() => expect(api.answerPlanInput).toHaveBeenCalledWith(
      71,
      81,
      expect.objectContaining({
        answers: [{ question_id: 'question_0', value: null }],
        response_text: 'Use a canary rollout with a manual gate instead.',
      }),
    ));
    expect(onAnswered).toHaveBeenCalledTimes(1);
  });

  it('clears answers when the InputRequest identity changes', async () => {
    const { rerender } = render(
      <PlanInputForm
        run={run}
        request={requestWithQuestions(1, 81)}
        onAnswered={vi.fn()}
      />,
    );
    await userEvent.type(screen.getAllByRole('textbox')[0], 'answer for A');

    rerender(
      <PlanInputForm
        run={run}
        request={requestWithQuestions(1, 82)}
        onAnswered={vi.fn()}
      />,
    );

    await waitFor(() => expect(screen.getAllByRole('textbox')[0]).toHaveValue(''));
  });
});
