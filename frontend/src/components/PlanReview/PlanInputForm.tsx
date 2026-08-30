import { useEffect, useMemo, useRef, useState } from 'react';

import { api, isApiRequestError, type PlanInputRequest, type PlanRun } from '../../api/client';
import { useFileUpload } from '../../hooks/useFileUpload';
import { Loader2, Paperclip, X } from '../icons';

interface PlanInputFormProps {
  run: Pick<PlanRun, 'id' | 'generation'>;
  request: Pick<
    PlanInputRequest,
    'id' | 'requested_by' | 'reason' | 'questions'
  >;
  compact?: boolean;
  onAnswered: (answered?: PlanInputRequest) => void | Promise<void>;
}

type AnswerValue = string | string[];

export function PlanInputForm({ run, request, compact = false, onAnswered }: PlanInputFormProps) {
  const [answers, setAnswers] = useState<Record<string, AnswerValue>>({});
  const [additional, setAdditional] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const additionalRef = useRef<HTMLTextAreaElement>(null);
  const uploads = useFileUpload();
  const clearUploads = uploads.clear;
  const answerIdempotencyKey = useMemo(
    () => `${request.id}:${crypto.randomUUID()}`,
    [request.id],
  );

  useEffect(() => {
    setAnswers({});
    setAdditional('');
    setSubmitting(false);
    setError(null);
    clearUploads();
  }, [request.id, clearUploads]);

  const missingRequired = useMemo(
    () => request.questions.some((question) => {
      if (!question.required) return false;
      const value = answers[question.id];
      const missing = value == null || value === '' || (Array.isArray(value) && value.length === 0);
      return missing && !additional.trim();
    }),
    [additional, answers, request.questions],
  );

  const submit = async () => {
    if (submitting || missingRequired || uploads.isUploading || uploads.hasFailed) return;
    setSubmitting(true);
    setError(null);
    try {
      const results = uploads.uploadedResults;
      const answered = await api.answerPlanInput(run.id, request.id, {
        expected_run_generation: run.generation,
        idempotency_key: answerIdempotencyKey,
        answers: request.questions.map((question) => ({
          question_id: question.id,
          value: answers[question.id] ?? null,
        })),
        ...(additional.trim() ? { response_text: additional.trim() } : {}),
        ...(results.length ? {
          file_paths: results.map((item) => item.path),
          image_paths: results.filter((item) => item.is_image).map((item) => item.path),
          attachments: results.map((item) => ({
            url: item.url,
            name: item.filename || item.url.split('/').pop() || 'file',
            is_image: item.is_image,
          })),
        } : {}),
      });
      setAnswers({});
      setAdditional('');
      uploads.clear();
      await onAnswered(answered);
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : String(submitError));
      if (isApiRequestError(submitError) && submitError.status === 409) {
        await onAnswered();
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form
      className={`space-y-4 ${compact ? '' : 'rounded-xl border border-amber-500/25 bg-amber-500/5 p-4'}`}
      onSubmit={(event) => { event.preventDefault(); void submit(); }}
    >
      <fieldset disabled={submitting} className="contents">
      <div>
        <div className="flex flex-wrap items-center gap-2">
          <span className="rounded-full bg-amber-500/15 px-2 py-1 text-[10px] font-semibold uppercase tracking-wide text-amber-300">
            {request.requested_by} needs input
          </span>
          <span className="text-xs text-gray-500">
            {request.questions.length} {request.questions.length === 1 ? 'question' : 'questions'}
          </span>
        </div>
        {request.reason && <p className="mt-2 text-sm leading-6 text-gray-300">{request.reason}</p>}
      </div>

      <div className="max-h-[min(52vh,560px)] space-y-4 overflow-y-auto pr-1">
        {request.questions.map((question, index) => {
          const value = answers[question.id];
          return (
            <fieldset key={question.id} className="rounded-xl border border-gray-700 bg-gray-900/70 p-3.5">
              <legend className="px-1 text-xs font-semibold text-indigo-300">
                {index + 1}. {question.header}{question.required ? ' *' : ''}
              </legend>
              <p className="mb-3 text-sm leading-6 text-gray-200">{question.question}</p>
              {question.response_type === 'text' ? (
                <textarea
                  value={typeof value === 'string' ? value : ''}
                  onChange={(event) => setAnswers((current) => ({ ...current, [question.id]: event.target.value }))}
                  rows={3}
                  maxLength={50000}
                  className="w-full resize-y rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
                />
              ) : (
                <div className="space-y-2">
                  {question.options.map((option) => {
                    const multi = question.response_type === 'multi_choice';
                    const selected = multi
                      ? Array.isArray(value) && value.includes(option.value)
                      : value === option.value;
                    return (
                      <label key={option.value} className={`flex cursor-pointer items-center gap-3 rounded-lg border px-3 py-2 text-sm transition-colors ${selected ? 'border-indigo-500/60 bg-indigo-500/10 text-indigo-300' : 'border-gray-700 text-gray-300 hover:border-gray-600'}`}>
                        <input
                          type={multi ? 'checkbox' : 'radio'}
                          name={`plan-question-${request.id}-${question.id}`}
                          checked={selected}
                          onChange={() => setAnswers((current) => {
                            if (!multi) return { ...current, [question.id]: option.value };
                            const existing = Array.isArray(current[question.id]) ? current[question.id] as string[] : [];
                            return {
                              ...current,
                              [question.id]: selected
                                ? existing.filter((item) => item !== option.value)
                                : [...existing, option.value],
                            };
                          })}
                        />
                        {option.label}
                      </label>
                    );
                  })}
                  {question.response_type === 'single_choice' && (
                    <button
                      type="button"
                      className="w-full rounded-lg border border-dashed border-gray-700 px-3 py-2 text-left text-xs text-gray-400 transition-colors hover:border-indigo-500/50 hover:bg-indigo-500/5 hover:text-gray-200"
                      onClick={() => {
                        setAnswers((current) => {
                          const next = { ...current };
                          delete next[question.id];
                          return next;
                        });
                        additionalRef.current?.focus();
                      }}
                    >
                      None of these options fit — answer in additional context
                    </button>
                  )}
                </div>
              )}
            </fieldset>
          );
        })}
      </div>

      <textarea
        ref={additionalRef}
        value={additional}
        onChange={(event) => setAdditional(event.target.value)}
        aria-label="Additional context"
        placeholder="Additional context (may replace an option when none fit)"
        rows={2}
        maxLength={50000}
        className="w-full resize-y rounded-lg border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 outline-none focus:border-indigo-500"
      />

      {uploads.uploads.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {uploads.uploads.map((upload) => (
            <span key={upload.id} className="flex max-w-full items-center gap-1 rounded-lg border border-gray-700 bg-gray-800 px-2 py-1 text-xs text-gray-300">
              {upload.preview && <img src={upload.preview} alt="" className="h-8 w-8 rounded object-cover" />}
              <span className="max-w-40 truncate">{upload.file?.name || upload.result?.filename || 'file'}</span>
              {upload.status === 'uploading' && <Loader2 size={11} className="animate-spin" />}
              {upload.status === 'failed' && (
                <button type="button" className="rounded px-1 py-0.5 text-red-300 transition-colors hover:bg-red-500/10 hover:text-red-200" onClick={() => uploads.retryFile(upload.id)}>Retry</button>
              )}
              <button type="button" aria-label={`Remove ${upload.file?.name || upload.result?.filename || 'file'}`} onClick={() => uploads.removeFile(upload.id)} className="rounded p-0.5 text-gray-500 transition-colors hover:bg-gray-700 hover:text-gray-200"><X size={11} /></button>
            </span>
          ))}
        </div>
      )}
      {error && <p className="text-xs text-red-400">{error}</p>}
      {missingRequired && additional.trim() === '' && (
        <p className="text-xs text-gray-500">
          Answer each required question, or explain your alternative in Additional context.
        </p>
      )}
      <div className="flex items-center justify-between gap-3">
        <div>
          <input
            ref={inputRef}
            type="file"
            multiple
            className="hidden"
            onChange={(event) => {
              uploads.addFiles(Array.from(event.target.files || []), setError);
              event.target.value = '';
            }}
          />
          <button type="button" onClick={() => inputRef.current?.click()} className="flex items-center gap-1.5 rounded-lg border border-gray-700 px-3 py-2 text-xs text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-800 hover:text-gray-200">
            <Paperclip size={13} /> Attach files
          </button>
        </div>
        <button
          type="submit"
          disabled={submitting || missingRequired || uploads.isUploading || uploads.hasFailed}
          className="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-4 py-2 text-xs font-semibold text-white hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {submitting && <Loader2 size={12} className="animate-spin" />}
          Submit answers
        </button>
      </div>
      </fieldset>
    </form>
  );
}
