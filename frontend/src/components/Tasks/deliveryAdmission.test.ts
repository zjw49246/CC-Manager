import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  acknowledgeDeliveryAdmission,
  acknowledgeDeliveryQuickStart,
  prepareDeliveryAdmission,
  prepareDeliveryQuickStart,
} from './deliveryAdmission';

const draft = {
  project_id: 1,
  monitored_repo_id: 2,
  title: 'Ship it',
  requirements: 'Implement and test it.',
  provider: 'codex' as const,
};

describe('Delivery admission persistence', () => {
  beforeEach(() => {
    sessionStorage.clear();
    vi.restoreAllMocks();
  });

  it('reuses the key for the same canonical request after a reload boundary', () => {
    const first = prepareDeliveryAdmission('task-form', draft);
    const replay = prepareDeliveryAdmission('task-form', {
      requirements: draft.requirements,
      title: draft.title,
      monitored_repo_id: draft.monitored_repo_id,
      project_id: draft.project_id,
      provider: 'codex',
    });

    expect(replay.idempotency_key).toBe(first.idempotency_key);
  });

  it('rotates the key when caller intent changes', () => {
    const first = prepareDeliveryAdmission('task-form', draft);
    const changed = prepareDeliveryAdmission('task-form', {
      ...draft,
      requirements: 'A different task.',
    });

    expect(changed.idempotency_key).not.toBe(first.idempotency_key);
  });

  it('rotates the key when the Delivery provider changes', () => {
    const first = prepareDeliveryAdmission('task-form-provider', draft);
    const changed = prepareDeliveryAdmission('task-form-provider', {
      ...draft,
      provider: 'claude',
    });

    expect(changed.idempotency_key).not.toBe(first.idempotency_key);
  });

  it('clears the key only after the exact request is acknowledged', () => {
    const first = prepareDeliveryAdmission('task-form', draft);
    acknowledgeDeliveryAdmission('task-form', {
      ...first,
      requirements: 'Not the acknowledged payload.',
    });
    expect(
      prepareDeliveryAdmission('task-form', draft).idempotency_key,
    ).toBe(first.idempotency_key);

    acknowledgeDeliveryAdmission('task-form', first);
    expect(
      prepareDeliveryAdmission('task-form', draft).idempotency_key,
    ).not.toBe(first.idempotency_key);
  });

  it('keeps same-page retries idempotent when browser storage is unavailable', () => {
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('storage disabled');
    });

    const first = prepareDeliveryAdmission('storage-blocked', draft);
    const retry = prepareDeliveryAdmission('storage-blocked', draft);

    expect(retry.idempotency_key).toBe(first.idempotency_key);
  });

  it('freezes the quick-start automatic merge choice into idempotency', () => {
    const manual = prepareDeliveryQuickStart('quick-start', {
      project_id: 1,
      requirements: 'Ship it.',
      auto_merge: false,
    });
    const automatic = prepareDeliveryQuickStart('quick-start', {
      project_id: 1,
      requirements: 'Ship it.',
      auto_merge: true,
    });

    expect(automatic.idempotency_key).not.toBe(manual.idempotency_key);
    acknowledgeDeliveryQuickStart('quick-start', automatic);
    expect(prepareDeliveryQuickStart('quick-start', {
      project_id: 1,
      requirements: 'Ship it.',
      auto_merge: true,
    }).idempotency_key).not.toBe(automatic.idempotency_key);
  });
});
