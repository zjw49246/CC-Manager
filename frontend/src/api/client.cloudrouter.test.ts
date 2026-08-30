import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from './client';

describe('API account compatibility routing', () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockResolvedValue({
      status: 200,
      ok: true,
      headers: { get: () => null },
      json: async () => ({ id: 'api/account 1', name: 'CloudRouter' }),
    });
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it('lists and creates API accounts through the shared endpoint', async () => {
    await api.getCloudRouterAccounts(true);
    await api.createCloudRouterAccount({
      name: 'CloudRouter Claude',
      api_key: 'cr-secret',
      api_provider: 'cloudrouter',
    });

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      '/api/cloudrouter/accounts?force=true',
      '/api/cloudrouter/accounts',
    ]);
    expect(fetchMock.mock.calls[1][1]).toMatchObject({
      method: 'POST',
      body: JSON.stringify({
        name: 'CloudRouter Claude',
        api_key: 'cr-secret',
        api_provider: 'cloudrouter',
      }),
    });
  });

  it('creates an Apex account through the same compatibility endpoint', async () => {
    await api.createCloudRouterAccount({
      name: 'Apex Codex',
      api_key: 'lck_test_only_not_real',
      api_provider: 'apex',
    });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/cloudrouter/accounts',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({
          name: 'Apex Codex',
          api_key: 'lck_test_only_not_real',
          api_provider: 'apex',
        }),
      }),
    );
  });

  it('encodes account ids for refresh and safe retirement', async () => {
    await api.refreshCloudRouterAccount('api/account 1');
    await api.deleteCloudRouterAccount('api/account 1');

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      '/api/cloudrouter/accounts/api%2Faccount%201/refresh',
      '/api/cloudrouter/accounts/api%2Faccount%201',
    ]);
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: 'POST' });
    expect(fetchMock.mock.calls[1][1]).toMatchObject({ method: 'DELETE' });
  });

  it('preserves the HTTP status and structured detail for a busy retirement', async () => {
    fetchMock.mockResolvedValueOnce({
      status: 409,
      statusText: 'Conflict',
      ok: false,
      headers: { get: () => null },
      json: async () => ({
        detail: {
          message: 'API account is still in use',
          error: 'API account is still in use',
          code: 'runtime_busy',
          reason: 'Task #42 is still using this API account',
          cleanup_pending: true,
        },
      }),
    });

    await expect(api.deleteCloudRouterAccount('cloudrouter-1')).rejects.toMatchObject({
      message: 'API account is still in use',
      status: 409,
      detail: {
        message: 'API account is still in use',
        error: 'API account is still in use',
        code: 'runtime_busy',
        reason: 'Task #42 is still using this API account',
        cleanup_pending: true,
      },
    });
  });
});
