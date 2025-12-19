import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { ApiError, request } from '../client';

test('request surfaces structured API errors', async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () =>
      new Response(JSON.stringify({ success: false, error: { code: 'TOKEN_EXPIRED', message: 'Session expired' } }), {
        status: 401,
        headers: { 'content-type': 'application/json' },
      });

    await assert.rejects(
      request('GET', '/api/auth/me', { auth: { token: 'abc' } }),
      (error: unknown) => {
        const apiError = error as ApiError;
        assert.equal(apiError.httpStatus, 401);
        assert.equal(apiError.code, 'TOKEN_EXPIRED');
        assert.equal(apiError.message, 'Session expired');
        return true;
      },
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('request falls back to status text when parsing fails', async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () => new Response('bad', { status: 500, statusText: 'Server Error' });

    await assert.rejects(
      request('GET', '/api/status'),
      (error: unknown) => {
        const apiError = error as ApiError;
        assert.equal(apiError.httpStatus, 500);
        assert.equal(apiError.message, 'bad');
        return true;
      },
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('request returns parsed JSON on success without wrappers', async () => {
  const payload = { value: 123 };
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () =>
      new Response(JSON.stringify(payload), { status: 200, headers: { 'content-type': 'application/json' } });

    const result = await request<{ value: number }>('GET', '/api/status');
    assert.deepEqual(result, payload);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
