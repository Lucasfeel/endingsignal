import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { attachNormalizedMeta, normalizeMeta, type ContentLike } from '../normalizers';

test('normalizeMeta handles nullish, string, and object inputs safely', () => {
  assert.deepEqual(normalizeMeta(null), {});
  assert.deepEqual(normalizeMeta(undefined), {});
  assert.deepEqual(normalizeMeta('{"a":1}'), { a: 1 });
  assert.deepEqual(normalizeMeta('invalid json'), {});
  const obj = { key: 'value' };
  assert.deepEqual(normalizeMeta(obj), obj);
});

test('attachNormalizedMeta normalizes meta fields on arrays and nested payloads', () => {
  const input: ContentLike[] = [
    { content_id: '1', title: 'One', meta: '{"foo":"bar"}', source: 'naver' },
    { content_id: '2', title: 'Two', meta: null, source: 'naver' },
  ];

  const normalizedArray = attachNormalizedMeta(input, 'webtoon') as typeof input;
  assert.deepEqual(normalizedArray[0].meta, { foo: 'bar' });
  assert.deepEqual(normalizedArray[1].meta, {});
  assert.equal(normalizedArray[0].content_type, 'webtoon');

  const nested: { contents: ContentLike[] } = {
    contents: [{ content_id: '3', title: 'Three', meta: '{"bar":1}', source: 'kakao' }],
  };
  const normalizedNested = attachNormalizedMeta(nested, 'novel') as typeof nested;
  assert.deepEqual(normalizedNested.contents[0].meta, { bar: 1 });
  assert.equal(normalizedNested.contents[0].content_type, 'novel');
});
