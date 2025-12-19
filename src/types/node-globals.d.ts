declare module 'node:test' {
  export const test: (name: string, fn: () => Promise<void> | void) => void;
}

declare module 'node:assert' {
  export const strict: {
    equal(actual: unknown, expected: unknown): void;
    deepEqual(actual: unknown, expected: unknown): void;
    rejects(
      block: Promise<unknown>,
      error?: ((err: unknown) => boolean) | RegExp,
    ): Promise<void>;
  };
}
