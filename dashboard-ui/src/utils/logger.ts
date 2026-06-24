/**
 * Application logger that is silenced in production builds.
 *
 * Why not bare console.log?
 * - console.log leaks implementation details in production bundles.
 * - This wrapper lets us strip all debug output by checking import.meta.env.DEV
 *   without hunting for every console.log call before a release.
 * - All calls are prefixed with [ArbitrageUI] for easy filtering in DevTools.
 */
const PREFIX = '[ArbitrageUI]';

export const logger = {
  /** Debug-level — only emitted in development mode. */
  debug: (...args: unknown[]): void => {
    if (import.meta.env.DEV) {
      console.debug(PREFIX, ...args);
    }
  },

  /** Info-level — only emitted in development mode. */
  info: (...args: unknown[]): void => {
    if (import.meta.env.DEV) {
      console.info(PREFIX, ...args);
    }
  },

  /** Warning-level — always emitted (visible in production). */
  warn: (...args: unknown[]): void => {
    console.warn(PREFIX, ...args);
  },

  /** Error-level — always emitted (visible in production). */
  error: (...args: unknown[]): void => {
    console.error(PREFIX, ...args);
  },
};
