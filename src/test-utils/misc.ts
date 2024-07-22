/**
 * Jest might swallows some logs even with --verbose flag, e.g. on infinite loop.
 * This function will avoid that swallowing behavior.
 */
export const log = (...args: any[]) =>
  args.forEach((a, i) => {
    process.stdout.write(String(a));
    if (i < args.length - 1) {
      process.stdout.write(" ");
    } else {
      process.stdout.write("\n");
    }
  });

/**
 * promise that waits for other timers to resolve
 */
export const awhile = () => new Promise(setImmediate);
