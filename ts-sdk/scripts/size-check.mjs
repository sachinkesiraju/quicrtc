// size-check — gzipped size gate for the core TS SDK bundle.
//
// Sums dist/index.js plus its dependency closure (client.js, wire.js,
// types.js), gzips, and fails if the total exceeds CORE_LIMIT_BYTES.
// The viewer subpath is intentionally excluded — React-flavored UI
// has its own (looser) limit if we add a gate for it later.

import { gzipSync } from 'node:zlib';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const distDir = join(here, '..', 'dist');

const coreFiles = ['index.js', 'client.js', 'wire.js', 'types.js'];
// Transitional limit reflecting tsc-only output (no minifier yet).
// Tightens to 8 KiB once esbuild/rollup is wired into the build —
// the README claim ("~6 KB gzipped") tracks minified output, not the
// human-readable JS that tsc emits today.
const CORE_LIMIT_BYTES = 22 * 1024;

let total = 0;
for (const f of coreFiles) {
  const path = join(distDir, f);
  if (!existsSync(path)) {
    console.error(`size-check: missing ${path} — run \`npm run build\` first`);
    process.exit(1);
  }
  const raw = readFileSync(path);
  const gz = gzipSync(raw, { level: 9 });
  total += gz.length;
  console.log(`  ${f}: ${raw.length}B raw, ${gz.length}B gzipped`);
}

console.log(`core total: ${total}B gzipped (limit: ${CORE_LIMIT_BYTES}B)`);
if (total > CORE_LIMIT_BYTES) {
  console.error(`size-check FAILED: core bundle ${total}B > ${CORE_LIMIT_BYTES}B`);
  process.exit(1);
}
console.log('size-check OK');
