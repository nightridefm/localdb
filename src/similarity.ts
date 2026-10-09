// pg_trgm-compatible trigram similarity, exposed to SQL as UDFs on every
// connection (see engine.ts). SQLite has no trigram similarity built in;
// these functions reproduce the exact pg_trgm semantics so Postgres-style
// fuzzy predicates (`similarity(a, b) > 0.3`) keep their meaning when a
// query moves to the local database: lowercase both sides, split into
// alphanumeric words, pad each word with two leading + one trailing
// space, collect 3-grams into a set; similarity = |A ∩ B| / |A ∪ B|.

export function trigramSimilarity(a: string, b: string): number {
  return similarityToQuery(a, queryTrigrams(b, adhocQuery()));
}

/**
 * pg_trgm word_similarity-style fuzzy match: the greatest Jaccard between
 * the QUERY's whole trigram set and any contiguous WORD window of `text`
 * (window length capped at query word count + 1 - growing an extent past
 * that only dilutes). This is what search arms actually need: plain
 * whole-string similarity() dilutes a short query against long row text
 * ('vsto' vs 'Vosto - Neon Dreams' scores 0.15 whole-string but 0.375
 * against the word 'vosto'), a ceiling pg_trgm itself had - Postgres
 * ships word_similarity() for exactly this. Word-extent windows are an
 * approximation of pg_trgm's trigram-extent definition (values can
 * differ slightly; same 0..1 range and intent).
 */
export function wordSimilarity(query: string, text: string): number {
  return wordSimilarityToQuery(queryTrigrams(query, adhocQuery()), text);
}

/**
 * The FILTER-arm scorer: AND over query words. Each query word is scored
 * against its best word window of `text` (windows of 1-2 words, so
 * spacing variants like 'nightcall' vs 'Night Call' still match) and the
 * result is the MINIMUM across query words - every word the user typed
 * must fuzzy-match somewhere. This is what makes a search box MONOTONE:
 * adding a word can only shrink the result set. word_similarity's
 * best-extent semantics (kept for ORDER BY ranking) violates that -
 * one matched word of a two-word query scores ~0.45 and PASSES a 0.3
 * threshold, so typing 'vosto neon' after 'vosto' GREW the results with
 * every row containing 'neon'. Not a pg_trgm function; named
 * differently on purpose.
 */
export function searchSimilarity(query: string, text: string): number {
  return searchSimilarityToWords(queryWordTrigrams(query), text);
}

// Trigrams are encoded as integers (6 bits per symbol: space=0, 0-9,
// a-z), so a trigram is an index into a 2^18 table. The trigram
// DEFINITION is pg_trgm's: lowercase words split on non-alphanumerics,
// each padded '  w ' - non-ASCII acts as a word boundary, matching the
// string version and the PLS importer's copy.
//
// These functions run once per ROW of a scan (50k rows per search
// keystroke is normal), so the row side allocates nothing: trigrams go
// into module-level typed arrays, and set semantics (dedupe, union,
// intersection) come from generation stamps instead of Set objects. The
// query side is built once per query string and memoized by the UDFs.
// Nothing here re-enters, so one set of scratch buffers serves every
// function.
const TRIGRAMS = 1 << 18;
const MASK = TRIGRAMS - 1;

/** stamp[t] === gen marks t as a member of the set being built. */
const stamp = new Uint32Array(TRIGRAMS);
let gen = 0;
function nextGen(): number {
  if (++gen === 0xffffffff) {
    stamp.fill(0);
    gen = 1;
  }
  return gen;
}

// Row parse output. rowTri holds each word's trigrams (len + 1 per word,
// duplicates included) back to back; word w spans
// [wordEnd[w - 1] (0 for w = 0), wordEnd[w]).
let rowTri = new Int32Array(256);
let wordEnd = new Int32Array(64);
// Per-word DEDUPED trigrams for search (same layout via uniqEnd), and the
// deduped union of each adjacent word pair (via pairEnd).
let uniqTri = new Int32Array(256);
let uniqEnd = new Int32Array(64);
let pairTri = new Int32Array(512);
let pairEnd = new Int32Array(64);

function growInt32(a: Int32Array<ArrayBuffer>, need: number): Int32Array<ArrayBuffer> {
  if (a.length >= need) return a;
  let n = a.length;
  while (n < need) n *= 2;
  return new Int32Array(n);
}

/** Trigram symbol of a lowercase char code, or -1 for a word boundary. */
function symbol(ch: number): number {
  if (ch >= 97 && ch <= 122) return ch - 86;
  if (ch >= 48 && ch <= 57) return ch - 47;
  return -1;
}

/**
 * Parse `s` into rowTri / wordEnd and return the word count. ASCII is
 * lowercased inline; any non-ASCII char re-runs the parse over
 * toLowerCase(), because Unicode lowercasing can produce ASCII letters
 * (KELVIN SIGN -> 'k', 'İ' -> 'i' + a combining dot) and change the
 * length.
 */
function parseRow(s: string): number {
  const n = s.length;
  for (let i = 0; i < n; i++) {
    if (s.charCodeAt(i) >= 128) return parseLower(s.toLowerCase(), false);
  }
  return parseLower(s, true);
}

function parseLower(s: string, asciiUpper: boolean): number {
  const n = s.length;
  // A word of len chars yields len + 1 trigrams, so n + n/1 bounds it.
  rowTri = growInt32(rowTri, 2 * n + 2);
  wordEnd = growInt32(wordEnd, n + 1);
  let k = 0;
  let words = 0;
  let t = 0;
  let inWord = false;
  for (let i = 0; i < n; i++) {
    let ch = s.charCodeAt(i);
    if (asciiUpper && ch >= 65 && ch <= 90) ch += 32;
    const sym = symbol(ch);
    if (sym < 0) {
      if (inWord) {
        rowTri[k++] = (t << 6) & MASK;
        wordEnd[words++] = k;
        inWord = false;
      }
      continue;
    }
    if (!inWord) {
      inWord = true;
      t = 0;
    }
    t = ((t << 6) | sym) & MASK;
    rowTri[k++] = t;
  }
  if (inWord) {
    rowTri[k++] = (t << 6) & MASK;
    wordEnd[words++] = k;
  }
  return words;
}

/** Dedupe each parsed word into uniqTri / uniqEnd. */
function dedupeWords(words: number): void {
  uniqTri = growInt32(uniqTri, wordEnd[words - 1]!);
  uniqEnd = growInt32(uniqEnd, words);
  let k = 0;
  let from = 0;
  for (let w = 0; w < words; w++) {
    const g = nextGen();
    const to = wordEnd[w]!;
    for (let i = from; i < to; i++) {
      const t = rowTri[i]!;
      if (stamp[t] !== g) {
        stamp[t] = g;
        uniqTri[k++] = t;
      }
    }
    uniqEnd[w] = k;
    from = to;
  }
}

/** Union of each adjacent deduped word pair into pairTri / pairEnd. */
function buildPairs(words: number): void {
  pairTri = growInt32(pairTri, 2 * uniqEnd[words - 1]!);
  pairEnd = growInt32(pairEnd, words);
  let k = 0;
  for (let w = 0; w + 1 < words; w++) {
    const g = nextGen();
    const to = uniqEnd[w + 1]!;
    for (let i = w === 0 ? 0 : uniqEnd[w - 1]!; i < to; i++) {
      const t = uniqTri[i]!;
      if (stamp[t] !== g) {
        stamp[t] = g;
        pairTri[k++] = t;
      }
    }
    pairEnd[w] = k;
  }
}

/**
 * A memoized query trigram set: a membership table for O(1) probes, plus
 * the member list so a query change can clear exactly what it set.
 */
class QueryTrigrams {
  member = new Uint8Array(TRIGRAMS);
  list = new Int32Array(0);
  size = 0;
  words = 0;
}

/** Shared by the direct (non-UDF) entry points; the UDFs memoize their own. */
let adhoc: QueryTrigrams | null = null;
function adhocQuery(): QueryTrigrams {
  return (adhoc ??= new QueryTrigrams());
}

function queryTrigrams(s: string, q: QueryTrigrams): QueryTrigrams {
  for (let i = 0; i < q.size; i++) q.member[q.list[i]!] = 0;
  const words = parseRow(s);
  const n = words ? wordEnd[words - 1]! : 0;
  const list = new Int32Array(n);
  let size = 0;
  for (let i = 0; i < n; i++) {
    const t = rowTri[i]!;
    if (!q.member[t]) {
      q.member[t] = 1;
      list[size++] = t;
    }
  }
  q.list = list;
  q.size = size;
  q.words = words;
  return q;
}

/** Per query word: its deduped trigrams, sorted for binary search. */
function queryWordTrigrams(s: string): Int32Array[] {
  const words = parseRow(s);
  if (!words) return [];
  dedupeWords(words);
  const out: Int32Array[] = [];
  let from = 0;
  for (let w = 0; w < words; w++) {
    out.push(uniqTri.slice(from, uniqEnd[w]!).sort());
    from = uniqEnd[w]!;
  }
  return out;
}

function has(sorted: Int32Array, t: number): boolean {
  let lo = 0;
  let hi = sorted.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const v = sorted[mid]!;
    if (v === t) return true;
    if (v < t) lo = mid + 1;
    else hi = mid - 1;
  }
  return false;
}

/** |qw ∩ buf[from, to)|, where the range is already deduped. */
function sharedWith(qw: Int32Array, buf: Int32Array, from: number, to: number): number {
  let shared = 0;
  for (let i = from; i < to; i++) if (has(qw, buf[i]!)) shared++;
  return shared;
}

function similarityToQuery(a: string, q: QueryTrigrams): number {
  if (q.size === 0) return 0;
  const words = parseRow(a);
  if (!words) return 0;
  const g = nextGen();
  const n = wordEnd[words - 1]!;
  let size = 0;
  let shared = 0;
  for (let i = 0; i < n; i++) {
    const t = rowTri[i]!;
    if (stamp[t] !== g) {
      stamp[t] = g;
      size++;
      if (q.member[t]) shared++;
    }
  }
  return shared / (q.size + size - shared);
}

function wordSimilarityToQuery(q: QueryTrigrams, text: string): number {
  if (q.size === 0) return 0;
  const words = parseRow(text);
  if (!words) return 0;
  const maxWin = q.words + 1;
  let best = 0;
  for (let i = 0; i < words && best < 1; i++) {
    // The window [i, j] grows one word at a time; its union is tracked
    // incrementally under one generation.
    const g = nextGen();
    let size = 0;
    let shared = 0;
    const end = Math.min(words, i + maxWin);
    for (let j = i; j < end; j++) {
      const to = wordEnd[j]!;
      for (let x = j === 0 ? 0 : wordEnd[j - 1]!; x < to; x++) {
        const t = rowTri[x]!;
        if (stamp[t] !== g) {
          stamp[t] = g;
          size++;
          if (q.member[t]) shared++;
        }
      }
      best = Math.max(best, shared / (q.size + size - shared));
    }
  }
  return best;
}

function searchSimilarityToWords(qWords: Int32Array[], text: string): number {
  if (qWords.length === 0) return 0;
  const words = parseRow(text);
  if (!words) return 0;
  dedupeWords(words);
  // Adjacent-pair unions built ONCE per row and shared by every query word.
  if (words > 1) buildPairs(words);
  let min = 1;
  for (const qw of qWords) {
    let best = 0;
    for (let w = 0; w < words; w++) {
      const from = w === 0 ? 0 : uniqEnd[w - 1]!;
      const to = uniqEnd[w]!;
      const shared = sharedWith(qw, uniqTri, from, to);
      const s = shared / (qw.length + (to - from) - shared);
      if (s > best) best = s;
    }
    if (words > 1 && best < 1) {
      for (let p = 0; p + 1 < words; p++) {
        const from = p === 0 ? 0 : pairEnd[p - 1]!;
        const to = pairEnd[p]!;
        const shared = sharedWith(qw, pairTri, from, to);
        const s = shared / (qw.length + (to - from) - shared);
        if (s > best) best = s;
      }
    }
    if (best < min) {
      min = best;
      if (min === 0) return 0;
    }
  }
  return min;
}

/**
 * Register the trigram UDFs on a connection. pg_trgm returns NULL when
 * either argument is NULL - preserved for both functions so `> 0.3`
 * predicates filter NULL rows exactly as the Postgres originals did.
 *
 * - `similarity(a, b)` - whole-string Jaccard, exact pg_trgm parity.
 *   Right for name-vs-name comparisons of comparable length.
 * - `word_similarity(query, text)` - pg_trgm argument order; best
 *   window match, immune to long-row-text dilution. Used for ORDER BY
 *   ranking (a score for the best part of the row).
 * - `search_similarity(query, text)` - AND over query words (min of
 *   per-word best-window scores). What every FILTER arm uses: adding a
 *   query word can only narrow the result set.
 *
 * The QUERY argument (arg 1 of similarity, arg 0 of word_similarity) is
 * constant across a scan - its trigram set (+ word count) is memoized
 * per function, single entry keyed on the exact string, so a 50k-row
 * scan builds it once instead of 50k times.
 */
export function registerSimilarityUdf(db: {
  createFunction: (name: string, opts: object) => unknown;
}): void {
  let memoKey: string | null = null;
  const memo = new QueryTrigrams();
  db.createFunction('similarity', {
    arity: 2,
    deterministic: true,
    xFunc: (_ctx: number, a: unknown, b: unknown) => {
      if (a === null || b === null) return null;
      const bs = String(b);
      if (bs !== memoKey) {
        memoKey = bs;
        queryTrigrams(bs, memo);
      }
      return similarityToQuery(String(a), memo);
    },
  });
  let wMemoKey: string | null = null;
  const wMemo = new QueryTrigrams();
  db.createFunction('word_similarity', {
    arity: 2,
    deterministic: true,
    xFunc: (_ctx: number, query: unknown, text: unknown) => {
      if (query === null || text === null) return null;
      const qs = String(query);
      if (qs !== wMemoKey) {
        wMemoKey = qs;
        queryTrigrams(qs, wMemo);
      }
      return wordSimilarityToQuery(wMemo, String(text));
    },
  });
  let sMemoKey: string | null = null;
  let sMemoWords: Int32Array[] = [];
  db.createFunction('search_similarity', {
    arity: 2,
    deterministic: true,
    xFunc: (_ctx: number, query: unknown, text: unknown) => {
      if (query === null || text === null) return null;
      const qs = String(query);
      if (qs !== sMemoKey) {
        sMemoKey = qs;
        sMemoWords = queryWordTrigrams(qs);
      }
      return searchSimilarityToWords(sMemoWords, String(text));
    },
  });
}
