// pg_trgm-compatible trigram similarity, exposed to SQL as UDFs on every
// connection (see engine.ts). SQLite has no trigram similarity built in;
// these functions reproduce the exact pg_trgm semantics so Postgres-style
// fuzzy predicates (`similarity(a, b) > 0.3`) keep their meaning when a
// query moves to the local database: lowercase both sides, split into
// alphanumeric words, pad each word with two leading + one trailing
// space, collect 3-grams into a set; similarity = |A ∩ B| / |A ∪ B|.

export function trigramSimilarity(a: string, b: string): number {
  return similarityToSet(a, trigramSet(b));
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
  return wordSimilarityToSet(trigramSet(query), countWords(query), text);
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
  return searchSimilarityToSets(wordTrigramSets(query), text);
}

function searchSimilarityToSets(qSets: Array<Set<number>>, text: string): number {
  if (qSets.length === 0) return 0;
  const words = wordTrigramSets(text);
  if (words.length === 0) return 0;
  // Adjacent-pair unions built ONCE per row and shared by every query
  // word (building them per query word would double allocations).
  let pairs: Array<Set<number>> | null = null;
  if (words.length > 1) {
    pairs = [];
    for (let i = 0; i + 1 < words.length; i++) {
      const u = new Set(words[i]!);
      for (const t of words[i + 1]!) u.add(t);
      pairs.push(u);
    }
  }
  let min = 1;
  for (const qw of qSets) {
    let best = 0;
    for (const w of words) {
      const s = jaccard(qw, w);
      if (s > best) best = s;
    }
    if (pairs && best < 1) {
      for (const p of pairs) {
        const s = jaccard(qw, p);
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

function wordSimilarityToSet(q: Set<number>, qWords: number, text: string): number {
  if (q.size === 0) return 0;
  const words = wordTrigramSets(text);
  if (words.length === 0) return 0;
  const maxWin = qWords + 1;
  let best = 0;
  for (let i = 0; i < words.length && best < 1; i++) {
    const acc = new Set(words[i]!);
    best = Math.max(best, jaccard(q, acc));
    const end = Math.min(words.length, i + maxWin);
    for (let j = i + 1; j < end; j++) {
      for (const t of words[j]!) acc.add(t);
      best = Math.max(best, jaccard(q, acc));
    }
  }
  return best;
}

function similarityToSet(a: string, tb: Set<number>): number {
  return tb.size === 0 ? 0 : jaccard(trigramSet(a), tb);
}

function jaccard(a: Set<number>, b: Set<number>): number {
  if (a.size === 0 || b.size === 0) return 0;
  const small = a.size <= b.size ? a : b;
  const big = small === a ? b : a;
  let shared = 0;
  for (const t of small) if (big.has(t)) shared++;
  return shared / (a.size + b.size - shared);
}

// Trigrams are encoded as integers (6 bits per symbol: space=0, 0-9,
// a-z) instead of 3-char strings - a full-catalogue scan builds one of
// these per ROW, and Set<number> membership avoids both the per-trigram
// string allocation and string hashing (measured ~4x on the 50k-row
// scan bench). The trigram DEFINITION is unchanged from pg_trgm:
// lowercase words split on non-alphanumerics, each padded '  w ' -
// non-ASCII acts as a word boundary, matching the string version and
// the PLS importer's copy.
function eachWord(s: string, cb: (lower: string, start: number, len: number) => void): void {
  const lower = s.toLowerCase();
  const n = lower.length;
  let i = 0;
  while (i < n) {
    const start = i;
    while (i < n) {
      const ch = lower.charCodeAt(i);
      const alnum = (ch >= 97 && ch <= 122) || (ch >= 48 && ch <= 57);
      if (!alnum) break;
      i++;
    }
    if (i > start) cb(lower, start, i - start);
    i++;
  }
}

function addWordTrigrams(out: Set<number>, lower: string, start: number, len: number): void {
  // virtual padded word: [sp, sp, w0..w(len-1), sp]
  const code = (j: number): number => {
    if (j < 2 || j >= len + 2) return 0;
    const ch = lower.charCodeAt(start + j - 2);
    return ch >= 97 && ch <= 122 ? ch - 86 : ch - 47;
  };
  let t = (code(0) << 6) | code(1);
  for (let j = 2; j < len + 3; j++) {
    t = ((t << 6) | code(j)) & 0x3ffff;
    out.add(t);
  }
}

function trigramSet(s: string): Set<number> {
  const out = new Set<number>();
  eachWord(s, (lower, start, len) => addWordTrigrams(out, lower, start, len));
  return out;
}

function wordTrigramSets(s: string): Array<Set<number>> {
  const out: Array<Set<number>> = [];
  eachWord(s, (lower, start, len) => {
    const set = new Set<number>();
    addWordTrigrams(set, lower, start, len);
    out.push(set);
  });
  return out;
}

function countWords(s: string): number {
  let n = 0;
  eachWord(s, () => n++);
  return n;
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
  let memoSet: Set<number> | null = null;
  db.createFunction('similarity', {
    arity: 2,
    deterministic: true,
    xFunc: (_ctx: number, a: unknown, b: unknown) => {
      if (a === null || b === null) return null;
      const bs = String(b);
      if (bs !== memoKey) {
        memoKey = bs;
        memoSet = trigramSet(bs);
      }
      return similarityToSet(String(a), memoSet!);
    },
  });
  let wMemoKey: string | null = null;
  let wMemoSet: Set<number> | null = null;
  let wMemoWords = 0;
  db.createFunction('word_similarity', {
    arity: 2,
    deterministic: true,
    xFunc: (_ctx: number, query: unknown, text: unknown) => {
      if (query === null || text === null) return null;
      const qs = String(query);
      if (qs !== wMemoKey) {
        wMemoKey = qs;
        wMemoSet = trigramSet(qs);
        wMemoWords = countWords(qs);
      }
      return wordSimilarityToSet(wMemoSet!, wMemoWords, String(text));
    },
  });
  let sMemoKey: string | null = null;
  let sMemoSets: Array<Set<number>> | null = null;
  db.createFunction('search_similarity', {
    arity: 2,
    deterministic: true,
    xFunc: (_ctx: number, query: unknown, text: unknown) => {
      if (query === null || text === null) return null;
      const qs = String(query);
      if (qs !== sMemoKey) {
        sMemoKey = qs;
        sMemoSets = wordTrigramSets(qs);
      }
      return searchSimilarityToSets(sMemoSets!, String(text));
    },
  });
}
