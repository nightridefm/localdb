// Shared deterministic data generator for the bench harnesses.
export function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export const WORDS = (
  'neon synth rider night drive chrome sunset grid laser retro wave dark city ' +
  'pulse dream machine ghost signal velvet storm turbo phantom electric glass ' +
  'mirror steel horizon runner blade vector crystal echo fury shadow zenith ' +
  'aurora binary cascade digital ember flux gravity hologram infrared jade ' +
  'kinetic lunar matrix nova orbit prism quantum raster stellar tension ultra ' +
  'vortex wire xenon yield zero alpha beta gamma delta omega circuit diode ' +
  'triode voltage current plasma fusion reactor engine piston nitro boost'
).split(/\s+/);

export function makeRowGen(seed) {
  const rand = mulberry32(seed);
  const baseTs = Date.parse('2020-01-01T00:00:00Z');
  return function row(i) {
    const w1 = WORDS[(rand() * WORDS.length) | 0];
    const w2 = WORDS[(rand() * WORDS.length) | 0];
    const w3 = rand() < 0.4 ? ' ' + WORDS[(rand() * WORDS.length) | 0] : '';
    return {
      id: i.toString(16).padStart(32, '0'),
      title: `${w1} ${w2}${w3}`,
      artist: 'artist_' + ((rand() * 5000) | 0),
      duration: 120 + rand() * 300,
      plays: (rand() * 10000) | 0,
      created_at: new Date(baseTs + i * 1000).toISOString(),
    };
  };
}

export function makeLinkGen(seed, uploadCount) {
  const rand = mulberry32(seed);
  return function link(i) {
    return {
      playlist_id: 1 + (i % 1000),
      upload_id: ((rand() * uploadCount) | 0).toString(16).padStart(32, '0'),
      position: i,
    };
  };
}

export function median(arr) {
  const s = [...arr].sort((x, y) => x - y);
  return s[s.length >> 1];
}
