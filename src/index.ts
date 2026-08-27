export { LocalDb } from './client/LocalDb.ts';
export { bindModels, jsonBool, ftsName, validateModels } from './sql.ts';
export { queryKey, hashString } from './protocol.ts';
export {
  trigramSimilarity,
  wordSimilarity,
  searchSimilarity,
  registerSimilarityUdf,
} from './similarity.ts';
export type { AnalyzeEntry } from './protocol.ts';
export type {
  LocalDbConfig,
  ModelMeta,
  QuerySpec,
  QueryResult,
  ShapeDef,
  SyncStatus,
  CommitNotice,
  Unsubscribe,
} from './types.ts';
