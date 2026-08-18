import { Chunk, Effect, Schema, Stream } from 'effect';
import * as EventJournalStorage from '../src/EventJournalStorage.js';
import * as SqlliteClientNode from '@effect/sql-sqlite-node';

import { describe, expect, it } from '@effect/vitest';
import { SqlClient } from '@effect/sql';

class SampleEvent1 extends Schema.TaggedClass<SampleEvent1>()(
  'SampleEvent1',
  {},
) {}

class SampleEvent2 extends Schema.TaggedClass<SampleEvent2>()(
  'SampleEvent2',
  {},
) {}

const EventUnion = [SampleEvent1, SampleEvent2] as const;

describe('EventJournalStorage', () => {
  it.effect('Journal should persist then read events', () =>
    Effect.gen(function* () {
      const journal = yield* EventJournalStorage.EventJournalStorage;

      yield* journal.append(
        'products',
        'product-1',
        EventUnion,
        new EventJournalStorage.EventJournalStorageEntry({
          createdAt: new Date(0),
          sequence: 1,
          event: new SampleEvent1(),
        }),
      );
      const events = yield* Stream.runCollect(
        journal.read('products', 'product-1', 0, EventUnion),
      );

      expect(Chunk.size(events)).toEqual(1);
    }).pipe(Effect.provide(EventJournalStorage.inMemory)),
  );

  it.effect('Journal should differentiate entities', () =>
    Effect.gen(function* () {
      const journal = yield* EventJournalStorage.EventJournalStorage;

      yield* journal.append(
        'products',
        'product-1',
        EventUnion,
        new EventJournalStorage.EventJournalStorageEntry({
          createdAt: new Date(0),
          sequence: 1,
          event: new SampleEvent2(),
        }),
      );
      const events = yield* Stream.runCollect(
        journal.read('products', 'product-2', 0, EventUnion),
      );

      expect(Chunk.size(events)).toEqual(0);
    }).pipe(Effect.provide(EventJournalStorage.inMemory)),
  );

  it.effect('Creates sqllite events', () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DELETE FROM event_journal`;
      const journal = yield* EventJournalStorage.EventJournalStorage;

      yield* journal.append(
        'products',
        'product-1',
        EventUnion,
        new EventJournalStorage.EventJournalStorageEntry({
          createdAt: new Date(0),
          sequence: 1,
          event: new SampleEvent1(),
        }),
      );
      const events = yield* Stream.runCollect(
        journal.read('products', 'product-1', 0, EventUnion),
      );

      expect(Chunk.size(events)).toEqual(1);
    }).pipe(
      Effect.provide(
        EventJournalStorage.sqlLite({ journalTableName: 'event_journal' }),
      ),
      Effect.provide(
        SqlliteClientNode.SqliteClient.layer({ filename: 'test.db' }),
      ),
    ),
  );
});
