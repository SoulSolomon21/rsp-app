import type { SqlError } from '@effect/sql';
import { SqlClient } from '@effect/sql';
import { Array, Context, Effect, Layer, Option, pipe } from 'effect';
import * as Data from 'effect/Data';
import * as HashMap from 'effect/HashMap';
import * as Ref from 'effect/Ref';
import * as Schema from 'effect/Schema';
import * as Stream from 'effect/Stream';
import type * as AggregateId from './AggregateId.js';

export class EventJournalStorageEntry<Event> extends Data.Class<{
  readonly createdAt: Date;
  readonly sequence: number;
  readonly event: Event;
}> {}

export class EventJournalAppendEntryFailed extends Data.TaggedError(
  'EventJournalAppendEntryFailed',
)<{
  sequence: number;
}> {}

export class EventJournalStorage extends Context.Tag('@@EventJournalStorage')<
  EventJournalStorage,
  {
    /**
     * This function allows to append new events into the journal.
     * We expect to receive the expected sequence number in order to check for race conditions
     */
    append<Events extends ReadonlyArray<Schema.Schema.AnyNoContext>>(
      aggregateRootName: string,
      aggregateId: AggregateId.AggregateId,
      schema: Events,
      entry: EventJournalStorageEntry<Schema.Schema.Type<Events[number]>>,
    ): Effect.Effect<void, EventJournalAppendEntryFailed>;
    /**
     * This function returns a stream of the persisted events into the journal.
     */
    read<Events extends ReadonlyArray<Schema.Schema.AnyNoContext>>(
      aggregateRootName: string,
      aggregateId: AggregateId.AggregateId,
      fromSequence: number,
      eventSchema: Events,
    ): Stream.Stream<
      EventJournalStorageEntry<Schema.Schema.Type<Events[number]>>
    >;
  }
>() {}

export const inMemory = Effect.gen(function* () {
  const state = yield* Ref.make<
    HashMap.HashMap<
      string,
      HashMap.HashMap<AggregateId.AggregateId, Array<any>>
    >
  >(HashMap.empty());

  const append: EventJournalStorage['Type']['append'] = (
    aggregateRootName,
    aggregateId,
    _,
    journalEntry,
  ) =>
    Ref.update(state, (oldState) =>
      HashMap.modifyAt(oldState, aggregateRootName, (maybeState) =>
        pipe(
          maybeState,
          Option.getOrElse(() =>
            HashMap.empty<AggregateId.AggregateId, Array<any>>(),
          ),
          HashMap.modifyAt(aggregateId, (maybeEventList) =>
            pipe(
              maybeEventList,
              Option.getOrElse(() => []),
              Array.append(journalEntry),
              Option.some,
            ),
          ),
          Option.some,
        ),
      ),
    );

  const read: EventJournalStorage['Type']['read'] = (
    aggregateRootName,
    aggregateId,
    fromSequence,
    _,
  ) =>
    pipe(
      Ref.get(state),
      Effect.map((stateValue) =>
        pipe(
          stateValue,
          HashMap.get(aggregateRootName),
          Option.getOrElse(() => HashMap.empty<string, Array<any>>()),
          HashMap.get(aggregateId),
          Option.getOrElse(() => [] as Array<any>),
          Array.filter(
            (entry: EventJournalStorageEntry<any>) =>
              entry.sequence >= fromSequence,
          ),
          Stream.fromIterable,
        ),
      ),
      Stream.unwrap,
    );

  return { append, read };
}).pipe(Layer.effect(EventJournalStorage));

function makeEventUnionSchema<
  Events extends ReadonlyArray<Schema.Schema.AnyNoContext>,
>(
  events: Events,
): Schema.Schema<
  Schema.Schema.Type<Events[number]>,
  Schema.Schema.Encoded<Events[number]>,
  never
> {
  return Schema.Union<Events>(...events) as any;
}

interface EventJournalSqlliteRow {
  aggregate_root: string;
  aggregate_id: string;
  sequence: number;
  event_payload: string;
}

export interface EventJournalSqlliteMakeArgs {
  journalTableName: string;
}

export const sqlLite = (args: EventJournalSqlliteMakeArgs) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    // create the events table if not exists
    yield* sql`CREATE TABLE IF NOT EXISTS ${sql.literal(args.journalTableName)} (
    aggregate_root TEXT NOT NULL, 
    aggregate_id TEXT NOT NULL, 
    sequence INT NOT NULL, 
    event_payload TEXT,
    PRIMARY KEY(aggregate_root, aggregate_id, sequence)
  )`;

    const append: EventJournalStorage['Type']['append'] = (
      aggregateRootName,
      aggregateId,
      schemas,
      journalEntry,
    ) =>
      Effect.gen(function* () {
        const event_payload = yield* Schema.encode(
          Schema.parseJson(makeEventUnionSchema(schemas)),
        )(journalEntry.event);

        yield* sql`INSERT INTO ${sql.literal(args.journalTableName)}(aggregate_root, aggregate_id, sequence, event_payload) VALUES (${aggregateRootName}, ${aggregateId}, ${journalEntry.sequence}, ${event_payload});`;
      }).pipe(
        Effect.catchTag(
          'SqlError',
          (_) =>
            new EventJournalAppendEntryFailed({
              sequence: journalEntry.sequence,
            }),
        ),
        Effect.orDie,
      );

    const read: EventJournalStorage['Type']['read'] = (
      aggregateRootName,
      aggregateId,
      _,
      schemas,
    ) =>
      Effect.gen(function* () {
        const raw_data =
          yield* sql`SELECT * FROM ${sql.literal(args.journalTableName)} WHERE aggregate_root = ${aggregateRootName} AND aggregate_id = ${aggregateId} ORDER BY sequence ASC`
            .raw as Effect.Effect<
            Array<EventJournalSqlliteRow>,
            SqlError.SqlError
          >;

        return Stream.fromIterable(raw_data).pipe(
          Stream.mapEffect((row) =>
            Schema.decode(Schema.parseJson(makeEventUnionSchema(schemas)))(
              row.event_payload,
            ),
          ),
        );
      }).pipe(Stream.unwrap, Stream.orDie);

    return { append, read };
  }).pipe(Layer.effect(EventJournalStorage));
