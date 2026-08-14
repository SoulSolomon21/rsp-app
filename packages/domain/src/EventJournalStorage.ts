import { Array, Context, Effect, Layer, Option, pipe, Stream } from "effect"
import * as HashMap from "effect/HashMap"
import * as Ref from "effect/Ref"
import type * as Schema from "effect/Schema"
import type * as AggregateId from "./AggregateId.js"

export interface EventJournalStorageEntry<Event> {
  sequence: number
  event: Event
}

export class EventJournalStorage extends Context.Tag("@@EventJournalStorage")<EventJournalStorage, {
  /**
   * This function allows to append new events into the journal.
   * We expect to receive the expected sequence number in order to check for race conditions
   */
  append<Events extends ReadonlyArray<Schema.Schema.All>>(
    aggregateRootName: string,
    aggregateId: AggregateId.AggregateId,
    expectedSequence: number,
    schema: Events,
    event: Schema.Schema.Type<Events[number]>
  ): Effect.Effect<void>
  /**
   * This function returns a stream of the persisted events into the journal.
   */
  read<Events extends ReadonlyArray<Schema.Schema.All>>(
    aggregateRootName: string,
    aggregateId: AggregateId.AggregateId,
    fromSequence: number,
    eventSchema: Events
  ): Stream.Stream<EventJournalStorageEntry<Schema.Schema.Type<Events[number]>>>
}>() {}

export const inMemory = Effect.gen(function*() {
  const state = yield* Ref.make<
    HashMap.HashMap<string, HashMap.HashMap<AggregateId.AggregateId, Array<any>>>
  >(
    HashMap.empty()
  )

  const append: EventJournalStorage["Type"]["append"] = (aggregateRootName, aggregateId, expectedSequence, _, event) =>
    Ref.update(
      state,
      (oldState) =>
        HashMap.modifyAt(oldState, aggregateRootName, (maybeState) =>
          pipe(
            maybeState,
            Option.getOrElse(() => HashMap.empty<AggregateId.AggregateId, Array<any>>()),
            HashMap.modifyAt(aggregateId, (maybeEventList) =>
              pipe(
                maybeEventList,
                Option.getOrElse(() => []),
                Array.append(event),
                Option.some
              )),
            Option.some
          ))
    )

  const read: EventJournalStorage["Type"]["read"] = (aggregateRootName, aggregateId, __, _) =>
    pipe(
      Ref.get(state),
      Effect.map((stateValue) =>
        pipe(
          stateValue,
          HashMap.get(aggregateRootName),
          Option.getOrElse(() => HashMap.empty<string, Array<any>>()),
          HashMap.get(aggregateId),
          Option.getOrElse(() => [] as Array<any>),
          Array.map((event, sequence) => ({
            event,
            sequence
          })),
          Stream.fromIterable
        )
      ),
      Stream.unwrap
    )

  return { append, read }
}).pipe(Layer.effect(EventJournalStorage))
