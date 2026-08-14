import type { Schema } from "effect"
import { Effect, pipe, Ref, Stream } from "effect"
import * as Clock from "effect/Clock"
import type * as AggregateId from "./AggregateId.js"
import type * as AggregateRoot from "./AggregateRoot.js"
import * as EventJournalStorage from "./EventJournalStorage.js"
import type * as Message from "./Message.js"

export interface EventSourcedAggregate<
  Aggregate extends AggregateRoot.AggregateRoot.All,
  Events extends ReadonlyArray<Message.Message.AnyForAggregate<Aggregate>>,
  State extends Schema.Schema.All
> {
  produce: EventSourcedAggregateProducer<Schema.Schema.Type<Events[number]>, Schema.Schema.Type<State>>
}

interface EventSourcedAggregateDraft<Events, State> {
  read: Effect.Effect<State>
  append: (event: Events) => Effect.Effect<void>
}

interface EventSourcedAggregateProducer<Events, State> {
  (
    aggregateId: AggregateId.AggregateId,
    fn: (draft: EventSourcedAggregateDraft<Events, State>) => Effect.Effect<void>
  ): Effect.Effect<void, never, EventJournalStorage.EventJournalStorage>
}

interface EventSourcedAggregateArgs<
  Aggregate extends AggregateRoot.AggregateRoot.All,
  Events extends ReadonlyArray<Message.Message.AnyForAggregate<Aggregate>>,
  State extends Schema.Schema.All
> {
  aggregateRoot: Aggregate
  eventTypes: Events
  state: State
  initialState: (aggregateId: AggregateId.AggregateId) => Schema.Schema.Type<State>
  reduce: (
    state: Schema.Schema.Type<State>,
    event: EventJournalStorage.EventJournalStorageEntry<Schema.Schema.Type<Events[number]>>
  ) => Schema.Schema.Type<State>
}

export function make<
  Aggregate extends AggregateRoot.AggregateRoot.All,
  Events extends ReadonlyArray<Message.Message.AnyForAggregate<Aggregate>>,
  State extends Schema.Schema.All
>(
  args: EventSourcedAggregateArgs<Aggregate, Events, State>
): EventSourcedAggregate<Aggregate, Events, State> {
  const readCurrentStateFromJournal = (aggregateId: AggregateId.AggregateId) =>
    pipe(
      EventJournalStorage.EventJournalStorage,
      Effect.map((journal) => journal.read(args.aggregateRoot.aggregateRootName, aggregateId, 0, args.eventTypes)),
      Stream.unwrap,
      Stream.runFold(
        { currentState: args.initialState(aggregateId), lastSequence: 0 },
        ({ currentState: state }, journalEntry) => ({
          currentState: args.reduce(state, journalEntry),
          lastSequence: journalEntry.sequence
        })
      )
    )

  const produce: EventSourcedAggregateProducer<Schema.Schema.Type<Events[number]>, Schema.Schema.Type<State>> = (
    aggregateId,
    draft
  ) =>
    Effect.gen(function*() {
      const { currentState, lastSequence } = yield* readCurrentStateFromJournal(aggregateId)
      const stateRef = yield* Ref.make(currentState)
      const sequenceRef = yield* Ref.make(lastSequence)
      const journal = yield* EventJournalStorage.EventJournalStorage
      const read = Ref.get(stateRef)
      const append = (event: Schema.Schema.Type<Events[number]>) =>
        Effect.gen(function*() {
          const sequence = yield* Ref.updateAndGet(sequenceRef, (n) => n + 1)
          const createdAt = new Date(yield* Clock.currentTimeMillis)
          const journalEntry = new EventJournalStorage.EventJournalStorageEntry({
            event,
            sequence,
            createdAt
          })
          yield* journal.append(
            args.aggregateRoot.aggregateRootName,
            aggregateId,
            args.eventTypes,
            journalEntry
          )
          yield* Ref.update(stateRef, (currentState) => args.reduce(currentState, journalEntry))
        })

      yield* draft({ read, append })
    })

  return { produce }
}
