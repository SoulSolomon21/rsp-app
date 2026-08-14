import { Chunk, Effect, Schema, Stream } from "effect"
import * as EventJournalStorage from "../src/EventJournalStorage.js"

import { describe, expect, it } from "@effect/vitest"

class SampleEvent1 extends Schema.TaggedClass<SampleEvent1>()("SampleEvent1", {}) {}

class SampleEvent2 extends Schema.TaggedClass<SampleEvent2>()("SampleEvent2", {}) {}

const EventUnion = [SampleEvent1, SampleEvent2] as const

describe("EventJournalStorage", () => {
  it.effect("Journal should persist then read events", () =>
    Effect.gen(function*() {
      const journal = yield* EventJournalStorage.EventJournalStorage

      yield* journal.append(
        "products",
        "product-1",
        EventUnion,
        new EventJournalStorage.EventJournalStorageEntry({
          createdAt: new Date(0),
          sequence: 1,
          event: new SampleEvent1()
        })
      )
      const events = yield* Stream.runCollect(journal.read("products", "product-1", 0, EventUnion))

      expect(Chunk.size(events)).toEqual(1)
    }).pipe(Effect.provide(EventJournalStorage.inMemory)))

  it.effect("Journal should differentiate entities", () =>
    Effect.gen(function*() {
      const journal = yield* EventJournalStorage.EventJournalStorage

      yield* journal.append(
        "products",
        "product-1",
        EventUnion,
        new EventJournalStorage.EventJournalStorageEntry({
          createdAt: new Date(0),
          sequence: 1,
          event: new SampleEvent2()
        })
      )
      const events = yield* Stream.runCollect(journal.read("products", "product-2", 0, EventUnion))

      expect(Chunk.size(events)).toEqual(0)
    }).pipe(Effect.provide(EventJournalStorage.inMemory)))
})
