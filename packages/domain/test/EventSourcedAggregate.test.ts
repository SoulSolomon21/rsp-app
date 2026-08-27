import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as AggregateRoot from "../src/AggregateRoot.js";
import * as EventSourcedAggregate from "../src/EventSourcedAggregate.js";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as EventJournalStorage from "../src/EventJournalStorage.js";
import * as MessageHeaders from "../src/MessageHeaders.js"
import * as Stream from "effect/Stream";

export const ProductAggregate = AggregateRoot.AggregateRoot({
  aggregateRootName: "products"
})

export class ProductCreated extends Schema.TaggedClass<ProductCreated>()(
  "ProductCreated",
  ProductAggregate.Event({
    name: Schema.NonEmptyString,
    uom: Schema.NonEmptyString
  })
) { }

export class ProductNameChanged extends Schema.TaggedClass<ProductNameChanged>()(
  "ProductNameChanged",
  ProductAggregate.Event({
    newName: Schema.NonEmptyString,
    oldName: Schema.NonEmptyString
  })
) { }

const ProductEventJournal = EventSourcedAggregate.make({
  aggregateRoot: ProductAggregate,
  eventTypes: [ProductCreated, ProductNameChanged],
  state: Schema.Option(Schema.Struct({
    productName: Schema.NonEmptyString,
    uom: Schema.NonEmptyString,
    createdAt: Schema.Date,
    updatedAt: Schema.Date,
  })),
  reduce: (state, journalEntry) => {
    const product = Option.flatten(state)
    const event = journalEntry.event
    switch (event._tag) {
      case "ProductCreated":
        return Option.some({
          productName: event.name,
          uom: event.uom,
          createdAt: journalEntry.createdAt,
          updatedAt: journalEntry.createdAt
        });
      case "ProductNameChanged":
        return Option.map(product, (oldProduct) => ({ ...oldProduct, productName: event.newName, updatedAt: journalEntry.createdAt }))
    }
  }
})

describe('EventJournalStorage', () => {
  it.effect('Journal should persist then read events', () =>
    Effect.gen(function* () {
      yield* ProductEventJournal.produce("product-1")(({ append }) => Effect.gen(function* () {
        const _headers = MessageHeaders.MessageHeaders.make({
          messageId: "message-id",

        })

        const productCreatedEvent = ProductCreated.make({
          _headers,
          _aggregateId: "product-1",
          name: "Pizza",
          uom: "PCS",
        })

        yield* append(productCreatedEvent)
      }))

      const eventJournal = yield* EventJournalStorage.EventJournalStorage
      const journalEventsCount = yield* Stream.runCount(eventJournal.read(ProductAggregate.aggregateRootName, "product-1", 0, [ProductCreated, ProductNameChanged]))

      expect(journalEventsCount).toEqual(1)
    }).pipe(
      Effect.provide(
        EventJournalStorage.inMemory
      ),
    ),
  )
})
