import { Effect, Schema } from "effect"
import * as AggregateRoot from "./AggregateRoot.js"
import type { Message } from "./Message.js"

const ProductAggregate = AggregateRoot.AggregateRoot({
  aggregateRootName: "products"
})

export class ChangeProductName extends Schema.TaggedRequest<ChangeProductName>()("ChangeProductName", {
  payload: ProductAggregate.Command({
    newName: Schema.NonEmptyString
  }),
  success: Schema.Boolean,
  failure: Schema.Boolean
}) {}

export class ReadProductName extends Schema.TaggedRequest<ReadProductName>()("ReadProductName", {
  payload: ProductAggregate.Query({}),
  success: Schema.Boolean,
  failure: Schema.Boolean
}) {}

export class ProductNameChanged extends Schema.TaggedClass<ProductNameChanged>()(
  "ProductNameChanged",
  ProductAggregate.Event({
    oldName: Schema.NonEmptyString,
    newName: Schema.NonEmptyString
  })
) {}

export class ProductDiscontinued extends Schema.TaggedClass<ProductDiscontinued>()(
  "ProductDiscontinued",
  ProductAggregate.Event({
    reason: Schema.String
  })
) {}

const MemberAggregate = AggregateRoot.AggregateRoot({
  aggregateRootName: "member"
})

export class MemberJoined extends Schema.TaggedClass<MemberJoined>()(
  "MemberJoined",
  MemberAggregate.Event({})
) {}

interface EventJournal<A extends AggregateRoot.AggregateRoot<string>, Events> {
  append(event: Events): Effect.Effect<void, never, void>
}

function makeEventSourcedAggregate<A extends AggregateRoot.AggregateRoot<string>>(aggregateRoot: A) {
  return <Events extends ReadonlyArray<Message.AnyForAggregate<A>>>(...events: Events) =>
  <R>(
    updateAggregateState: (event: Schema.Schema.Type<Events[number]>) => Effect.Effect<void, never, R>
  ): EventJournal<A, Schema.Schema.Type<Events[number]>> => {}
}

const ProductEventJournal = makeEventSourcedAggregate(ProductAggregate)(ProductNameChanged, ProductDiscontinued)(
  (event) => {
    switch (event._tag) {
      case "ProductNameChanged":
        return Effect.logInfo("Product name now is", event.newName)
      case "ProductDiscontinued":
        return Effect.logInfo("Product discontinued. Reason: ", event.reason)
    }
  }
)

const A = ProductEventJournal.append([ProductNameChanged])

const memberAggregateJournal = makeEventSourcedAggregate(MemberAggregate)(MemberJoined)
