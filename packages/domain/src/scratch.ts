import { Option, Schema } from "effect"
import * as AggregateRoot from "./AggregateRoot.js"
import { make } from "./EventSourcedAggregate.js"

export const ProductAggregate = AggregateRoot.AggregateRoot({
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

const ProductEventJournal = make({
  aggregateRoot: ProductAggregate,
  eventTypes: [ProductNameChanged, ProductDiscontinued],
  state: Schema.Option(Schema.String),
  initialState: () => Option.none(),
  reduce(state, { event }) {
    switch (event._tag) {
      case "ProductNameChanged":
        return Option.some(event.newName)
      case "ProductDiscontinued":
        return Option.none()
    }
  }
})

const A = ProductEventJournal.produce("product-1")

  /////////////////////////


