import { Option, Schema } from "effect"
import * as AggregateMessage from "../src/AggregateMessage.js"
import * as AggregateRoot from "../src/AggregateRoot.js"

import { describe, expect, it } from "@effect/vitest"

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

describe("AggregateMessage", () => {
  it("Query - it should check the type of a message", () => {
    const messageKind = AggregateMessage.getAggregateMessageKind(ReadProductName)
    expect(Option.isSome(messageKind)).toBe(true)
    expect(messageKind).toEqual(Option.some("Query"))
  })

  it("Command - it should check the type of a message", () => {
    const messageKind = AggregateMessage.getAggregateMessageKind(ChangeProductName)
    expect(Option.isSome(messageKind)).toBe(true)
    expect(messageKind).toEqual(Option.some("Command"))
  })

  it("Event - it should check the type of a message", () => {
    const messageKind = AggregateMessage.getAggregateMessageKind(ProductNameChanged)
    expect(Option.isSome(messageKind)).toBe(true)
    expect(messageKind).toEqual(Option.some("Event"))
  })
})
