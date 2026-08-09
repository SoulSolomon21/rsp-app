import { Schema } from "effect"
import * as AggregateMessage from "./Message.js"

const AggregateRootTypeId = Symbol.for("@rsp-app/domain/AggregateRoot")
export type AggregateRootTypeId = typeof AggregateRootTypeId

export interface AggregateRoot<AggregateRootName extends string> {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  new(_: never): {}
  [AggregateRootTypeId]: AggregateRootTypeId
  aggregateRootName: AggregateRootName
  Query: AggregateMessage.PayloadConstructor<AggregateRootName, Schema.Literal<["Query"]>>
  Command: AggregateMessage.PayloadConstructor<AggregateRootName, Schema.Literal<["Command"]>>
  Event: AggregateMessage.PayloadConstructor<AggregateRootName, Schema.Literal<["Event"]>>
}

export namespace AggregateRoot {
  export type All = AggregateRoot<string>
  export type Name<A extends All> = [A] extends [AggregateRoot<infer Name>] ? Name : never
}

export interface AggregateRootArgs<AggregateRootName extends string> {
  aggregateRootName: AggregateRootName
}

export function AggregateRoot<AggregateRootName extends string>(
  args: AggregateRootArgs<AggregateRootName>
): AggregateRoot<AggregateRootName> {
  return {
    [AggregateRootTypeId]: AggregateRootTypeId,
    aggregateRootName: args.aggregateRootName,
    Query: AggregateMessage.makePayloadConstructor(args.aggregateRootName, Schema.Literal("Query")),
    Command: AggregateMessage.makePayloadConstructor(args.aggregateRootName, Schema.Literal("Command")),
    Event: AggregateMessage.makePayloadConstructor(args.aggregateRootName, Schema.Literal("Event"))
  } as AggregateRoot<AggregateRootName>
}
