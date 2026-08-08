import { Option, Schema } from "effect"
import * as AggregateMessage from "./AggregateMessage.js"

const AggregateRootTypeId = Symbol.for("@@AggregateRoot")
export type AggregateRootTypeId = typeof AggregateRootTypeId

interface AggregateMessageConstructor<AggregateRootName extends string> {
  <Payload extends Schema.Struct.Fields>(
    payload: Payload
  ): Payload & AggregateRootMetadataFields<AggregateRootName>
}

export interface AggregateRoot<AggregateRootName extends string> {
  [AggregateRootTypeId]: AggregateRootTypeId
  aggregateRootName: AggregateRootName
  Query: AggregateMessageConstructor<AggregateRootName>
  Command: AggregateMessageConstructor<AggregateRootName>
  Event: AggregateMessageConstructor<AggregateRootName>
}

export namespace AggregateRoot {
  export type All = AggregateRoot<string>
  export type Name<A extends All> = [A] extends [AggregateRoot<infer Name>] ? Name : never
}

export interface AggregateRootArgs<AggregateRootName extends string> {
  aggregateRootName: AggregateRootName
}

type AggregateRootMetadataFields<AggregateRootName extends string> = {
  _id: typeof Schema.UUID
  _aggregateRoot: Schema.tag<AggregateRootName>
  _aggregateId: typeof Schema.NonEmptyString
  _causationId: Schema.optionalWith<Schema.Option<typeof Schema.UUID>, {
    default: () => Option.Option<string>
  }>
  _correlationId: Schema.optionalWith<Schema.Option<typeof Schema.UUID>, {
    default: () => Option.Option<string>
  }>
}

export function AggregateRoot<AggregateRootName extends string>(
  args: AggregateRootArgs<AggregateRootName>
): AggregateRoot<AggregateRootName> {
  const attachMetadataPayloadFields =
    (messageKind: AggregateMessage.AggregateMessageKind) =>
    <Payload extends Schema.Struct.Fields>(
      basicPayload: Payload
    ) => ({
      ...basicPayload,
      _id: Schema.NonEmptyString.pipe(AggregateMessage.withAggregateMessageKindAnnotation(messageKind)),
      _aggregateRoot: Schema.tag(args.aggregateRootName),
      _aggregateId: Schema.NonEmptyString,
      _causationId: Schema.optionalWith(Schema.Option(Schema.UUID), { default: () => Option.none<string>() }),
      _correlationId: Schema.optionalWith(Schema.Option(Schema.UUID), { default: () => Option.none<string>() })
    })

  return {
    [AggregateRootTypeId]: AggregateRootTypeId,
    aggregateRootName: args.aggregateRootName,
    Query: attachMetadataPayloadFields("Query"),
    Command: attachMetadataPayloadFields("Command"),
    Event: attachMetadataPayloadFields("Event")
  }
}
