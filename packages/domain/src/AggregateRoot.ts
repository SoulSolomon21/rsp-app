import { Schema } from "effect"
import * as AggregateMessage from "./AggregateMessage.js"

const AggregateRootTypeId = Symbol.for("@@AggregateRoot")
export type AggregateRootTypeId = typeof AggregateRootTypeId

export interface AggregateRoot<AggregateRootName extends string> {
  [AggregateRootTypeId]: AggregateRootTypeId
  aggregateRootName: AggregateRootName
  Query: <Payload extends Schema.Struct.Fields>(
    payload: Payload
  ) => Payload & AggregateRootMetadataFields<AggregateRootName>
  Command: <Payload extends Schema.Struct.Fields>(
    payload: Payload
  ) => Payload & AggregateRootMetadataFields<AggregateRootName>
}

export interface AggregateRootArgs<AggregateRootName extends string> {
  aggregateRootName: AggregateRootName
}

type AggregateRootMetadataFields<AggregateRootName extends string> = {
  _id: typeof Schema.UUID
  _aggregateRoot: Schema.tag<AggregateRootName>
  _aggregateId: typeof Schema.NonEmptyString
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
      _id: Schema.UUID.pipe(AggregateMessage.withAggregateMessageKindAnnotation(messageKind)),
      _aggregateRoot: Schema.tag(args.aggregateRootName),
      _aggregateId: Schema.NonEmptyString
    })

  return {
    [AggregateRootTypeId]: AggregateRootTypeId,
    aggregateRootName: args.aggregateRootName,
    Query: attachMetadataPayloadFields("Query"),
    Command: attachMetadataPayloadFields("Command")
  }
}
