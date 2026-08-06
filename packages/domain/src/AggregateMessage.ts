import { Schema, SchemaAST } from "effect"

export const AggregateMessageAnnotationTypeId = Symbol.for("@@AggregateMessageAnnotationTypeId")

export type AggregateMessageKind = "Query" | "Command"

export function withAggregateMessageKindAnnotation(messageKind: AggregateMessageKind) {
  return <A extends Schema.Schema.All>(schema: A): A =>
    Schema.annotations({
      [AggregateMessageAnnotationTypeId]: messageKind
    })(schema)
}

export function getAggregateMessageKindFromSchemaAnnotation<A extends Schema.Schema.All>(schema: A) {
  return SchemaAST.getAnnotation<AggregateMessageKind>(AggregateMessageAnnotationTypeId)(schema.ast)
}
