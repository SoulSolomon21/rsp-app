import type { Schema } from "effect"
import { Option, SchemaAST } from "effect"

export const AggregateMessageAnnotationTypeId = Symbol.for("@@AggregateMessageAnnotationTypeId")

export namespace MessageKind {
  export type All = AggregateMessageKind
}

export type AggregateMessageKind = "Query" | "Command" | "Event"

export function getMessageKind<A extends Schema.Schema.All>(schema: A) {
  return getAggregateMessageKindFromAST(schema.ast)
}

function getAggregateMessageKindFromAST(ast: SchemaAST.AST): Option.Option<AggregateMessageKind> {
  const messageKind = SchemaAST.getAnnotation<AggregateMessageKind>(AggregateMessageAnnotationTypeId)(ast)

  if (Option.isSome(messageKind)) {
    return messageKind
  }

  switch (ast._tag) {
    case "TypeLiteral":
      for (const propertySignature of ast.propertySignatures) {
        const result = getAggregateMessageKindFromAST(propertySignature.type)
        if (Option.isSome(result)) {
          return result
        }
      }
      return Option.none()
    case "Transformation":
      return getAggregateMessageKindFromAST(ast.from).pipe(Option.orElse(() => getAggregateMessageKindFromAST(ast.to)))
    case "Refinement":
      return getAggregateMessageKindFromAST(ast.from)
    default:
      return Option.none()
  }
}
