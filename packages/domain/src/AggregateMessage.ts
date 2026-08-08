import { Option, Schema, SchemaAST } from "effect"
import type { AggregateRoot } from "./AggregateRoot.js"

export namespace AggregateMessage {
  export type AnyForAggregate<A extends AggregateRoot.All> = {
    Type: {
      _aggregateRoot: AggregateRoot.Name<A>
    }
  }
}

export const AggregateMessageAnnotationTypeId = Symbol.for("@@AggregateMessageAnnotationTypeId")

export type AggregateMessageKind = "Query" | "Command" | "Event"

export function withAggregateMessageKindAnnotation(messageKind: AggregateMessageKind) {
  return <A extends Schema.Schema.All>(schema: A): A =>
    Schema.annotations({
      [AggregateMessageAnnotationTypeId]: messageKind
    })(schema)
}

export function getAggregateMessageKind<A extends Schema.Schema.All>(schema: A) {
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
