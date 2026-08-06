import { Schema } from "effect"

export const UnitOfMeasureIdTypeId = Symbol.for("@@UnitOfMeasureId")

export const UnitOfMeasureId = Schema.UUID.pipe(
  Schema.annotations({ identifier: "UnitOfMeasureId" }),
  Schema.brand(UnitOfMeasureIdTypeId)
)

export const UnitOfMeasure = Schema.Struct({
  unitOfMeasureId: UnitOfMeasureId,
  name: Schema.NonEmptyString
}).pipe(
  Schema.annotations({
    identifier: "UnitOfMeasure"
  })
)
