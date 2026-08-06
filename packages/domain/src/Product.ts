import { Schema } from "effect"
import * as UnitOfMeasure from "./UnitOfMeasure.js"

const ProductIdTypeId = Symbol.for("@@ProductId")

export const ProductId = Schema.UUID.pipe(
  Schema.annotations({ identifier: "ProductId" }),
  Schema.brand(ProductIdTypeId)
)

export type ProductId = Schema.Schema.Type<typeof ProductId>

export const Product = Schema.Struct({
  productId: ProductId,
  name: Schema.NonEmptyString,
  unitOfMeasure: UnitOfMeasure.UnitOfMeasureId
}).pipe(
  Schema.annotations({ identifier: "Product" })
)
