import { Schema } from "effect"
import * as Member from "./Member.js"

export const WareHouseIdTypeId = Symbol.for("@@WareHouseId")

export const WareHouseId = Schema.UUID.pipe(
  Schema.annotations({ identifier: "WareHouseId" }),
  Schema.brand(WareHouseIdTypeId)
)

export const WareHouse = Schema.Struct({
  wareHouseId: WareHouseId,
  name: Schema.NonEmptyString,
  ownerId: Schema.Option(Member.MemberId)
}).pipe(
  Schema.annotations({ identifier: "WareHouse" })
)
