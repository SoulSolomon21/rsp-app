import { Schema } from "effect"

export const MemberIdTypeId = Symbol.for("@@Member")
export const MemberId = Schema.UUID.pipe(
  Schema.annotations({ identifier: "MemberIdTypeId" })
)

export const Member = Schema.Struct({
  memberId: MemberId,
  name: Schema.NonEmptyString,
  surname: Schema.NonEmptyString,
  birthday: Schema.Date
}).pipe(
  Schema.annotations({ identifier: "Member" })
)

export type Member = Schema.Schema.Type<typeof Member>
