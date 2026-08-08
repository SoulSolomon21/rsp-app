import { Schema } from "effect"

export class MessageId extends Schema.NonEmptyString.pipe(Schema.annotations({ identifier: "MessageId" })) {}
