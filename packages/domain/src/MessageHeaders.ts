import { Option, Schema } from "effect"
import * as MessageId from "./MessageId.js"

export class MessageHeaders extends Schema.Class<MessageHeaders>("MessageHeaders")({
  messageId: MessageId.MessageId,
  causationId: Schema.optionalWith(Schema.Option(MessageId.MessageId), { default: () => Option.none<string>() }),
  correlationId: Schema.optionalWith(Schema.Option(MessageId.MessageId), { default: () => Option.none<string>() })
}) {}
