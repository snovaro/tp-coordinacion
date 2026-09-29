from common import message_protocol
import uuid

class MessageHandler:
    def __init__(self):
        self.request_id = uuid.uuid4().hex

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.request_id, fruit, amount])

    def serialize_eof_message(self, _):
        return message_protocol.internal.serialize([self.request_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if (fields[0] == self.request_id): # [request_id, [fruit_1, amount_1], [fruit_2, amount_2]]
            return fields[1:]
        return None