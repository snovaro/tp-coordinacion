from common import message_protocol
import uuid

from common.message_protocol.internal import InternalMessageType

class MessageHandler:
    def __init__(self):
        self.request_id = uuid.uuid4().hex
        self.msg_counter = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.msg_counter += 1
        return message_protocol.internal.serialize([
            InternalMessageType.DATA, 
            self.request_id, 
            fruit, 
            amount
            ])

    def serialize_eof_message(self, _):
        return message_protocol.internal.serialize([
            InternalMessageType.EOF, 
            self.request_id, 
            self.msg_counter
            ])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if (fields[0] == self.request_id): # [request_id, [fruit_1, amount_1], [fruit_2, amount_2]]
            return fields[1:]
        return None