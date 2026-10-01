import json

class InternalMessageType:
    DATA = "DATA"
    EOF = "EOF"
    EOF_RECEIVED = "EOF_RECEIVED" # Lo envia el primer sum que recibe EOF
    COUNT = "COUNT" # Lo envian todos los sum entre ellos cuando la coordinacion de EOF se activo


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
