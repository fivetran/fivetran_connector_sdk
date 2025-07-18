from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage

# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: InboundMessage):
        try:
            # Check if the payload is a String or Byte, decode if its the later
            payload = message.get_payload_as_string() if message.get_payload_as_string() is not None else message.get_payload_as_bytes()
            if isinstance(payload, bytearray):
                print(f"Received a message of type: {type(payload)}. Decoding to string")
                payload = payload.decode()

            topic = message.get_destination_name()
            print("\n" + f"Message Payload String: {payload} \n")
            print("\n" + f"Message Topic: {topic} \n")
            print("\n" + f"Message dump: {message} \n")
        except Exception as e:
            print(f"Error processing message: {e.__traceback__}")