import asyncio
import websockets
import fire

class AggClient:
    async def _connect_and_listen(self, service_address: str):
        uri = f"ws://{service_address}/updates"
        print(f"Connecting to {uri}...")
        try:
            async with websockets.connect(uri) as websocket:
                print("Connected. Listening for updates...")
                while True:
                    message = await websocket.recv()
                    print(message)
        except websockets.exceptions.ConnectionClosedOK:
            print("Connection closed normally.")
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Connection closed with error: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    def run(self, service_address: str = "localhost:3000"):
        """Connects to the aggregation service websocket and prints updates."""
        asyncio.run(self._connect_and_listen(service_address))

if __name__ == '__main__':
    fire.Fire(AggClient)
