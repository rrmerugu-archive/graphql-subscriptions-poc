import json

import aiohttp
import async_timeout
import asyncio
import uuid
import json


class AiohttpTransport:
    nest_asyncio_applied = False

    # Default heartbeat of 5.0 seconds.
    def __init__(self, call_from_event_loop=None, read_timeout=None, write_timeout=None, event_loop=None, **kwargs):

        # Start event loop and initialize websocket and client to None
        if event_loop is None:
            self._loop = asyncio.new_event_loop()
        else:
            self._loop = event_loop
        self._websocket = None
        self._client_session = None

        # Set all inner variables to parameters passed in.
        self._aiohttp_kwargs = kwargs
        self._write_timeout = write_timeout
        self._read_timeout = read_timeout
        if "max_content_length" in self._aiohttp_kwargs:
            self._aiohttp_kwargs["max_msg_size"] = self._aiohttp_kwargs.pop("max_content_length")
        if "ssl_options" in self._aiohttp_kwargs:
            self._aiohttp_kwargs["ssl"] = self._aiohttp_kwargs.pop("ssl_options")

    async def connect(self, url, headers=None):
        # Start client session and use it to create a websocket with all the connection options provided.
        self._client_session = aiohttp.ClientSession(loop=self._loop)
        try:
            self._websocket = await self._client_session.ws_connect(url, **self._aiohttp_kwargs, headers=headers)
        except aiohttp.ClientResponseError as err:
            # If 403, just send forbidden because in some cases this prints out a huge verbose message
            # that includes credentials.
            if err.status == 403:
                raise Exception('Failed to connect to server: HTTP Error code 403 - Forbidden.')
            else:
                raise

    async def write(self, message):
        async with async_timeout.timeout(self._write_timeout):
            await self._websocket.send_str(json.dumps(message, default=str))

    async def read(self):
        # Inner function to perform async read.
        async with async_timeout.timeout(self._read_timeout):
            msg = await self._websocket.receive()

            # Need to handle multiple potential message types.
            if msg.type == aiohttp.WSMsgType.close:
                # Server is closing connection, shutdown and throw exception.
                await self.close()
                raise RuntimeError("Connection was closed by server.")
            elif msg.type == aiohttp.WSMsgType.closed:
                # Should not be possible since our loop and socket would be closed.
                raise RuntimeError("Connection was already closed.")
            elif msg.type == aiohttp.WSMsgType.error:
                # Error on connection, try to convert message to a string in error.
                raise RuntimeError("Received error on read: '" + str(msg.data) + "'")
            elif msg.type == aiohttp.WSMsgType.text:
                # Convert message to bytes.
                data = msg.data.strip().encode('utf-8')
            else:
                # General handle, return byte data.
                data = msg.data
            return json.loads(data)

    async def close(self):
        # If the loop is not closed (connection hasn't already been closed)
        if not self._loop.is_closed():
            # Execute the async close synchronously.
            if not self._websocket.closed:
                await self._websocket.close()
            if not self._client_session.closed:
                await self._client_session.close()

            # Close the event loop.
            if not self._loop.is_running():
                self._loop.close()

    @property
    def closed(self):
        # Connection is closed if either the websocket or the client session is closed.
        return self._websocket.closed or self._client_session.closed


class GremlinDriver:

    def __init__(self, gremlin_url, gremlin_traversal_source="g", event_loop=None):
        self.gremlin_url = gremlin_url
        self.gremlin_traversal_source = gremlin_traversal_source
        self.transporter = AiohttpTransport(read_timeout=3600, write_timeout=3600, event_loop=event_loop)

    async def prepare_message(self, gremlin_query):
        processor = ""
        # processor = "session"
        request_id = str(uuid.uuid4())
        query_message = {
            "requestId": request_id,
            "args": {
                "gremlin": gremlin_query,
                "bindings": {},
                "language": "gremlin-groovy",
                "aliases": {"g": self.gremlin_traversal_source},
                "session": request_id
            },
            'op': "eval",
            'processor': processor
        }

        query_message['args']['session'] = request_id
        return query_message

    @staticmethod
    async def get_status_code_from_response(response):
        return response.get("status", {}).get("code")

    async def execute_query(self, query_string):
        await self.transporter.connect(self.gremlin_url)
        message = await self.prepare_message(query_string)
        print("===message: ", message)
        await self.transporter.write(message)
        responses = []
        response_data = await self.transporter.read()
        responses.append(response_data)
        status_code = await self.get_status_code_from_response(response_data)

        while status_code == 206:
            print("status_code", status_code)
            response_data = await self.transporter.read()
            status_code = await self.get_status_code_from_response(response_data)
            responses.append(response_data)
        if status_code == 200:
            await self.transporter.close()
        return responses


# async def read_data():
#     query_string = "g.V().toList()"
#     responses =  await driver.execute_query(query_string )
#     print(responses)
#     for response in responses:
#         print(response['result']['data']['@value'].__len__())
#

# def run_as_sync()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    driver = GremlinDriver("ws://localhost:8182/gremlin", event_loop=loop)
    #
    query_string = "g.V().count()"
    response = loop.run_until_complete(driver.execute_query(query_string))
    print("count======", response[0]['result']['data']['@value'][0]['@value'])
    # exit()

    # for i in range(0, 1000):
    #     query_string = 'g.addV("Person").property("name", "User {}").next()'.format(i)
    #     response = loop.run_until_complete(driver.execute_query(query_string))
    #     print("result=====", response['result'])
    #
    #     query_string__ = "g.V().count()"
    #     response__ = loop.run_until_complete(driver.execute_query(query_string__))
    #     print("count======", response__['result']['data'])
    #     print("count======", response__['result']['data']['@value'][0]['@value'])
    #     print("++++++++++++======+++++++++++++")
    #
    query_string = "g.V().toList()"
    responses = loop.run_until_complete(driver.execute_query(query_string, ))
    print(responses)
    for response in responses:
        print(response['result']['data']['@value'].__len__())
