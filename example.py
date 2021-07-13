"""
https://github.com/mirumee/ariadne/issues/165
https://www.gitmemory.com/issue/mirumee/ariadne-website/77/803012616
"""
import json
from ariadne import EnumType, MutationType, SubscriptionType, make_executable_schema
from ariadne.asgi import GraphQL, WebSocketConnectionError
from broadcaster import Broadcast
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

import uvicorn

type_defs = """
  type Query {
    history: [Message]
  }

  type Message {
    to: String
    sender: String
    message: String
  }

  type Mutation {
    send(sender: String, to: String, message: String): Boolean
  }

  type Subscription {
    message: Message
  }
"""

# pubsub = Broadcast("memory://")
pubsub = Broadcast("redis://localhost:6379")

mutation = MutationType()


@mutation.field("send")
async def resolve_send(*_, **message):
    await pubsub.publish(channel="chatroom", message=json.dumps(message))
    return True


subscription = SubscriptionType()


@subscription.source("message")
async def source_message(_, info):
    user = info.context.get("user")
    # if not user:
    #     return

    async with pubsub.subscribe(channel="chatroom") as subscriber:

        async for event in subscriber:
            message = json.loads(event.message)
            recipient = message["to"].lower()
            if recipient == "@all":
                yield message
            elif user in (message["to"].lower(), message["sender"].lower()):
                yield message


@subscription.field("message")
def resolve_message(obj, *_):
    return obj


schema = make_executable_schema(type_defs, mutation, subscription)


def on_connect(ws, payload):
    user_token = str(payload.get("authUser") or "").strip().lower()
    if "ban" in user_token:
        raise WebSocketConnectionError({"message": "User is banned", "code": "BANNED", "ctx": user_token, "loc": "__ROOT__"})
    ws.scope["user_token"] = user_token or None


def get_context(request):
    if request.scope["type"] == "websocket":
        return {
            "user": request.scope.get("user_token"),
        }

    return {"request": request}


graphql = GraphQL(
    schema=schema,
    context_value=get_context,
    on_connect=on_connect,
    debug=True,
)

app = Starlette(
    debug=True,
    middleware=[
        Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"]),
        # Middleware(HTTPSRedirectMiddleware)
    ],
    on_startup=[pubsub.connect],
    on_shutdown=[pubsub.disconnect],
)
app.mount("/", graphql)
app.mount("/subs", graphql)
if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=8000)
