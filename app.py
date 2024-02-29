# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import sys
import traceback
from datetime import datetime
from http import HTTPStatus

from aiohttp import web
from aiohttp.web import Request, Response, json_response
from botbuilder.core import (
    TurnContext,
)
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.integration.aiohttp import CloudAdapter, ConfigurationBotFrameworkAuthentication
from botbuilder.schema import Activity, ActivityTypes
from botbuilder.schema import Activity, ActivityTypes

from bots import EchoBot
from config import DefaultConfig

# CONFIG = DefaultConfig()

# # Create adapter.
# # See https://aka.ms/about-bot-adapter to learn more about how bots work.
# ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))


# # Catch-all for errors.
# async def on_error(context: TurnContext, error: Exception):
#     # This check writes out errors to console log .vs. app insights.
#     # NOTE: In production environment, you should consider logging this to Azure
#     #       application insights.
#     print(f"\n [on_turn_error] unhandled error: {error}", file=sys.stderr)
#     traceback.print_exc()

#     # Send a message to the user
#     await context.send_activity("The bot encountered an error or bug.")
#     await context.send_activity(
#         "To continue to run this bot, please fix the bot source code."
#     )
#     # Send a trace activity if we're talking to the Bot Framework Emulator
#     if context.activity.channel_id == "emulator":
#         # Create a trace activity that contains the error object
#         trace_activity = Activity(
#             label="TurnError",
#             name="on_turn_error Trace",
#             timestamp=datetime.utcnow(),
#             type=ActivityTypes.trace,
#             value=f"{error}",
#             value_type="https://www.botframework.com/schemas/error",
#         )
#         # Send a trace activity, which will be displayed in Bot Framework Emulator
#         await context.send_activity(trace_activity)


# ADAPTER.on_turn_error = on_error

# # Create the Bot
# BOT = EchoBot()


# # Listen for incoming requests on /api/messages
# async def messages(req: Request) -> Response:
#     # Main bot message handler.
#     if "application/json" in req.headers["Content-Type"]:
#         body = await req.json()
#     else:
#         return Response(status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)

#     activity = Activity().deserialize(body)
#     auth_header = req.headers["Authorization"] if "Authorization" in req.headers else ""

#     response = await ADAPTER.process_activity(auth_header, activity, BOT.on_turn)
#     if response:
#         return json_response(data=response.body, status=response.status)
#     return Response(status=HTTPStatus.OK)


# APP = web.Application(middlewares=[aiohttp_error_middleware])
# APP.router.add_post("/api/messages", messages)

# if __name__ == "__main__":
#     try:
#         web.run_app(APP, host="localhost", port=CONFIG.PORT)
#     except Exception as error:
#         raise error

from aiohttp import web
from botbuilder.core import (
    BotFrameworkAdapterSettings,
    TurnContext,
    BotFrameworkAdapter,
)
from botbuilder.schema import Activity, ActivityTypes
from botbuilder.core import Bot

CONFIG = DefaultConfig()

# Create adapter.
# See https://aka.ms/about-bot-adapter to learn more about how bots work.
ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))
HOST = "http://localhost:8000/crag"

# Catch-all for errors.
async def on_error(context: TurnContext, error: Exception):
    # This check writes out errors to console log .vs. app insights.
    # NOTE: In production environment, you should consider logging this to Azure
    #       application insights.
    print(f"\n [on_turn_error] unhandled error: {error}", file=sys.stderr)
    traceback.print_exc()

    # Send a message to the user
    await context.send_activity("The bot encountered an error or bug.")
    await context.send_activity(
        "To continue to run this bot, please fix the bot source code."
    )
    # Send a trace activity if we're talking to the Bot Framework Emulator
    if context.activity.channel_id == "emulator":
        # Create a trace activity that contains the error object
        trace_activity = Activity(
            label="TurnError",
            name="on_turn_error Trace",
            timestamp=datetime.utcnow(),
            type=ActivityTypes.trace,
            value=f"{error}",
            value_type="https://www.botframework.com/schemas/error",
        )
        # Send a trace activity, which will be displayed in Bot Framework Emulator
        await context.send_activity(trace_activity)


ADAPTER.on_turn_error = on_error

###### langserve responses

import requests
from cachetools import cached, TTLCache

# Create a cache with a maximum of 100 entries and a time-to-live (TTL) of 180 seconds
cache = TTLCache(maxsize=100, ttl=180)

@cached(cache)
def rag_server_response(query,host="http://localhost:8000/rag",key="query"):
    import requests 
    import json
    resp = requests.get(host+"?"+key+"="+query)
    if resp.status_code==200:
        return json.loads(resp.text)

@cached(cache)
def rag_server_response_stream(query, host="http://localhost:8000/rag", key="query"):
    try:
        resp = requests.get(host, params={key: query}, stream=True)
        resp.raise_for_status()  # Raise an exception if the response status code is not 200
        for chunk in resp.iter_content(chunk_size=1024):  # Adjust chunk size as needed
            yield chunk
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")

#streaming
async def stream_obj_response(copilotobj,query,qkey="question"):
    "Streaming response using async stream"
    final_response = []
    async for msg in copilotobj.astream({qkey:query}):
        print(msg.get("answer"), end="", flush=True)
        final_response.append(msg.get("answer"), end="", flush=True)
    return final_response


@cached(cache)
def copilot_response_server(query:str, qkey="question",fullrag=False,copilotobj=None,stream=False,**kwargs):
    "Gets answers using vector RAG from vectors, graphs or RAG response class"
    if fullrag:
        host = kwargs.get("host","http://localhost:8000/rag")
        if stream:
            r = rag_server_response_stream(query,host=host)
        else:
            r = rag_server_response(query,host=host)
    else:
        #qkey for vector question and for graph 
        if stream: #streaming response
            r = stream_obj_response(copilotobj,query,qkey)
        else:
            r=copilotobj.invoke({qkey:query})
    return r


####################3
class EchoBot(Bot):
    async def on_turn(self, context: TurnContext):
        # Check if the activity type is a message
        if context.activity.type == ActivityTypes.message:
            # Echo back the user's message
            await context.send_activity(f"Bot: {copilot_response_server(context.activity.text,host=HOST,fullrag=True)}")


# Create the Bot instance
BOT = EchoBot()


# Listen for incoming requests on /api/messages
async def messages(req: Request) -> Response:
    # Main bot message handler.
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return Response(status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)

    activity = Activity().deserialize(body)
    auth_header = req.headers["Authorization"] if "Authorization" in req.headers else ""

    response = await ADAPTER.process_activity(auth_header, activity, BOT.on_turn)
    if response:
        return json_response(data=response.body, status=response.status)
    return Response(status=HTTPStatus.OK)


APP = web.Application(middlewares=[aiohttp_error_middleware])
APP.router.add_post("/api/messages", messages)

if __name__ == "__main__":
    try:
        web.run_app(APP, host="localhost", port=CONFIG.PORT)
    except Exception as error:
        raise error
