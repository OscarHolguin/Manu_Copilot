from langchain.agents import AgentExecutor, tool
from langchain.agents.format_scratchpad import format_to_openai_functions
from langchain.tools import StructuredTool
import asyncio

from langserve import RemoteRunnable
host = "http://localhost:8000/"

copilotv=RemoteRunnable(host+"cotalk/")
copilotg=RemoteRunnable(host+"kg/")
copilotrag = RemoteRunnable(host+"rag/")

def rag_server_response(query,host="http://localhost:8000/rag",key="query"):
    import requests 
    import json
    resp = requests.get(host+"?"+key+"="+query)
    if resp.status_code==200:
        return json.loads(resp.text)


#Simple function to
def vector_answers(query:str,qkey="question",chain=copilotv)->list:
    """Gets answers using vector RAG.
    Args:
        query: A string containing the question to ask.
        qkey: A string indicating the key for the question in the input dictionary. Default is "question".
        chain: A RemoteRunnable object that represents the vector RAG model.
    Returns:
        A list of answers from the vector RAG model.
    """
    
    return chain({"question":query}.get("answer"))
    

def RAG_asnwers_tool():
    structured_tool = StructuredTool.from_function(
        name="Get LLM response using vector or graph RAG tool",
        func=copilot_response_server,
        description="Copilot use this tool if you want to get a RAG response using knowledge graph or vector embeddings from business documents by API provisioning LangServe")
    
    return structured_tool

# from langchain.agents import tool


# @tool
# def get_word_length(word: str) -> int:
#     """Returns the length of a word."""
#     return len(word)


# @tool
# def graph_response(query):
#     pass

async def stream_obj_response(copilotobj,query,qkey="question"):
    "Streaming response using async stream"
    final_response = []
    async for msg in copilotobj.astream({qkey:query}):
        await final_response.append(msg)
    return final_response
        
    

def copilot_response_server(query:str, qkey="question",fullrag=False,copilotobj=None,stream=False):
    "Gets answers using vector RAG from vectors, graphs or RAG response class"
    if fullrag:
        r = rag_server_response(query)
    else:
        #qkey for vector question and for graph 
        if stream: #streaming response
            r = stream_obj_response(copilotobj,query,qkey)
        else:
            r=copilotobj.invoke({qkey:query})
    return r
    

#TEST STREAMING RESPONSE
import asyncio
print(asyncio.run(stream_obj_response(copilotobj=copilotv,query="how many tickets are related to benchmill")))