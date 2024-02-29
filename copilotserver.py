
from fastapi import FastAPI
from langchain.chat_models import ChatAnthropic, ChatOpenAI
from langchain.prompts import ChatPromptTemplate
#prompt = ChatPromptTemplate.from_template("Answer the users question: {question}")
from langserve import add_routes
from langchain.agents import AgentExecutor, tool


from ResponsePipeline import vecrag,llmrca,loaded_embeddings,rag2
from utilitiesPOC import getllm
##SERVER SITE
app = FastAPI(
    title="Manufacturing-Copilot's Server",
    version="1.0",
    description="Spin up a simple api server using Langchain's Runnable interfaces",
)

#Response from vectorchain directlty
vectorchain = vecrag.get_retriever(method="vector",text_embeddings = loaded_embeddings,store="faiss")
add_routes(app, vectorchain, enable_feedback_endpoint=True,path="/cotalk") #this supports only vector chain qa

#Response from graph
graphchain = vecrag.get_retriever(method="graph",)
add_routes(app, graphchain, enable_feedback_endpoint=True,path="/kg") #this supports only graph chain qa

@app.get("/crag")
def graphnvector(query,promptcomb='try using the functions Copilot_Knowledge first to answer the users query if no answer then the copilot retriever'):
    #langchaintools to combine them 
    #adding vector retriever as tool
    #
    llmazure =getllm(openai=True,azure =True,mode = "langchain", auth="token",model="gpt-35-turbo-16k",dep_name = "chat-bot-trail",            
                  api_version= "2023-12-01-preview", api_key = "1bbc7a5ceb0e4677888f22d1eb4c8617",api_base = "https://chat-bot-iot.openai.azure.com/")#([message])

    from langchain.agents import Tool
    toolslgc = [
    Tool(
        name="copilot_retriever",
        func=lambda q:vectorchain.invoke({"question":q}).get("answer"),
        description="useful for when you want to answer questions about RCA documents and tickets using FAISS.",
        return_direct=False,
    ),
    Tool(
        name="Copilot_Knowledge",
        func=lambda q:graphchain.invoke({"question":q}).get("result"),
        description="useful for when you want to answer questions about neo4j kg using langchain.",
        return_direct=False,
    )]

    #llm_with_tools= llm.bind_tools(toolslgc)
    #now combine them with agent executor
    from langchain.agents import AgentExecutor, create_openai_tools_agent
    from langchain import hub
    prompt = hub.pull("hwchase17/openai-tools-agent")
    agent = create_openai_tools_agent(llmazure, toolslgc, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=toolslgc, verbose=True)
    querytemplate= promptcomb+", this is the query:{question}" 
    result = agent_executor.invoke({"input": 
                       querytemplate.format(question=query)}).get("output")
    return result



#WHOLE RAG ANSWER conversations with vector only
@app.get("/rag")
def vector_rag(query: str, method="vector"):
    if method.lower()=="vector":
        result=vecrag.response(query=query,method=method,store="faiss",text_embeddings = loaded_embeddings)
    else:
        result=vecrag.response(query=query)
    return {"result": result}




if __name__ =="__main__":
    import uvicorn
    uvicorn.run(app,host="localhost", port=8000)




#Client site
#consume it
from langserve import RemoteRunnable

copilotv=RemoteRunnable("http://localhost:8000/cotalk/")
copilotg=RemoteRunnable("http://localhost:8000/kg/")
copilotrag = RemoteRunnable("http://localhost:8000/rag/")






# from fastapi import FastAPI
# from langserve import add_routes
# from langchain.agents import AgentExecutor, tool
# from langchain.agents.format_scratchpad import format_to_openai_functions
# from langchain.agents.output_parsers import OpenAIFunctionsAgentOutputParser
# from pydantic import BaseModel

# from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
# from langchain.tools.render import format_tool_to_openai_function
# from ResponsePipeline import vecrag, vectorchain,llmrca,RAG2

# # vecrag = RAG(llmrca,None,type="langchain",embeddings = embeddings_model,usememory=True)
# # vectorchain = vecrag.get_retriever(method="vector",text_embeddings = loaded_embeddings,store="faiss")





# @tool
# def vec_answers(query:str)->list:
#     "Gets answers using vector RAG"
#     return vectorchain({"question":query}.get("answer"))

# tools = [vec_answers]

# prompt = ChatPromptTemplate.from_messages(
#     [
#         ("system","You are a manufacturing copilot personal assistant  "),
#         ("user","{input}"),
#         MessagesPlaceholder(variable_name="agent_scratchpad"),
#     ]
# )


# tools_llm = llmrca.bind(functions = [format_tool_to_openai_function(t) for t in tools])

# agent = (
#     {
#         "input": lambda x : x["input"],
#         "agent_scratchpad": lambda x : format_to_openai_functions(
#             x["intermediate_steps"]
#         )
#     }
#     | prompt
#     | tools_llm
#     | OpenAIFunctionsAgentOutputParser()
# )

# agent_executor = AgentExecutor(agent=agent,tools=tools)


# app = FastAPI(
#      title="LangChain Server Vector Copilot",
#      version="1.0",
#      description="Spin up a simple api server for Copilot using Langchain's Runnable interfaces",
#  )



# from typing import Optional

# from pydantic import BaseModel, validator

# # class Input1(BaseModel):
# #     __root__: Optional[Input1]

# #     @validator('__root__')
# #     def root_as_str(cls, v):
# #         return str(v)

# # class Output1(BaseModel):
# #     output: str

# class Input1(BaseModel):
#     input : str
#     #__root__: Optional[str] = None
#     # Add a description and an example for the input
#     class Config:
#         schema_extra = {
#             "description": "The input for the Copilot app",
#             "example": {"__root__": "Hello, Copilot!"}
#         }

#     @validator('__root__')
#     def root_as_str(cls, v):
#         return str(v)

# class Output1(BaseModel):
#     output: str
#     # Add a description and an example for the output
#     class Config:
#         schema_extra = {
#             "description": "The output from the Copilot app",
#             "example": {"output": "Hello, user!"}
#         }



# add_routes(app, agent_executor,input_type=Input1,output_type=Output1)

# if __name__=="__main__":
#     import uvicorn
#     uvicorn.run(app,host="localhost",port=8000)
    
    
    
