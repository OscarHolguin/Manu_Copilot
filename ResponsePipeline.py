# Databricks notebook source
# MAGIC %run ./utilitiesPOC

# COMMAND ----------

from utilitiesPOC import *

from auths import openaisecret, embeddings_secret, adls_secret

storage_account =  '{"storage_account_key": "","storage_container": "manufacturing-copilot","storage_account": "euwdsrg03rsta07dls01"}'
path = "Documents/Embeddings/"



llmconfig= '{"mode": "langchain", "auth": "token", "model": "gpt-35-turbo-16k", "deployment_name": "mxdrca","api_version": "2023-05-15","api_key_secret":"","api_endpoint": "https://usedoai0efaoa03.openai.azure.com/"}'
embedconfig= '{"embedmode": "langchain","model":"text-embedding-ada-002","deployment_name":"azure_embedding","api_key_secret":"","embeddingurl":"https://usedoai0efaoa03.openai.azure.com/"}'

# COMMAND ----------



storage_account = json.loads(storage_account)
storage_account_key, container, storage_account = storage_account.get('storage_account_key'),storage_account.get('storage_container'),storage_account.get('storage_account')

#read from file
storage_account_key = adls_secret

llmconfig = json.loads(llmconfig)
embedconfig = json.loads(embedconfig)

embedmode, embedmodel,embed_deployment,embed_key, embed_url = embedconfig.values()
mode, auth, model, deployment_name, api_version, api_key_secret, api_endpoint = llmconfig.values()

api_key_secret = openaisecret
embed_key = embeddings_secret

# COMMAND ----------

#GETTING THE SECRETS
api_key = api_key_secret


embed_key = embed_key
# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Define LLM 
#DEFINE LLM USING AZURE OPEN AI
llmrca = getllm(openai=True,azure =True,mode = mode, auth=auth,model=model,dep_name = deployment_name,api_version= api_version,
                     api_key =api_key,api_base = api_endpoint)


embeddings_model  = getembeddings(model=embedmodel,dep_name=embed_deployment,api_key=embed_key,api_base=embed_url)

# COMMAND ----------

mount_point = '/mnt/copilotembedds'


#@async_retry
def read_embeddings(fromadls=False,dbricks=False,**kwargs):
    if fromadls:
        if dbricks:
            #reads it into a dataframe first from the mount point
            storage_account=storage_account,container=container,mount_point=mount_point,path=path,storage_account_key=storage_account_key
            for mount in dbutils.fs.mounts():
                if mount.mountPoint.startswith(mount_point):
                    print("Unmounting mnt point")
                    dbutils.fs.unmount(mount.mountPoint)
            
            #mount to path
            dbutils.fs.refreshMounts() #refresh mounts to avoid errors

            mntfiles = mount_to_local(storage_account, container, mount_point,path, storage_account_key)
            #reads into dataframe
            adlsembedspath = [e.path for e in dbutils.fs.ls(mntfiles)][-1] #read last file 
            adlsembedspath= adlsembedspath.replace("dbfs:","/dbfs")
            df = pd.read_pickle(adlsembedspath)

        else:
            #read from adls but not from databricks
            from azure.storage.filedatalake import DataLakeServiceClient
            storage_account,container,path,storage_account_key=kwargs.get("storage_account"),kwargs.get("container"),kwargs.get("path"),kwargs.get("storage_account_key")
            connstr = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
            service = DataLakeServiceClient.from_connection_string(conn_str=connstr)
            file_system_client = service.get_file_system_client(container)
            paths = file_system_client.get_paths(path=path)
            files = [p for p in paths if not p.is_directory]
            files.sort(key=lambda x: x.last_modified)
            last_file = files[-1].name
            file_client = file_system_client.get_file_client(last_file)
            
            # Download the file content and load it as a Python object
            download = file_client.download_file()
            downloaded_bytes = download.readall()
            df = pickle.loads(downloaded_bytes)
            #df = pd.read_pickle(data)
            
    else:
        #read from local FS
        adlsembedspath = [n for n in os.listdir(path)][-1]
        
        df = pd.read_pickle(adlsembedspath)
    #returns the texts and embeddings
    txtlist = [t for t in df.texts]
    embeddings = [e for e in df.embeddings]
    return txtlist,embeddings





def read_embeddings_fromfs(path):
    #service = DataLakeServiceClient.from_connection_string(conn_str="my_connection_string")

    adlsembedspath = [n for n in os.listdir(path)][-1]        
    df = pd.read_pickle(path+"/"+adlsembedspath)
    #returns the texts and embeddings
    txtlist = [t for t in df.texts]
    embeddings = [e for e in df.embeddings]
    return txtlist,embeddings




# COMMAND ----------


loaded_embeddings = read_embeddings(fromadls=True,dbricks=False,storage_account=storage_account,container=container,path=path,storage_account_key=storage_account_key)#read_embeddings_fromfs(path)

# COMMAND ----------

from dataclasses import dataclass
from typing import ClassVar
import pickle

@dataclass 
class RAG2:
    memory_conversation  = None
    def __init__(self,llm,graph,type="langchain",embeddings=None,usememory=False):
        self.llm = llm
        self.graph = graph
        self.type = type
        self.embeddings = embeddings
        self.usememory = usememory

    @staticmethod
    def memory(type='langchain',memory_key="chat_history",output_key='result',rmessages=True,readmem=None):
        if readmem:
            if type.lower()=="langchain":
                from langchain.memory import ConversationBufferMemory, ReadOnlySharedMemory
                memory = ConversationBufferMemory(
                memory_key=memory_key, return_messages=rmessages)
                #output_key=output_key)
                
                readonlymemory = ReadOnlySharedMemory(memory=memory)

                return memory
        else: 
            return None
    

    def vector_retriever(self,store="chroma",**kwargs):
        texts = kwargs.get("texts")
        if store.lower()=="chroma":
            if not self.embeddings:
                self.embeddings = getembeddings(model=kwargs.get("model"),dep_name = kwargs.get("dep_name"),api_key = kwargs.get("api_key"),api_base= kwargs.get("api_base"))
            

            from langchain.chains import RetrievalQA
            persist_directory = kwargs.get("persist_directory","filesdb")
            search_kwargs = kwargs.get("search_kwargs",{"k": 3})
            from langchain.vectorstores import Chroma
            
            db = Chroma.from_documents(texts,self.embeddings,persist_directory= persist_directory)
            vec_retriever = db.as_retriever(search_kwargs=search_kwargs)
         
            return vec_retriever  

            
        elif store.lower()=="faiss":
            #to be implemented
            from langchain_community.vectorstores import FAISS
            from langchain_core.output_parsers import StrOutputParser
            from langchain_core.prompts import ChatPromptTemplate
            from langchain_core.runnables import RunnablePassthrough
            from langchain_openai import ChatOpenAI, OpenAIEmbeddings
            if not self.embeddings:
                self.embeddings = getembeddings(model=kwargs.get("model"),dep_name = kwargs.get("dep_name"),api_key = kwargs.get("api_key"),api_base= kwargs.get("api_base"))
            
            if kwargs.get("text_embeddings"):
                text_embeddings =kwargs.get("text_embeddings")
                vectorstore = FAISS.from_embeddings(text_embeddings=list(zip(text_embeddings[0],text_embeddings[1])), embedding = self.embeddings)
                retriever = vectorstore.as_retriever()
                return retriever
            
            else:
            
                vectorstore = FAISS.from_texts(texts, embedding=OpenAIEmbeddings())
                retriever = vectorstore.as_retriever()
                template = """Answer the question based only on the following context:{context} 
                Question: {query}"""
                prompt = ChatPromptTemplate.from_template(template)
                retrieval_chain = ({"context": retriever, "query": RunnablePassthrough()} | prompt
                                    | self.llm| StrOutputParser())
                return retrieval_chain
        
    def build_chain(self,**kwargs):
        #memory = self.memory() if (readmem := self.memory()) else None
        method = kwargs.get("method","graph")
        
        #PROMPT TEMPLATE        
        from langchain.prompts import HumanMessagePromptTemplate, SystemMessagePromptTemplate, ChatPromptTemplate,PromptTemplate

        
        #####

        memory = self.usememory
        verbose = kwargs.get("verbose",True)

        if memory:
            prompt = PromptTemplate(input_variables=["chat_history", "question"], template="{chat_history} {question}")
        else:
            prompt = PromptTemplate(input_variables=["question"],template ="{question}")

        self.memory_conversation = self.memory(readmem=memory) if not self.memory_conversation else self.memory_conversation

        if kwargs.get("falkor"):
            from langchain.chains import FalkorDBQAChain
            chain = FalkorDBQAChain.from_llm(llm=self.llm, graph=self.graph, verbose=verbose, memory=self.memory_conversation, **kwargs)
        else:
            if method.lower()=="graph":
                #general chain for graphs
                print("building graph chain")
                from langchain.chains import GraphCypherQAChain
                #Prompt template
                        
                general_system_template = r""" 
                Given a specific context, please give a short answer to the question talking like a professional Manufacturing AI Assistant.
                Your answers should be concise, precise and should easily demonstrate you are an expert. Remember you are also a specialist in Root Cause Analysis and FMEA
                you can perform RCA and FMEA basing yourself on the reference values retrieved and comparing them to the incidents. 
                ----
                {context}
                ----
                """
                general_user_template = "Question:```{question}```"
                messages = [
                            SystemMessagePromptTemplate.from_template(general_system_template),
                            HumanMessagePromptTemplate.from_template(general_user_template)
                ]
                qa_prompt = ChatPromptTemplate.from_messages( messages )

                chain = GraphCypherQAChain.from_llm(llm=self.llm, prompt=prompt,graph=self.graph,validate_cypher = True, 
                                                    verbose=verbose,memory=self.memory_conversation, promptTemplate=qa_prompt,**kwargs)
                chain.input_key = "question"

            elif method.lower()=="vector":
                print("Building vector chain ")
                retriever = kwargs.get("vec_retriever")
                from langchain.chains import RetrievalQA
                from langchain.chains import ConversationalRetrievalChain
                
                # template = ("Combine the chat history and follow up question into "
                # "a standalone question. Chat History: {chat_history}"
                # "Follow up question: {query} ")
                template = ("Every time you respond you should start with This is Copilot your AI assistant")
                prompt = PromptTemplate.from_template(template)
                

                #chain = RetrievalQA.from_chain_type(llm=self.llm, chain_type="stuff", retriever=retriever,return_source_documents=True,verbose=verbose)
                        
                general_system_template = r""" 
                Given a specific context, please give a short answer to the question talking like a professional Manufacturing AI Assistant.
                Your answers should be concise, precise and should easily demonstrate you are an expert. Remember you are also a specialist in Root Cause Analysis and FMEA
                you can perform RCA and FMEA basing yourself on the reference values retrieved and comparing them to the incidents. 
                ----
                {context}
                ----
                """
                general_user_template = "Question:```{question}```"
                messages = [
                            SystemMessagePromptTemplate.from_template(general_system_template),
                            HumanMessagePromptTemplate.from_template(general_user_template)
                ]
                qa_prompt = ChatPromptTemplate.from_messages( messages )
                
                chain = ConversationalRetrievalChain.from_llm(self.llm,retriever,memory=self.memory_conversation,get_chat_history=lambda h :h,
                                                              verbose=verbose,
                                                              combine_docs_chain_kwargs={"prompt": qa_prompt}) 
                
                #chain = RetrievalQA.from_chain_type(llm=self.llm, chain_type="stuff", retriever=retriever,
                #                                    return_source_documents=True,verbose=verbose,memory=self.memory_conversation,prompt=prompt)

        return chain


    def get_retriever(self,**kwargs):
        method = kwargs.get("method","graph")
        complexity = kwargs.get("complexity","simple")
        
        if method.lower()=='graph':
            if self.type.lower()=="langchain":
                from langchain.chains import GraphCypherQAChain
                if complexity.lower()=='simple':
                    print("SIMPLE RAG LANGCHAIN")
                    if usememory := kwargs.get("usememory") or self.memory_conversation:
                        print("With Read memory")
                    
                    chain = self.build_chain(**kwargs)
                    return chain
                
                    
            elif self.type.lower()=="llama":
                if complexity.lower()=='simple':
                    nl2graph = kwargs.get("nl2graph",False)
                    print(nl2graph)
                    from llama_index.query_engine import RetrieverQueryEngine
                    from llama_index.retrievers import KnowledgeGraphRAGRetriever                    
                    graph_rag_retriever = KnowledgeGraphRAGRetriever(        
                    storage_context=kwargs.get("storage_context"),
                    service_context=kwargs.get("service_context"),
                    llm=self.llm,
                    verbose=True,
                    with_nl2graphquery=nl2graph)
                    query_engine = RetrieverQueryEngine.from_args(graph_rag_retriever, service_context=kwargs.get("service_context"))
                    return query_engine

                
                elif complexity.lower()=="custom":
                    if self.type.lower()=="llama":
                        from llama_index import get_response_synthesizer
                        from llama_index.query_engine import RetrieverQueryEngine
                        # create custom retriever
                        vector_retriever = VectorIndexRetriever(index=vector_index)
                        kg_retriever = KGTableRetriever(index=kg_index, retriever_mode="keyword", include_text=False)
                        custom_retriever = CustomRetriever(vector_retriever, kg_retriever)
                        # create response synthesizer
                        response_synthesizer = get_response_synthesizer(service_context=service_context,response_mode="tree_summarize")
                        custom_query_engine = RetrieverQueryEngine(retriever=custom_retriever,response_synthesizer=response_synthesizer)
                        return custom_query_engine
                    elif self.type.lower()=='langchain':
                        from langchain.agents import initialize_agent, Tool

                        simple = self.get_retriever("simple",**kwargs)
                        
                        tools = [
                            Tool(name= "GraphRAG",
                                 func= simple.run,
                                 description= "Simple RAG from knowledge graph"),
                            #add more RAG tools
                        ]
        elif method.lower()=="vector":
            print("Getting vector retriever")
            vec_retriever = self.vector_retriever(**kwargs)

            chain = self.build_chain(vec_retriever=vec_retriever,**kwargs)
            return chain



    def response(self,query,complexity="simple",**kwargs):
        method = kwargs.get("method","vector")
        verbose = kwargs.get("verbose",True)
        retriever = self.get_retriever(**kwargs)
        try:
            if method.lower()=="vector":
                print("I AM USING VECTOR LETS GO")
                result =  retriever({"question": query}).get('answer')
                
            elif method.lower()=="graph":
                if self.type.lower()=='langchain':
                    print('using retriever({"query": query}).get("answer")')
                    #result = retriever({"question": query}).get('answer')#
                    result = retriever.invoke({"question": query}).get('result') 
                    pickled_str = pickle.dumps(self.memory_conversation)
                elif self.type.lower()=='llama':
                    result = retriever.query(query)
                    if 'Empty Response' in result.response:
                        raise("Error empty result or no information found")
                    else:
                        return result.response
            
            if not result or "I'm sorry, but I don't have" in result:
                print('Not using RAG')
                raise ("Error empty result or no information found in the KG")
            elif "I'm sorry, but I don't have" in result:
                raise("ERROR 404")
            else:
                return result

        except Exception as e:
            print(e)
            if self.type.lower()=='langchain':
                from langchain.schema import HumanMessage
                #message = HumanMessage(content=query)
                from langchain.chains import ConversationChain
                from langchain.prompts.prompt import PromptTemplate
                if self.usememory:
                    print("Normal LLM response with same memory")
                    prompt = PromptTemplate(input_variables=["chat_history", "question"], template="{chat_history} {query}")
                    chat = ConversationChain(llm=self.llm,verbose=kwargs.get("verbose",True),prompt=prompt,memory=self.memory_conversation ,input_key="question")
                    return chat.invoke({"question":query}).get("response")
                else:
                    from langchain.schema import HumanMessage
                    message = HumanMessage(content=query)
                    return self.llm.invoke([message])

            else:
                #return response with llama index self.llm(response)
                pass
            

# COMMAND ----------
#start graph
url ="neo4j+s://b75e0bd8.databases.neo4j.io"
username ="neo4j"
password = "aziMmumvuVru-rgeKuO0zT7mEtTxu29Jgrmp6lFKv0w"
graph_store = get_graphdb("langchain",graphdb = "neo4j",url=url,username=username,password=password)


#call RAG class initialize vector class 
rag2 = RAG2
vecrag = rag2(llmrca,graph_store,type="langchain",embeddings = embeddings_model,usememory=True)
#vectorchain = vecrag.get_retriever(method="vector",text_embeddings = loaded_embeddings,store="faiss")


# COMMAND ----------
if __name__=="__main__":
    

    # print(vectorchain({"question": "where and why did the production stoppage occurred"}).get("answer")) #if use memory we call it like this 
    # print(vectorchain({"question": "what was my previous question?"}).get("answer"))
    # #
    print(vecrag.response(query="where and why did the production stoppage occurred",method="vector",store="faiss",text_embeddings = loaded_embeddings))
    print(vecrag.response(query="what was my previous question?",method="vector",store="faiss",text_embeddings = loaded_embeddings))
# COMMAND ----------



# COMMAND ----------

#ENTER LANGSERVE

    # text_embedding_pairs = zip(loaded_embeddings[0], loaded_embeddings[1])
    # faiss = FAISS.from_embeddings(text_embedding_pairs, embeddings_model)

# # COMMAND ----------

# # COMMAND ----------




# # COMMAND ----------

from fastapi import FastAPI
# from langchain.chat_models import ChatAnthropic, ChatOpenAI
from langchain.prompts import ChatPromptTemplate
prompt = ChatPromptTemplate.from_template("Answer the users question: {question}")
vectorchain = vecrag.get_retriever(method="vector",text_embeddings = loaded_embeddings,store="faiss")
from langserve import add_routes


# add_routes(
#      app,
#      llmrca,
#      path="/openai",
#  )
app = FastAPI(
    title="LangChain Server",
    version="1.0",
    description="Spin up a simple api server using Langchain's Runnable interfaces",
)
# lsgpt = vecrag.response(query=prompt,method="vector",store="faiss",text_embeddings = loaded_embeddings)

# print(vecrag.response(query=prompt.format_messages(question="how much docuemnts you have in your knowledge base"),method="vector",store="faiss",text_embeddings = loaded_embeddings))
add_routes(app, vectorchain, enable_feedback_endpoint=True,path="/cotalk") #this supports only vector chain qa

@app.get("/rag")
def vector_rag(query_param: str):
    #whole rag response for conversations
    result=vecrag.response(query=prompt.format_messages(question=query_param),method="vector",store="faiss",text_embeddings = loaded_embeddings)
    return {"result": result}

from langserve import RemoteRunnable

cop=RemoteRunnable("http://localhost:8000/cotalk/")

##SERVER SITE EXAMPLE###############################################################################################################################################

app = FastAPI(
    title="Manufacturing-Copilot's Server",
    version="1.0",
    description="Spin up a simple api server using Langchain's Runnable interfaces",
)

#Response from vectorchain directlty
vectorchain = vecrag.get_retriever(method="vector",text_embeddings = loaded_embeddings,store="faiss")
add_routes(app, vectorchain, enable_feedback_endpoint=True,path="/cotalk") #this supports only vector chain qa

#Response from graph
graphchain = vecrag.get_retriever(method="graph",usememory=True)
add_routes(app, graphchain, enable_feedback_endpoint=True,path="/kg") #this supports only graph chain qa

#WHOLE RAG ANSWER conversations with vector only
@app.get("/rag")
def vector_rag(query_param: str, method:str):
    if method.lower()=="vector":
        result=vecrag.response(query=prompt.format_messages(question=query_param),method=method,store="faiss",text_embeddings = loaded_embeddings)
    else:
        result=vecrag.response(query=query_param)
    return {"result": result}

########################################################################################################################################################################



if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
     
# #     import requests
#     inputs = {"question": "how many tickets are there"}
# #     response = requests.post("http://localhost:8000/invoke", json=inputs)

    
# from langserve.client import Client

# # Create a client object with the server URL
# client = Client("http://localhost:8000")
# print("NOW ANOTHER OPTIOOONNNNN")
# response = client.invoke("where and why did the production stoppage occurred")
# print(response)

#from langserve import RemoteRunnable


# from langchain.schema import SystemMessage, HumanMessage
# from langchain.prompts import ChatPromptTemplate
# from langchain.schema.runnable import RunnableMap
# from langserve import RemoteRunnable

# openai = RemoteRunnable("http://localhost:8000/openai/")
# anthropic = RemoteRunnable("http://localhost:8000/anthropic/")
# joke_chain = RemoteRunnable("http://localhost:8000/joke/")

# joke_chain.invoke({"topic": "parrots"})

# # or async
# await joke_chain.ainvoke({"topic": "parrots"})

# prompt = [
#     SystemMessage(content='Act like either a cat or a parrot.'),
#     HumanMessage(content='Hello!')
# ]

# # Supports astream
# async for msg in anthropic.astream(prompt):
#     print(msg, end="", flush=True)

# prompt = ChatPromptTemplate.from_messages(
#     [("system", "Tell me a long story about {topic}")]
# )

# # Can define custom chains
# chain = prompt | RunnableMap({
#     "openai": openai,
#     "anthropic": anthropic,
# })

# chain.batch([{"topic": "parrots"}, {"topic": "cats"}])


# # COMMAND ----------

# #DEPLOY TO AZURE
# #az containerapp up --name [container-app-name] --source . --resource-group [resource-group-name] --environment  [environment-name] --ingress external --target-port 8001 --env-vars=OPENAI_API_KEY=your_key


