# Databricks notebook source
# MAGIC %md
# MAGIC #Information Extraction
# MAGIC
# MAGIC This notebook gets the documents from the dataloader and applies preprocessing 
# MAGIC The pipeline will get the text from the loaded documents, embbed it and eventually it will store it somewhere
# MAGIC This also can extract the triples from the text and upload them to the KG Database
# MAGIC
# MAGIC
# MAGIC For future development:
# MAGIC This pipeline should only run one time
# MAGIC And whenever a new file is added there should be a counter of files already done

# COMMAND ----------

# DBTITLE 0,Run utilities
# MAGIC %run ./utilitiesPOC

# COMMAND ----------

dbutils.widgets.text("timeout", '180')
dbutils.widgets.text("llmconfig", '{"mode": "langchain", "auth": "token", "model": "gpt-35-turbo-16k", "deployment_name": "mxdrca","api_version": "2023-05-15","api_key_secret":"AzureOpenAIKey","api_endpoint": "AzureOpenAIEndpoint"}')
dbutils.widgets.text("embedconfig",'{"embedmode": "langchain","model":"text-embedding-ada-002","deployment_name":"azure_embedding","api_key_secret":"AzureOpenAIEmbeddingKey","embeddingurl":"AzureOpenAIEmbeddingEndpoint"}')

dbutils.widgets.text("kgargs",'{"url":"NEO4JURL","username":"neo4j","password":"NEO4JPASSWORD","kgbuild":"True","kgwrite":"True"}')

dbutils.widgets.text("path","/Documents/")
dbutils.widgets.text("storage_account", '{"storage_account_key": "euwdsrg03rsta07dls01-pwd","storage_container": "manufacturing-copilot","storage_account": "euwdsrg03rsta07dls01"}')


# COMMAND ----------

timeout = int(dbutils.widgets.get("timeout")) #timeout to execute nb in s
path = dbutils.widgets.get("path")


storage_account = json.loads(dbutils.widgets.get("storage_account"))
storage_account_key, container, storage_account = dbutils.secrets.get('kv_scope',storage_account.get('storage_account_key')),storage_account.get('storage_container'),storage_account.get('storage_account')


llmconfig = json.loads(dbutils.widgets.get("llmconfig"))
embedconfig = json.loads(dbutils.widgets.get("embedconfig"))

embedmode, embedmodel,embed_deployment,embed_key, embed_url = embedconfig.values()
mode, auth, model, deployment_name, api_version, api_key_secret, api_endpoint = llmconfig.values()

kgargs = json.loads(dbutils.widgets.get("kgargs"))
url,username,password,kgbuild,kgwrite = kgargs.values()

import ast
kgbuild,kgwrite = ast.literal_eval(kgbuild),ast.literal_eval(kgwrite)

# COMMAND ----------

#GETTING THE SECRETS
api_key = dbutils.secrets.get("kv_scope",api_key_secret)
api_endpoint = dbutils.secrets.get("kv_scope",api_endpoint)

embed_key = dbutils.secrets.get("kv_scope",embed_key)
embed_url =  dbutils.secrets.get("kv_scope",embed_url)

#NEO4J DB CONFIGS
url,password = dbutils.secrets.get("kv_scope",url),dbutils.secrets.get("kv_scope",password)

# COMMAND ----------

# DBTITLE 1,Run Data Loader
#First RUN THE DATA LOADER PIPELINE 
documentsloaded = dbutils.notebook.run("DataLoader",timeout)


# COMMAND ----------

#GET THE TEXT INFORMATION FROM THE DATALOADER
documentsdict = (ast.literal_eval(documentsloaded))
textlists= ast.literal_eval(documentsdict.get("data"))


# COMMAND ----------

#Join all into a single string and preprocess it
alltextsp = preprocess_doc(' '.join(textlists))

# COMMAND ----------

# DBTITLE 1,Define LLM
#DEFINE LLM USING AZURE OPEN AI
llmrca = getllm(openai=True,azure =True,mode = "langchain", auth="token",model="gpt-35-turbo-16k",dep_name = "mxdrca",api_version= "2023-05-15",
                     api_key = "88ae345166d54fb08b6ef2b21d4e7629",api_base = "https://usedoai0efaoa03.openai.azure.com/")

# COMMAND ----------

# DBTITLE 1,Knowledge Graph 
#This build the triplets of entities and their relations and can load it to the graph database in this case neo4j


def KGPipeline(processed_texts,llm,mode="langchain",**kwargs):

    #build the KG
    kgraph = build_knowledge_graph(mode,llm=llm,texts = processed_texts)
    #Extract the triplets from the documents 
    triplets = kgraph.get_triples()
    n1, relations, n2 = list(zip(*triplets)) #entity -> relation -> entity 

    #Entity Disambiguation (disambiguate entities)
    #call disambiguation script
    
    
    #connect to the Knowledge Graph and write the triplets
    if write:=kwargs.get("write") == True:
        url,username,password = kwargs.get("url"),kwargs.get("username"),kwargs.get("password")
        print(f"WRITING TO {url}")
        write_to_kg(kg="neo4j",triplets = triplets,url=url, username = username, pwd=password)
    
    return kgraph


#if build the graph and write call the function with those parameters
if kgbuild:
    print("Building the knowledge graph with write to the neo4jdb set to: ",str(kgwrite))
    kgraph = KGPipeline(alltextsp,llmrca,write=kgwrite,url=url,username=username,password=password)

# COMMAND ----------

# DBTITLE 1,Embeddings
#first initialize embeddings
embeddings_model  = getembeddings(model=embedmodel,dep_name=embed_deployment,api_key=embed_key,api_base=embed_url)

# COMMAND ----------



# COMMAND ----------

def write_embeddings(texts,docsembeddings,mntpnt='/mnt/copilotembeds'):
    #write from a dataframe to the storage account
    embeddf = pd.DataFrame({"texts":texts,"embeddings":docsembeddings})
    for mount in dbutils.fs.mounts():
        if mount.mountPoint.startswith(mntpnt):
            print("Unmounting mnt point")
            dbutils.fs.unmount(mount.mountPoint)
    
    #mount to path
    mount_point = mntpnt
    mntfiles = mount_to_local(storage_account, container, mount_point, path, storage_account_key)
    #Save the dataframe to the pikle file with the corresponding embedding models
    embeddf.to_pickle(f"/dbfs/{mount_point}/embeddings_{datetime.datetime.today().strftime('%d%m%Y')}.pkl") 
    print("Done writing embeddings ")

def read_embeddings(embedspath,mntpnt='/mnt/copilotembeds'):
    #reads it into a dataframe first from the mount point
    for mount in dbutils.fs.mounts():
        if mount.mountPoint.startswith(mntpnt):
            print("Unmounting mnt point")
            dbutils.fs.unmount(mount.mountPoint)
    
    #mount to path
    mount_point = mntpnt
    mntfiles = mount_to_local(storage_account, container, mount_point, path, storage_account_key)
    #reads into dataframe
    df = pd.read_pickle(embedspath)
    #returns the texts and embeddings
    txtlist = [t for t in df.texts]
    embeddings = [e for e in df.embeddings]
    return txtlist,embeddings



def vectorstore_embeddings(embeddingsmodel,vectorstore,texts,embedaction=True,embedwrite=True,embedread=False,**kwargs):
    #THIS FUNCTIONS EMBEDS THE DOCUMENTS AND TEXTS PASSED TO IT IT CAN ALSO STORE THE EMBEDDINGS AND LOAD THE VECTORSTORE 

    if embedaction:
        print("embedding the documents and writing them to ADLS")
        docsembeddings = embeddingsmodel.embed_documents(texts)
        if embedwrite:
            #WRITE TEXTS AND EMBEDDINGS TO ADLS FILE
            #create a dataframe that loads to adls
            write_embeddings(texts,docsembeddings,mntpnt='/mnt/copilotembeds')
    
    return docsembeddings

    if embedread:
        embedpath = kwargs.get("embedpath")
        textslread, embeddingsread = read_embeddings(embedspath)
        return textslread, embeddingsread


# def vectorstore_init():

#         if vectorstore.lower() == "faiss":
#             #initialize FAISS db
#             dbfaiss= FAISS.from_embeddings(text_embeddings = zip(texts,docsembeddings),embedding=embeddingsmodel)
#             return dbfaiss
#         elif vectorstore.lower()=="chroma":
#             from langchain.vectorstores import Chroma
#             persist_directory = kwargs.get("db")
#             vectordb = Chroma(persist_directory=persist_directory,embedding_function=embeddingsmodel)
#             #add new things to chromadb
#             if add_docs:=kwargs.get("addtext"):
#                 new_doc = kwargs.get("new_doc")
#                 ids = kwargs.get("idsnew")
#                 vectordb.add_documents(new_doc,ids=[idsnew])
#             #update document
#             if update:=kwargs.get("update_document"):
#                 docid = kwargs.get("docsid")
#                 update_doc = kwargs.get("update_doc")
#                 vectordb.update_document(docid, update_doc)
            
#             #collections things
#             #delete #vectordb._collection.delete(ids=id)
#             #count vectordb._collection.count()
#             #get vectordb._collection.get(ids=id)


# #WRITE VECTORSTORE EMBEDDINGS

dembs = vectorstore_embeddings(embeddings_model,vectorstore="faiss",embedaction=True,embedwrite=True,texts=textlists)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

edf.to_pickle("file.pkl")

# COMMAND ----------

for mount in dbutils.fs.mounts():
    if mount.mountPoint.startswith('/mnt/copilot'):
        print("Unmounting mnt point")
        dbutils.fs.unmount(mount.mountPoint)
#mount to path
mount_point = '/mnt/copilot'


# COMMAND ----------

mntfiles = mount_to_local(storage_account, container, mount_point, path, storage_account_key)

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/copilot//Documents/")

# COMMAND ----------

edf.to_pickle("/dbfs/mnt/copilot/Embeddings/embeddings_202402022.pkl")

# COMMAND ----------

readtxt = [n for n in pd.read_pickle("/dbfs/mnt/copilot/Embeddings/embeddings_202402022.pkl").texts]
readembs = [n for n in pd.read_pickle("/dbfs/mnt/copilot/Embeddings/embeddings_202402022.pkl").embeddings]

# COMMAND ----------

dbf1=FAISS.from_embeddings(text_embeddings = zip(readtxt,readembs),embedding=embeddings_model)
dbf1.similarity_search("Why did the motor failed")

# COMMAND ----------

textlists

# COMMAND ----------

texts = [d.content for d in documents]
embeddings = emb.embed_documents(texts)
text_embeddings = zip(texts, embeddings)
vs = FAISS.from_embeddings(text_embeddings=text_embeddings, embedding=emb)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from langchain_community.vectorstores import FAISS
# db = FAISS.from_documents(documents, OpenAIEmbeddings())

dbfaiss= FAISS.from_embeddings(text_embeddings = zip(textlists,docsembeddings),embedding=embeddings_model)

# COMMAND ----------

dbfaiss.similarity_search("Why did the motor failed")

# COMMAND ----------

collection = chroma_client.create_collection(name="my_collection")



# COMMAND ----------



# COMMAND ----------

from chromadb import EmbeddingFunction
from transformers import AutoTokenizer, AutoModel

tokenizer = AutoTokenizer.from_pretrained("thenlper/gte-base")
model = AutoModel.from_pretrained("thenlper/gte-base")

class CustomEmbeddingFunction(EmbeddingFunction):
    def __call__(self, texts: Documents) -> Embeddings:
        # Tokenize the texts
        inputs = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
        # Generate embeddings using the model
        outputs = model(**inputs)
        embeddings = outputs.last_hidden_state[:, 0, :]
        return embeddings.tolist()
    

    

# COMMAND ----------

#get the embed the docs and store them somewhere
# have a strategy



# COMMAND ----------

#CRU

#CREATE 
#READ FOR RESPONSE LLM 
#UPDATE 


#FILE1 
#FILE2


#GETTING METADATA NAME, MODIFED DATE  TRIGGER 

#1 SHOULD LOADING AND EMBEDDINGS + PIPELINE2
#PIPELINE 2  RAG RESPONSES - LLMS LANGSERVE*


#UI (BOT) FROM  PIPELINE2