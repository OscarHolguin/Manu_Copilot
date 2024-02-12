# Databricks notebook source
# MAGIC %run ./utilitiesPOC

# COMMAND ----------

dbutils.widgets.text("storage_account", '{"storage_account_key": "euwdsrg03rsta07dls01-pwd","storage_container": "manufacturing-copilot","storage_account": "euwdsrg03rsta07dls01"}')
dbutils.widgets.text("sqlServer",  '{"sqlServerName": "euwdsrg03rsql01.database.windows.net", "sqlDatabase": "EUWDSRG03RRSG02ADB01", "userName": "dbWSS", "password-secret": "EUWDSRG03RRSG02ADB01-pwd"}')
dbutils.widgets.text("file","qualitystudy.pdf")
dbutils.widgets.text("path","/Documents/")



# COMMAND ----------

storage_account = json.loads(dbutils.widgets.get("storage_account"))
sqlServer = json.loads(dbutils.widgets.get("sqlServer"))
pdf_file = dbutils.widgets.get("file")
path = dbutils.widgets.get("path")
sqlServerName,sqlDatabase,userName,password = sqlServer.get('sqlServerName'),sqlServer.get('sqlDatabase'),sqlServer.get('userName'), dbutils.secrets.get(scope = "kv_scope", key= sqlServer.get('password-secret')) 
storage_account_key, container, storage_account = dbutils.secrets.get('kv_scope',storage_account.get('storage_account_key')),storage_account.get('storage_container'),storage_account.get('storage_account')

# COMMAND ----------

# DBTITLE 1,Loading Functions
def loaddata(path,file_ext,**kwargs):
    if file_ext.lower()=="csv":
        #CSV LOADER
        print("CSV FILE FOUND")
        from langchain_community.document_loaders.csv_loader import CSVLoader
        loader = CSVLoader(file_path=path,csv_args = kwargs)
        documents = loader.load()
        return documents

        
    elif file_ext.lower()=="pdf":
        print("PDF FILE FOUND")
        #read with document path
        extract_images = kwargs.get("extract_images",False)
        max_tokens = kwargs.get("max_tokens",2000)
        chunk_overlap = kwargs.get("chunk_overlap",200)
        #PDF Parser
        parser = PDFParser(path,extract_images = extract_images,max_tokens = max_tokens,chunk_overlap = chunk_overlap)
        pdfdocuments = parser.parse_pdf()
        return pdfdocuments



# COMMAND ----------

def loadfolder(folderpath=None,databricks=True,**kwargs)->"All documents loaded into a single text":
    #walk in the folder path
    if databricks:
        mntfiles = kwargs.get("mntpoint")
        path = kwargs.get("path")
        alldocs = []
        filespaths = [n.path for n in dbutils.fs.ls(mntfiles+path)]
        files = [n.name for n in dbutils.fs.ls(mntfiles+path)]
        ##read from a folder and keep checking 
        alldocs = []
        for p in filespaths:
            if len(p.split("."))>1:
                file_ext = p.split(".")[1]
                docs = loaddata(p.replace('dbfs:/','/dbfs//').replace(path,"/"),file_ext)
                alldocs.extend(docs)
        return alldocs

    else:   
        import os
        listpaths = os.listdir(folderpath)
        alldocs = []
        for fpath in listpaths:
            if len(fpath.split("."))>1:
                path,filext=fpath.split(".")
                file = loaddata(path, fileext,**kwargs)
                alldocs.extend(file)
        #alldoc_content = [content.page_content for content in allpdfs]
        return alldocs

# COMMAND ----------

# DBTITLE 1,Loading Single PDF File example
# if __name__ =="__main__":
#     #EXAMPLE LOADING A PDF DOCUMENT
#     #unmount if things are mounted to that mntpoint
#     for mount in dbutils.fs.mounts():
#         if mount.mountPoint.startswith('/mnt/copilot'):
#             print("Unmounting mnt point")
#             dbutils.fs.unmount(mount.mountPoint)
#     #mount to path
#     path = "/Documents/"
#     mount_point = '/mnt/copilot'
#     mntpdf = mount_to_local(storage_account, container, mount_point, path, storage_account_key)
#     pdfpath = f'/dbfs/{mntpdf}/{pdf_file}'#This has to be at folder level if we want to load more than 1
#     file_ext = pdfpath.split(".")[1]
#     filedocs = loaddata(pdfpath,file_ext)
#     # parser = PDFParser(pdfpath,extract_images = False,max_tokens = 1048,chunk_overlap = 64)
#     # documents = parser.parse_pdf()
#     print("FILE LOADED : ")
#     print(filedocs[0].page_content)

# COMMAND ----------

# DBTITLE 1,Multiple Folder Files
#LOAD THE WHOLE FOLDER
if __name__ =="__main__":
    #unmount if things are mounted to that mntpoint
    for mount in dbutils.fs.mounts():
        if mount.mountPoint.startswith('/mnt/copilot'):
            print("Unmounting mnt point")
            dbutils.fs.unmount(mount.mountPoint)
    #mount to path
    mount_point = '/mnt/copilot'
    dbutils.fs.refreshMounts() #refresh mounts to avoid errors
    mntfiles = mount_to_local(storage_account, container, mount_point, path, storage_account_key)
    alldocs1 = loadfolder(folderpath=None,databricks=True,mntpoint=mntfiles,path=path)
    

# COMMAND ----------

docstext = [d.page_content for d in alldocs1]

# COMMAND ----------

import json
nbexit= dbutils.notebook.exit({"data":json.dumps(docstext)})

# COMMAND ----------

