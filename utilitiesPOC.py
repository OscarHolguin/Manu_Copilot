# Databricks notebook source

# COMMAND ----------

import argparse
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import datetime
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql.window import Window
# # import mlflow
# from sklearn.metrics import make_scorer, accuracy_score, precision_score, recall_score, f1_score, classification_report, roc_auc_score, confusion_matrix, ConfusionMatrixDisplay
# from sklearn.model_selection import cross_validate
# from sklearn.tree import DecisionTreeClassifier
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.naive_bayes import GaussianNB
# import spacy
# import xgboost as xgb
# from xgboost.sklearn import XGBClassifier
# from sklearn import svm
# #copilot specific
# from nltk.tokenize import word_tokenize
# import nltk
# # nltk.download('punkt')
# # nltk.download('stopwords')
# import uuid
# from string import punctuation
# from langchain.graphs import FalkorDBGraph

# COMMAND ----------

#the function is prepared to have a 1 level nested schema 
# def buildSchema(columns,dType = StringType(),schemaType = ArrayType,multidType=True):
    
#     listidx=next((i for i, v in enumerate(columns) if isinstance(v,list)), None) #find the index of the sublist
#     if listidx:
#         fields = [StructField(columns[listidx-1],StructType([StructField(c,dType,True) for c in col]),True) if isinstance(col,list) 
#               else StructField(col,dType,True) for col in columns if columns.index(col)!=listidx-1]
#     else:
#         fields = [StructField(col,dType,True) for col in columns]
    
#     return schemaType(StructType(fields))

# COMMAND ----------

def read_from_adls(storage_account,container, path, key, filetype=None): #(kv_scope_name,spn_id,spn_secret,tenant_id,storage_account,container, file_path, file_extension, schema)
    key = ""
    adls = f"https://{storage_account}.blob.core.windows.net/{container}"
    spark.conf.set()
    pass
#https://euwdsrg03rsta07dls01.blob.core.windows.net/manufacturing-copilot

# COMMAND ----------

def write_to_adls(storage_account,container,filetype):
    pass


# COMMAND ----------

def write_delta_table(dbName,tableName,path,retention):
    #df.write.format("delta").save("/mnt/.../delta/sdf")
    pass

# COMMAND ----------

def mount_to_local(storage_account, container, mount_point, path, storage_account_key, kv_scope = None, run_id = None):#(kv_scope_name,sp_id,sp_secret,tenant_id,storage_account,container):
    # mount_point= '/mnt/'+mount_point
    mount_path = '/dbfs'+mount_point+'/'+path
    try:
        listedmnts=[s for s in dbutils.fs.ls(mount_path.replace('/dbfs','dbfs:'))]
    except:
        listedmnts=[]
    if any(mount.mountPoint==mount_point for mount in dbutils.fs.mounts()) or len(listedmnts)>0:
        print('Already mounted, returning mount path', mount_path)
        return mount_path

    if kv_scope: #if kv scope is provided key is not passed as hardocoded
        storage_account_key = dbutils.secrets.get(scope=kv_scope,key=storage_account_key)
    
    try:
        source=f'abfss://{container}@{storage_account}.blob.core.windows.net/{path}' 
        
        dbutils.fs.mount(
        source=source,
        mount_point=mount_path,
        extra_configs={f'fs.azure.account.key.{storage_account}.blob.core.windows.net':f'{storage_account_key}'}
    )
        print(f'Mounted {source} to {mount_path}')

    except:
        
        source=f'wasbs://{container}@{storage_account}.blob.core.windows.net/{path}' 
        dbutils.fs.mount(
        source=source,
        mount_point=mount_point,
        extra_configs={f'fs.azure.account.key.{storage_account}.blob.core.windows.net':f'{storage_account_key}'}
    )
        print(f'Mounted {source} to {mount_path}')
    return mount_point

# COMMAND ----------

def write_sql(sqlServerName ,sqlDatabase,userName,password,df,tablename="PDM_AD_PredictionTable", write_mode = 'append',sqlPort = 1433):

    try:
        sqlServerUrl = f"jdbc:sqlserver://{sqlServerName}:{sqlPort};database={sqlDatabase}" 
        connectionProperties = {
            "user" : userName,
            "password":password,
            "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        df.write.jdbc(sqlServerUrl, tablename, write_mode, connectionProperties)
    except Exception as e:
        print(e)


#    df_spark.write.jdbc(sqlServerUrl, "PROD_MxD_PDM_DeviceFailurePredictionTable", "append", connectionPropertie
# s)

# COMMAND ----------

def read_sql(sqlServerName ,sqlDatabase,userName,password,tablename,sqlPort = 1433,query =None):
    query = query if query else f"(SELECT * FROM {tablename}) AS subquery"
    try:
        sqlServerUrl = f"jdbc:sqlserver://{sqlServerName}:{sqlPort};database={sqlDatabase}" 
        connectionProperties = {
            "user" : userName,
            "password":password,
            "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        df = spark.read.format("jdbc").option("url", sqlServerUrl).option("dbtable", query) \
        .option("user", userName).option("password", password).load()
        return df

        #df.write.jdbc(sqlServerUrl, "PDM_AD_PredictionTable", write_mode, connectionProperties)
    except Exception as e:
        print(e)

# COMMAND ----------

def read_sql(sqlServerName ,sqlDatabase,userName,password,tablename,sqlPort = 1433,query =None,pandas=True):
    query = query if query else f"(SELECT * FROM {tablename}) AS subquery"
    if pandas:
        try:
            sqlServerUrl = f"jdbc:sqlserver://{sqlServerName}:{sqlPort};database={sqlDatabase}" 
            connectionProperties = {
                "user" : userName,
                "password":password,
                "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }
            df = spark.read.format("jdbc").option("url", sqlServerUrl).option("dbtable", query) \
            .option("user", userName).option("password", password).load()
            return df

            #df.write.jdbc(sqlServerUrl, "PDM_AD_PredictionTable", write_mode, connectionProperties)
        except Exception as e:
            print(e)
    else:
            connection_string = f"mssql+pymssql://{userName}:{password}@{sqlServerName}:1433/{sqlDatabase}"
            return connection_string

# COMMAND ----------

#returns the columns from a table in a list
def get_cols_from_table(table) ->list:
    return [row["col_name"] for row in spark.sql(f"DESCRIBE {table}").collect()]

# COMMAND ----------

# DBTITLE 1,READ STREAM EVENTHUB
def read_stream(options=None,rootSchema=None,rawcol='RawData',readformat='eventhubs',deseriallize=True,table=None,ignoreChanges=False,expcol = "Values"):

    typeschema = str(type(rootSchema).__repr__)

    explodedc = explode(col('JsonCollection')) if "array" in typeschema else explode(col('JsonCollection'+'.'+expcol))
    if readformat =='delta':
        if ignoreChanges:
            rawdf = spark.readStream.format('delta').option("ignoreChanges", True).table(table)
        else:
            rawdf = spark.readStream.format('delta').table(table)

    elif readformat == 'eventhubs':
        if not deseriallize:
            rawdf= (spark.readStream.format(readformat).options(**options).load())
        
        rawdf = (
        spark.readStream.format(readformat).options(**options).load()
        .withColumn(rawcol, col("body").cast("string"))
        .select(rawcol)
        .select(from_json(col(rawcol),rootSchema,{"dropFieldIfAllNull":False}).alias("JsonCollection"))
        .withColumn('Exp_RESULTS', explodedc)
        .drop('JsonCollection'))
    return rawdf

# COMMAND ----------

#(kv_scope_name,spn_id,spn_secret,tenant_id,storage_account,container, file_path, file_extension, schema)

# COMMAND ----------

# #connect via spn permissions
# service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

# spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
# spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# #connect via sas token
# spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
# spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))

# COMMAND ----------

#model_storage_account_key='mIZUXWLMd0eiIWlVmblJosRBAEIt9GlY0JfekpCQeLGYvYEp6jvyN5Xkxjgia0JCn0g3lcSFRAdSAnfhSbbERw==' 
#model_storage_container='azureml-blobstore-f75b06ba-0e0b-44d1-9a55-c4cedaf7350d' 
#nodel_storage_account='euwdsrg03rsta02'
##run_id = "24250be9-7318-4e79-8399-2f545ed56159"
#if any(mount.mountPoint==f'/mnt/mxd_model' for mount in dbutils.fs.mounts()):
#    pass
#else:
#    dbutils.fs.mount(
#        source=f'abfss://{model_storage_container}@{nodel_storage_account}.blob.core.windows.net/azureml/{run_id}/',
#        mount_point=f'/mnt/mxd_model',
#        extra_configs={f'fs.azure.account.key.{nodel_storage_account}.blob.core.windows.net':f'{model_storage_account_key}'}
#    )

# COMMAND ----------

# model_storage_account_key='mIZUXWLMd0eiIWlVmblJosRBAEIt9GlY0JfekpCQeLGYvYEp6jvyN5Xkxjgia0JCn0g3lcSFRAdSAnfhSbbERw==' 
# model_storage_container='azureml-blobstore-f75b06ba-0e0b-44d1-9a55-c4cedaf7350d' 
# model_storage_account='euwdsrg03rsta02'
# run_id = "24250be9-7318-4e79-8399-2f545ed56159"
# model_storage_container

# COMMAND ----------

def flatten(df)->'DataFrame flat':
    
    pass

# COMMAND ----------

# from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, StringType, BooleanType


# COMMAND ----------



# COMMAND ----------

# def schema_from_json(json_string):
#     if isinstance(json_string,dict):
#         json_dict = json_string
#     else:
#         json_dict = json.loads(json_string)

#     def build_schema(json_obj):
#         data_type = type(json_obj)
#         if data_type == dict:
#             fields = []
#             for key, value in json_obj.items():
#                 field_schema = build_schema(value)
#                 field = StructField(key, field_schema, nullable=True)
#                 fields.append(field)
#             return StructType(fields)
#         elif data_type == list:
#             if len(json_obj) == 0:
#                 return ArrayType(StringType(), containsNull=True)
#             else:
#                 element_schema = build_schema(json_obj[0])
#                 return ArrayType(element_schema, containsNull=True)
#         elif data_type == int:
#             return IntegerType()
#         elif data_type == str:
#             return StringType()
#         elif data_type == bool:
#             return BooleanType()
#         else:
#             return StringType()

#     schema = build_schema(json_dict)

#     return schema

# COMMAND ----------



# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": "<application-id>",
#           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# configs = {"fs.azure.account.auth.type": "OAuth",
#        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#        "fs.azure.account.oauth2.client.id": "xxxxxxxxx", 
#        "fs.azure.account.oauth2.client.secret": "xxxxxxxxx", 
#        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/xxxxxxxxx/oauth2/v2.0/token", 
#        "fs.azure.createRemoteFileSystemDuringInitialization": "true"}



# COMMAND ----------

# def score_model():
#     scoring = {'accuracy':make_scorer(accuracy_score), 
#                'precision':make_scorer(precision_score),
#                'recall':make_scorer(recall_score), 
#                'f1_score':make_scorer(f1_score)}


# # COMMAND ----------

# def best_supclf_model(X_train,y_train,scoring:dict,folds=None,bestmetric='F1 Score') ->'Best score supervised classification model':
#     print('Starting comparison of supervised classification models')
#     #initialize different classification models
#     dtr_model = DecisionTreeClassifier()
#     rfc_model = RandomForestClassifier(n_estimators=100)
#     gnb_model = GaussianNB()
#     svc_model = svm.SVC()
#     xgbm_model = XGBClassifier(objective = 'binary:logistic',use_label_encoder=False,learning_rate = 0.8,n_estimators=160,max_depth=12,min_child_weight=0.5,
#                          gamma=0.1,subsample=0.5,colsample_bytree=1,reg_lambda=1,reg_alpha=1);
    
    
    
#     dtr = cross_validate(dtr_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with dtr')
#     rfc = cross_validate(rfc_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with rfc')
#     gnb = cross_validate(gnb_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with gnb')
#     svc = cross_validate(svc_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with svm')
#     xgbm = cross_validate(xgbm_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with xgbm')
    

#     models_scores_table = pd.DataFrame({'dtr_model':[dtr['test_accuracy'].mean(),
#                                                        dtr['test_precision'].mean(),
#                                                        dtr['test_recall'].mean(),
#                                                        dtr['test_f1_score'].mean()],
                                       
#                                        'rfc_model':[rfc['test_accuracy'].mean(),
#                                                         rfc['test_precision'].mean(),
#                                                         rfc['test_recall'].mean(),
#                                                         rfc['test_f1_score'].mean()],
                                       
#                                        'gnb_model':[gnb['test_accuracy'].mean(),
#                                                                gnb['test_precision'].mean(),
#                                                                gnb['test_recall'].mean(),
#                                                                gnb['test_f1_score'].mean()],
                                      
#                                         'svc_model':[svc['test_accuracy'].mean(),
#                                                                 svc['test_precision'].mean(),
#                                                                 svc['test_recall'].mean(),
#                                                                 svc['test_f1_score'].mean()],
                                       
#                                        'xgbm':[xgbm['test_accuracy'].mean(),
#                                                               xgbm['test_precision'].mean(),
#                                                               xgbm['test_recall'].mean(),
#                                                               xgbm['test_f1_score'].mean()]},
                                      
#                                       index=['Accuracy', 'Precision', 'Recall', 'F1 Score'])
    
#     models_scores_table['Best_Score'] = models_scores_table.idxmax(axis=1)
#     print(models_scores_table)
    
#     bestm = models_scores_table["Best_Score"].loc[bestmetric]
#     print(bestm)
    
#     return locals()[bestm],models_scores_table

# COMMAND ----------

# def best_supclf_model(X_train,y_train,scoring:dict,folds=None,bestmetric = 'F1 Score') ->'Best score supervised classification model':
#     print('Starting comparison of supervised classification models')
#     #initialize different classification models
#     dtr_model = DecisionTreeClassifier()
#     rfc_model = RandomForestClassifier(n_estimators=100)
#     gnb_model = GaussianNB()
#     # svc_model = svm.SVC()
#     xgbm_model = XGBClassifier(objective = 'binary:logistic',use_label_encoder=False,learning_rate = 0.8,n_estimators=160,max_depth=12,min_child_weight=0.5,
#                          gamma=0.1,subsample=0.5,colsample_bytree=1,reg_lambda=1,reg_alpha=1);
    
    
    
#     dtr = cross_validate(dtr_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with dtr')
#     rfc = cross_validate(rfc_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with rfc')
#     gnb = cross_validate(gnb_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with gnb')
#     # svc = cross_validate(svc_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done wirth svm')
#     xgbm = cross_validate(xgbm_model, X_train, y_train, cv=folds, scoring=scoring)
#     print('Done with xgbm')
    

#     models_scores_table = pd.DataFrame({'dtr_model':[dtr['test_accuracy'].mean(),
#                                                        dtr['test_precision'].mean(),
#                                                        dtr['test_recall'].mean(),
#                                                        dtr['test_f1_score'].mean()],
                                       
#                                        'rfc_model':[rfc['test_accuracy'].mean(),
#                                                         rfc['test_precision'].mean(),
#                                                         rfc['test_recall'].mean(),
#                                                         rfc['test_f1_score'].mean()],
                                       
#                                        'gnb_model':[gnb['test_accuracy'].mean(),
#                                                                gnb['test_precision'].mean(),
#                                                                gnb['test_recall'].mean(),
#                                                                gnb['test_f1_score'].mean()],
                                      
#                                        # 'Support Vector Machines':[svc['test_accuracy'].mean(),
#                                        #                         svc['test_precision'].mean(),
#                                        #                         svc['test_recall'].mean(),
#                                        #                         svc['test_f1_score'].mean()],
                                       
#                                        'xgbm':[xgbm['test_accuracy'].mean(),
#                                                               xgbm['test_precision'].mean(),
#                                                               xgbm['test_recall'].mean(),
#                                                               xgbm['test_f1_score'].mean()]},
                                      
#                                       index=['Accuracy', 'Precision', 'Recall', 'F1 Score'])
    
#     models_scores_table['Best_Score'] = models_scores_table.idxmax(axis=1)
#     print(models_scores_table)
    
#     bestm = models_scores_table["Best_Score"].loc[bestmetric]

#     return locals()(bestm)
#     # if models_scores_table["Best_Score"].loc['F1 Score'] == "Decision_Tree":
#     #     print("Going for Decision Tree Model")
#     #     model = DecisionTreeClassifier()
#     #     model.fit(X_train, y_train)
#     #     mlflow.sklearn.log_model(
#     #         sk_model=model,
#     #         registered_model_name=args.registered_model_name,
#     #         artifact_path=args.registered_model_name,
#     #     )

#     #     # Saving the model to a file
#     #     mlflow.sklearn.save_model(
#     #         sk_model=model,
#     #         path=os.path.join(args.model, "trained_model"),
#     #     )
#     # elif models_scores_table["Best_Score"].loc['F1 Score'] == "Random_Forest":
#     #     print("Going for Random Forest Model")
#     #     model = RandomForestClassifier(n_estimators=100)
#     #     model.fit(X_train, y_train)
#     #     mlflow.sklearn.log_model(
#     #         sk_model=model,
#     #         registered_model_name=args.registered_model_name,
#     #         artifact_path=args.registered_model_name,
#     #     )

#     #     # Saving the model to a file
#     #     mlflow.sklearn.save_model(
#     #         sk_model=model,
#     #         path=os.path.join(args.model, "trained_model"),
#     #     )
#     # elif models_scores_table["Best_Score"].loc['F1 Score'] == "Gaussian_Naive_Bayes":
#     #     print("Going for Gaussian Naive Bayes Model")
#     #     model = GaussianNB()
#     #     model.fit(X_train, y_train)
#     #     mlflow.sklearn.log_model(
#     #         sk_model=model,
#     #         registered_model_name=args.registered_model_name,
#     #         artifact_path=args.registered_model_name,
#     #     )

#     #     # Saving the model to a file
#     #     mlflow.sklearn.save_model(
#     #         sk_model=model,
#     #         path=os.path.join(args.model, "trained_model"),
#     #     )
#     # else:
#     #     print("Going for XGBoost Model")
#     #     model = XGBClassifier(objective = 'binary:logistic',use_label_encoder=False,learning_rate = 0.8,n_estimators=160,max_depth=12,min_child_weight=0.5,
#     #                      gamma=0.1,subsample=0.5,colsample_bytree=1,reg_lambda=1,reg_alpha=1);
#     #     model.fit(X_train, y_train)
#     #     mlflow.xgboost.log_model(
#     #         xgb_model=model,
#     #         registered_model_name=args.registered_model_name,
#     #         artifact_path=args.registered_model_name,
#     #     )

#     #     # Saving the model to a file
#     #     mlflow.xgboost.save_model(
#     #         xgb_model=model,
#     #         path=os.path.join(args.model, "trained_model"),
#     #     )
    
     
#     # y_pred = model.predict(X_test)
#     # y_pred_prob = model.predict_proba(X_test)[:,1]
#     # print("\nModel Report")
#     # print("\nAUC Score (Balanced): %f" % roc_auc_score(y_test, y_pred_prob))
#     # print(classification_report(y_test.values, y_pred))
#     # cm = confusion_matrix(y_test.values, y_pred)
#     # _fig, _ax = plt.subplots(figsize=(15, 15))
#     # cm_display = ConfusionMatrixDisplay(confusion_matrix = cm, display_labels = [False, True])
#     # cm_display.plot(cmap="Greens", values_format='', ax=_ax)
#     # mlflow.log_figure(_fig, 'testing_confusion_matrix.png')
    
#     # # Stop Logging
#     # mlflow.end_run()

    
    

# COMMAND ----------

# scoring = {'accuracy':make_scorer(accuracy_score), 
#                'precision':make_scorer(precision_score),
#                'recall':make_scorer(recall_score), 
#                'f1_score':make_scorer(f1_score)}
    
#     train_df = pd.read_csv(select_first_file(args.train_data))  
#     feature_columns = [x for x in train_df.columns if x not in ['FAILURE_TARGET']]
#     y_train = train_df.pop("FAILURE_TARGET")
#     X_train = train_df[feature_columns]

#     test_df = pd.read_csv(select_first_file(args.test_data))
#     y_test = test_df.pop("FAILURE_TARGET")
#     X_test = test_df[feature_columns]

#     print(f"Training with data of shape {X_train.shape}")
#     print(f"Evaluating with data of shape {X_test.shape}")
    
#     dtr_model = DecisionTreeClassifier()
#     rfc_model = RandomForestClassifier(n_estimators=100)
#     gnb_model = GaussianNB()
#     svc_model = svm.SVC()
#     xgbm_model = XGBClassifier(objective = 'binary:logistic',use_label_encoder=False,learning_rate = 0.8,n_estimators=160,max_depth=12,min_child_weight=0.5,
#                          gamma=0.1,subsample=0.5,colsample_bytree=1,reg_lambda=1,reg_alpha=1);
    
    
#     dtr = cross_validate(dtr_model, X_train, y_train, cv=args.folds, scoring=scoring)
#     rfc = cross_validate(rfc_model, X_train, y_train, cv=args.folds, scoring=scoring)
#     gnb = cross_validate(gnb_model, X_train, y_train, cv=args.folds, scoring=scoring)
#     xgbm = cross_validate(xgbm_model, X_train, y_train, cv=args.folds, scoring=scoring)
    
#     models_scores_table = pd.DataFrame({'Decision_Tree':[dtr['test_accuracy'].mean(),
#                                                        dtr['test_precision'].mean(),
#                                                        dtr['test_recall'].mean(),
#                                                        dtr['test_f1_score'].mean()],
                                       
#                                       'Random_Forest':[rfc['test_accuracy'].mean(),
#                                                        rfc['test_precision'].mean(),
#                                                        rfc['test_recall'].mean(),
#                                                        rfc['test_f1_score'].mean()],
                                       
#                                       'Gaussian_Naive_Bayes':[gnb['test_accuracy'].mean(),
#                                                               gnb['test_precision'].mean(),
#                                                               gnb['test_recall'].mean(),
#                                                               gnb['test_f1_score'].mean()],
                                       
#                                        'XGBoost':[xgbm['test_accuracy'].mean(),
#                                                               xgbm['test_precision'].mean(),
#                                                               xgbm['test_recall'].mean(),
#                                                               xgbm['test_f1_score'].mean()]},
                                      
#                                       index=['Accuracy', 'Precision', 'Recall', 'F1 Score'])
    
#     models_scores_table['Best_Score'] = models_scores_table.idxmax(axis=1)
#     print(models_scores_table)
#     if models_scores_table["Best_Score"].loc['F1 Score'] == "Decision_Tree":
#         print("Going for Decision Tree Model")
#         model = DecisionTreeClassifier()
#         model.fit(X_train, y_train)
#         mlflow.sklearn.log_model(
#             sk_model=model,
#             registered_model_name=args.registered_model_name,
#             artifact_path=args.registered_model_name,
#         )

#         # Saving the model to a file
#         mlflow.sklearn.save_model(
#             sk_model=model,
#             path=os.path.join(args.model, "trained_model"),
#         )
#     elif models_scores_table["Best_Score"].loc['F1 Score'] == "Random_Forest":
#         print("Going for Random Forest Model")
#         model = RandomForestClassifier(n_estimators=100)
#         model.fit(X_train, y_train)
#         mlflow.sklearn.log_model(
#             sk_model=model,
#             registered_model_name=args.registered_model_name,
#             artifact_path=args.registered_model_name,
#         )

#         # Saving the model to a file
#         mlflow.sklearn.save_model(
#             sk_model=model,
#             path=os.path.join(args.model, "trained_model"),
#         )
#     elif models_scores_table["Best_Score"].loc['F1 Score'] == "Gaussian_Naive_Bayes":
#         print("Going for Gaussian Naive Bayes Model")
#         model = GaussianNB()
#         model.fit(X_train, y_train)
#         mlflow.sklearn.log_model(
#             sk_model=model,
#             registered_model_name=args.registered_model_name,
#             artifact_path=args.registered_model_name,
#         )

#         # Saving the model to a file
#         mlflow.sklearn.save_model(
#             sk_model=model,
#             path=os.path.join(args.model, "trained_model"),
#         )
#     else:
#         print("Going for XGBoost Model")
#         model = XGBClassifier(objective = 'binary:logistic',use_label_encoder=False,learning_rate = 0.8,n_estimators=160,max_depth=12,min_child_weight=0.5,
#                          gamma=0.1,subsample=0.5,colsample_bytree=1,reg_lambda=1,reg_alpha=1);
#         model.fit(X_train, y_train)
#         mlflow.xgboost.log_model(
#             xgb_model=model,
#             registered_model_name=args.registered_model_name,
#             artifact_path=args.registered_model_name,
#         )

#         # Saving the model to a file
#         mlflow.xgboost.save_model(
#             xgb_model=model,
#             path=os.path.join(args.model, "trained_model"),
#         )
    
     
#     y_pred = model.predict(X_test)
#     y_pred_prob = model.predict_proba(X_test)[:,1]
#     print("\nModel Report")
#     print("\nAUC Score (Balanced): %f" % roc_auc_score(y_test, y_pred_prob))
#     print(classification_report(y_test.values, y_pred))
#     cm = confusion_matrix(y_test.values, y_pred)
#     _fig, _ax = plt.subplots(figsize=(15, 15))
#     cm_display = ConfusionMatrixDisplay(confusion_matrix = cm, display_labels = [False, True])
#     cm_display.plot(cmap="Greens", values_format='', ax=_ax)
#     mlflow.log_figure(_fig, 'testing_confusion_matrix.png')
    
#     # Stop Logging
#     mlflow.end_run()


# COMMAND ----------

def check_balance(df,features,label,threshold =0.4) -> 'returns if dataset is balanced or not in the req label':
    balanced = True
    # n1 = len(list(features))
    # featuresdf = df[list(features)]
    # labeldf = df[[label]]
    label = list(label) if not isinstance(label,list) else label
    subset = pd.DataFrame(df.groupby(label)[features].agg('count'))
    col = subset.idxmax(axis=1)[0]
    value = min(subset[col])/max(subset[col])
    if value < threshold:
        print(f'dataset is unbalanced for {label}, label col is only {value*100}% ')
        balanced = False
    else:
        print('Dataset is balanced for ',label)
        balanced = True
    return balanced

# COMMAND ----------

# DBTITLE 1,MANUFACTURING COPILOT
from dataclasses import dataclass
from langchain.document_loaders import PyPDFLoader, UnstructuredPDFLoader, OnlinePDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter, TextSplitter
import spacy 
@dataclass
class PDFParser:
    pdf_path: str
    extract_images: bool = False
    
    chunked: bool = False
    online: bool = False
    max_tokens:int = 4000
    chunk_overlap: int = 20
    huggingembed: str = "sentence-transformers/all-MiniLM-L6-v2"

    def __post_init__(self):
        self.loader = self.get_loader()

    def get_loader(self):
        if self.chunked:
            return UnstructuredPDFLoader(self.pdf_path)
        elif self.online:
            return OnlinePDFLoader(self.pdf_path)
        else:
            return PyPDFLoader(self.pdf_path, extract_images=self.extract_images)

    def parse_pdf(self):
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.max_tokens,
            chunk_overlap=self.chunk_overlap,
            length_function=len,
            is_separator_regex=False,
        )
        documents = self.loader.load_and_split(text_splitter=text_splitter)
        return documents
    




# COMMAND ----------

# DBTITLE 1,LLM 
def getllm(openai=True, azure=False, mode="langchain",**kwargs):
    print("Note: to use the LLM use it as msg = HumanMessage(content= your query) \n llm(messages = [msg]) ")
    if openai:
        temperature = kwargs.get("temperature",0)
        if azure:
            print("Using Azure OpenAI")
            # from langchain.chat_models import AzureChatOpenAI
            from langchain_openai import AzureChatOpenAI
            auth = kwargs.get("auth")
            if auth.lower()=="token":
                if mode.lower()=="langchain":
                    params = {"model": kwargs.get("model"),
                        "azure_deployment": kwargs.get("dep_name"),
                        "openai_api_version": kwargs.get("api_version"),
                        "temperature": temperature,
                        "openai_api_key": kwargs.get("api_key"),
                        "azure_endpoint": kwargs.get("api_base"),
                        "max_tokens":kwargs.get("max_tokens",256)}
                    llm = AzureChatOpenAI(**params)
                elif mode.lower()=="llama":
                    print("Llama's version")
                    from llama_index.llms import AzureOpenAI
                    import openai
                    openai.api_type = "azure"
                    # openai.api_base =  kwargs.get("api_base") #"INSERT AZURE API BASE"
                    # openai.api_version = kwargs.get("api_version","2023-05-15")
                    
                    llm = AzureOpenAI(engine= kwargs.get("dep_name"),temperature=0,model=kwargs.get("model"),
                                     azure_deployment =kwargs.get("dep_name"),azure_endpoint = kwargs.get("api_base"),
                                     api_version = kwargs.get("api_version","2023-05-15"),api_key= kwargs.get("api_key"))
                return llm
            elif auth.lower() == "azure_ad":
                #from azure.identity import DefaultAzureCredential
                #credential = DefaultAzureCredential()
                #Set the API type to `azure_ad`
                #os.environ["OPENAI_API_TYPE"] = "azure_ad"
               ##Set the API_KEY to the token from the Azure credential
                #os.environ["OPENAI_API_KEY"] = credential.get_token("https://cognitiveservices.azure.com/.default").token            elif auth.lower()=="azure_ad_provider":
                llm = "Not implemented"
            elif auth == "azure ad_provider":
                #managed_identity:
                #from azure.identity import ChainedTokenCredential, ManagedIdentityCredential, AzureCliCredential   
                #credential = ChainedTokenCredential(
                #ManagedIdentityCredential(),
                #AzureCliCredential())
                llm = "not implemented"
        else:
            from langchain_openai import OpenAI
            api_base = kwargs.get("api_base_url")
            api_key =kwargs.get("api_key","NULL")
            llm = OpenAI(
                         base_url = api_base,
                         openai_api_key = api_key,**kwargs)
            return llm 
    else: #huggingface model
        pass    



def llm_cost(llm,message) -> "Total cost in USD":
    from langchain.callbacks import get_openai_callback
    with get_openai_callback() as cb:
        llm([message])
        cost=format(cb.total_cost, '.6f')
        print(f"Total Cost (USD): ${cost}")
    return cost



    def get_embeddings(self,documents=None,lib='langchain'):
#         if not documents:
#             documents = pass
        #huggingface or langchain
        if lib=='langchain':
            from langchain.embeddings import OpenAIEmbeddings
            embeddings_model = OpenAIEmbeddings()
            embeddings = embeddings_model.embed_documents(documents)
            return emebeddings
        else:
            from langchain.embeddings import HuggingFaceEmbeddings
            embeddings = HuggingFaceEmbeddings(model_name=self.huggingembed)
            return embeddings
            


def getembeddings(openai=True,azure=True,mode="langchain",**kwargs):
    if openai:
        temperature = kwargs.get("temperature",0)
        if azure:
            print("Using Azure OpenAI")
            if mode.lower()=="langchain":
                from langchain_openai import AzureOpenAIEmbeddings
                auth = kwargs.get("auth","token") #api token by default
                if auth.lower()=="token":
                    params = {"model": kwargs.get("model"),
                        "azure_deployment": kwargs.get("dep_name"),
                        "openai_api_version": kwargs.get("api_version","2023-05-15"),
                        "openai_api_key": kwargs.get("api_key"),
                        "azure_endpoint": kwargs.get("api_base"),
                        #"model_kwargs":{"max_tokens":kwargs.get("max_tokens",512)}
                        
                    }
                    embeddings = AzureOpenAIEmbeddings(**params)

                    return embeddings
                else:
                    pass #to be implemented different auth types

            elif mode.lower()=="llama":
                from llama_index.embeddings import AzureOpenAIEmbedding
                embed_model = AzureOpenAIEmbedding(
                model=kwargs.get("model"),
                deployment_name=kwargs.get("dep_name"),
                api_key= kwargs.get("api_key"),
                azure_endpoint=kwargs.get("api_base"),
                api_version=kwargs.get("api_version","2023-05-15"))
                
        else:
            pass
    else:
        model=kwargs.get("model"),
        from langchain.embeddings import HuggingFaceEmbeddings
        embeddings = HuggingFaceEmbeddings(model_name=model)
        return embeddings
        
#sample ussage 
# embeddings  = getembdeddings(model='text-embedding-ada-002',dep_name='azure_embedding',api_key="88ae345166d54fb08b6ef2b21d4e7629",api_base="https://usedoai0efaoa03.openai.azure.com//openai/deployments/azure_embedding")



# COMMAND ----------

# DBTITLE 1,Embeddings ADL
#read embeddings
def read_embeddings(storage_account,container,mount_point,path,storage_account_key):
    #reads it into a dataframe first from the mount point
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
    #returns the texts and embeddings
    txtlist = [t for t in df.texts]
    embeddings = [e for e in df.embeddings]
    return txtlist,embeddings



#write embeddings
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

# COMMAND ----------

#SUGGESTES PROMPTS
def get_suggested_prompts(qa,prompt= "Based on my text give me 5 suggested prompts to get insights I can analyze from my text, be brief only 1 sentence per prompt"):
    result =  qa({"question": prompt}).get('answer')
    return result

# COMMAND ----------

# DBTITLE 1,Knowledge Graphs and Graph DB

def get_graphdb(args,**kwargs):
    graphdb = kwargs.get("graphdb")
    url,username,password,database = kwargs.get("url"), kwargs.get("username"), kwargs.get("password"), kwargs.get("database")
    if args.lower()=='llama':
        from llama_index.storage.storage_context import StorageContext
        print('Using LLama Index graph class')
        if graphdb.lower()=='neo4j' or 'neo4j' in url:
            from llama_index.graph_stores import Neo4jGraphStore

            graph_store = Neo4jGraphStore(
                username=username,
                password=password,
                url=url,
                database=database,
            )
            #build storage context
            storage_context = StorageContext.from_defaults(graph_store=graph_store)
            return graph_store, storage_context
        elif graphdb.lower()=='nebula':
            from llama_index.graph_stores import NebulaGraphStore
            #TBD get varibales from kwargs
            graph_store = NebulaGraphStore(
            space_name=kwargs.get("space_name"),
            edge_types=kwargs.get("edge_types"),
            rel_prop_names=kwargs.get("rel_prop_names"),
            tags=kwargs.get("tags"))
            storage_context = StorageContext.from_defaults(graph_store=graph_store)
            return graph_store, storage_context
        
        else:
            raise("Error not implemented for that graph database")
    

    elif args.lower()=='langchain':
        print('Using langchain graph class')
        if graphdb.lower()=='neo4j' or 'neo4j' in url:
            from langchain.graphs import Neo4jGraph
            graph= Neo4jGraph(
               url=url,
               username=username,
               password=password
            )
    
            return graph
        elif graphdb.lower()=="falkordb":
            print("Using falkordb")
            from langchain.graphs import FalkorDBGraph
            graph = FalkorDBGraph(database = database,host=url,port=6379, username=username,password = pwd)


        else:
            raise("Error not implemented for that graph database")

# COMMAND ----------

 
 
def write_to_kg(kg="neo4j",**kwargs)->"graph updating":
    if kg.lower()=="neo4j":
        from py2neo import Graph, Node, Relationship
        triplets,url,username,pwd = kwargs.get("triplets"),kwargs.get("url"),kwargs.get("username"), kwargs.get("pwd")
        graph = Graph(url, auth=(username, pwd))
        nodes = {}
        rels = []
        for n1, r, n2 in triplets:
            if n1 not in nodes:
                nodes[n1] = Node("Entity", name=n1)
            if n2 not in nodes:
                nodes[n2] = Node("Entity", name=n2)
            #append relationship
            rels.append(Relationship(nodes[n1],r,nodes[n2]))
            # upload the nodes and relationships to your graph
            for rel in rels:
                graph.create(rel)
        # graph.create(*nodes.values())
        # graph.create(*rels)
        # graph.refresh_schema()

    elif kg.lower() == "falkordb":
        #add from documents
        graph_documents = kwargs.get("graph_documents")
        from langchain.graphs import FalkorDBGraph
        graph = FalkorDBGraph("falkordb")
        graph.add_graph_documents(graph_documents)
        graph.refresh_schema()

def build_knowledge_graph(args,**kwargs):
    documents = kwargs.get("documents")
    storage_context = kwargs.get("storage_context")
    service_context = kwargs.get("service_context")
    graphstore = kwargs.get("graph")
    max_triplets_per_chunk = kwargs.get("max_triplets",10)

    if args.lower()=="llama":
        from llama_index import (VectorStoreIndex,SimpleDirectoryReader,KnowledgeGraphIndex,ServiceContext)
        #this would upload automatically to the db of the storage context
        index = KnowledgeGraphIndex.from_documents(
        documents,
        max_triplets_per_chunk=max_triplets_per_chunk,
        storage_context=storage_context,
        service_context=service_context)
        return index
        
    elif args.lower() == "langchain":
        if kwargs.get("diffbot"):
            llm = kwargs.get("llm")
            documents = kwargs.get("documents")       
            from langchain_experimental.graph_transformers.diffbot import DiffbotGraphTransformer
            diffbot_api_key = kwargs.get("diffbotapi")
            diffbot_nlp = DiffbotGraphTransformer(diffbot_api_key=diffbot_api_key)
            graph_documents = diffbot_nlp.convert_to_graph_documents(documents)
            if write:=kwargs.get("write"):
                pass
        else:
            from langchain.indexes import GraphIndexCreator
            from langchain.graphs.networkx_graph import KnowledgeTriple
            llm = kwargs.get("llm")
            texts = kwargs.get("texts")
            index_creator = GraphIndexCreator(llm=llm)
            f_index_creator = GraphIndexCreator(llm=llm)
            fgraph = f_index_creator.from_text('')

            sentences = nltk.sent_tokenize(texts)
            for text in sentences:
                triples = index_creator.from_text(text)
                for (node1, relation, node2) in triples.get_triples():
                    fgraph.add_triple(KnowledgeTriple(node1, relation, node2))
            if write:=kwargs.get("write"):
                kg = kwargs.get("kg","neo4j")
                username = kwargs.get("username")
                pwd = kwargs.get("password")
                url = kwargs.get("url")
                triplets = fgraph.get_triplets()
                #if write to neo4j
                write_to_kg(kg=kg,triplets=triplets,username=username,pwd=pwd)
        
            return fgraph

# COMMAND ----------

#Advanced Retriever
# import QueryBundle
# import NodeWithScore


from typing import List

class CustomRetriever(BaseRetriever):
    """Custom retriever that performs both Vector search and Knowledge Graph search"""
    from llama_index import QueryBundle
    from llama_index.schema import NodeWithScore
    # Retrievers
    from llama_index.retrievers import (
        BaseRetriever,
        VectorIndexRetriever,
        KGTableRetriever,
    )


    def __init__(self,vector_retriever: VectorIndexRetriever,kg_retriever: KGTableRetriever,mode: str = "OR") -> None:
        """Init params."""
        self._vector_retriever = vector_retriever
        self._kg_retriever = kg_retriever
        if mode not in ("AND", "OR"):
            raise ValueError("Invalid mode.")
        self._mode = mode
        super().__init__()

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        """Retrieve nodes given query."""
        vector_nodes = self._vector_retriever.retrieve(query_bundle)
        kg_nodes = self._kg_retriever.retrieve(query_bundle)

        vector_ids = {n.node.node_id for n in vector_nodes}
        kg_ids = {n.node.node_id for n in kg_nodes}

        combined_dict = {n.node.node_id: n for n in vector_nodes}
        combined_dict.update({n.node.node_id: n for n in kg_nodes})

        if self._mode == "AND":
            retrieve_ids = vector_ids.intersection(kg_ids)
        else:
            retrieve_ids = vector_ids.union(kg_ids)

        retrieve_nodes = [combined_dict[rid] for rid in retrieve_ids]
        return retrieve_nodes

# COMMAND ----------

# DBTITLE 1,Retrieval Augmented Generation
from dataclasses import dataclass
from typing import ClassVar
import pickle

@dataclass 
class RAG:
    memory_conversation  = None
    def __init__(self,llm,graph,type="langchain",embeddings=None,usememory=False):
        self.llm = llm
        self.graph = graph
        self.type = type
        self.embeddings = embeddings
        self.usememory = usememory

    @staticmethod
    def memory(type='langchain',memory_key="chat_history",rmessages=True,readmem=None):
        if readmem:
            if type.lower()=="langchain":
                from langchain.memory import ConversationBufferMemory, ReadOnlySharedMemory
                memory = ConversationBufferMemory(
                memory_key=memory_key, return_messages=rmessages)
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
                vectorstore = FAISS.from_embeddings(text_embeddings=zip(text_embeddings[0],text_embeddings[1]), embedding = self.embeddings)
                retriever = vectorstore.as_retriever()
                return retriever
            
            else:
            
                vectorstore = FAISS.from_texts(texts, embedding=OpenAIEmbeddings())
                retriever = vectorstore.as_retriever()
                template = """Answer the question based only on the following context:{context} 
                Question: {question}"""
                prompt = ChatPromptTemplate.from_template(template)
                retrieval_chain = ({"context": retriever, "question": RunnablePassthrough()} | prompt
                                    | self.llm| StrOutputParser())
                return retrieval_chain
        
    def build_chain(self,method="graph",**kwargs):
        #memory = self.memory() if (readmem := self.memory()) else None
        from langchain.prompts.prompt import PromptTemplate

        memory = self.usememory
        verbose = kwargs.get("verbose",True)

        if memory:
            prompt = PromptTemplate(input_variables=["chat_history", "query"], template="{chat_history} {query}")
        else:
            prompt = PromptTemplate(input_variables=["query"],template ="{query}")

        self.memory_conversation = self.memory(readmem=memory) if not self.memory_conversation else self.memory_conversation

        if kwargs.get("falkor"):
            from langchain.chains import FalkorDBQAChain
            chain = FalkorDBQAChain.from_llm(llm=self.llm, graph=self.graph, verbose=verbose, memory=self.memory_conversation, **kwargs)
        else:
            if method.lower()=="graph":
                #general chain for graphs
                from langchain.chains import GraphCypherQAChain

                chain = GraphCypherQAChain.from_llm(llm=self.llm, prompt=prompt,graph=self.graph,validate_cypher = True, verbose=verbose,memory=self.memory_conversation,**kwargs)
                chain.input_key = "query"

            elif method.lower()=="vector":
                retriever = kwargs.get("vec_retriever")
                from langchain.chains import RetrievalQA
                from langchain.chains import ConversationalRetrievalChain

                qa = RetrievalQA.from_chain_type(llm=self.llm, chain_type="stuff", retriever=retriever,return_source_documents=True,verbose=verbose)
                chain = ConversationalRetrievalChain.from_llm(llm, db.as_retriever(),memory=self.memory_conversation) 

        return chain


    def get_retriever(self,complexity="simple",method="graph",**kwargs):
        if method.lower()=='graph':
            if self.type.lower()=="langchain":
                from langchain.chains import GraphCypherQAChain
                if complexity.lower()=='simple':
                    print("SIMPLE RAG LANGCHAIN")
                    if usememory := kwargs.get("usememory"):
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
            vec_retriever = self.vector_retriever(**kwargs)

            chain = self.build_chain(**kwargs)
            return chain



    def response(self,query,complexity="simple",**kwargs):
        method = kwargs.get("method","graph")
        verbose = kwargs.get("verbose",True)
        retriever = self.get_retriever(complexity,method=method,**kwargs)
        try:
            if method.lower()=="vector":
                return retriever({"question": query}).get('answer')
            else:
                pass

            if self.type.lower()=='langchain':
                print('using retriever({"question": query}).get("answer")')
                #result = retriever({"question": query}).get('answer')#
                result = retriever.invoke({"query": query}).get('result') 
                pickled_str = pickle.dumps(self.memory_conversation)
                if not result or "I'm sorry, but I don't have" in result:
                    print('Not using RAG')
                    raise ("Error empty result or no information found in the KG")
                else:
                    return result
            elif self.type.lower()=='llama':
                result = retriever.query(query)
                if 'Empty Response' in result.response:
                    raise("Error empty result or no information found")
                else:
                    return result.response

        except Exception as e:
            print(e)
            if self.type.lower()=='langchain':
                from langchain.schema import HumanMessage
                #message = HumanMessage(content=query)
                from langchain.chains import ConversationChain
                from langchain.prompts.prompt import PromptTemplate
                if self.usememory:
                    print("Normal LLM response with same memory")
                    prompt = PromptTemplate(input_variables=["chat_history", "query"], template="{chat_history} {query}")
                    chat = ConversationChain(llm=self.llm,verbose=kwargs.get("verbose",True),prompt=prompt,memory=self.memory_conversation ,input_key="query")
                    return chat.invoke({"query":query}).get("response")
                else:
                    from langchain.schema import HumanMessage
                    message = HumanMessage(content=query)
                    return self.llm.invoke([message])

            else:
                #return response with llama index self.llm(response)
                pass
            

# COMMAND ----------

def extract_info(pdfpath,extract_images = False,max_tokens = 1048,chunk_overlap = 64)-> pd.DataFrame:
    parser = PDFParser(pdfpath,extract_images = extract_images,max_tokens = max_tokens,chunk_overlap = chunk_overlap)
    documents = parser.parse_pdf()
    rows=[]
    for page in documents:
        row = {'text': preprocess_doc(page.page_content), **page.metadata, 'chunk_id': uuid.uuid4().hex}
        rows += [row]
    
    return pd.DataFrame(rows)

# COMMAND ----------

# DBTITLE 1,Extract entities
#once the document is parsed we can extract its entities
def extract_entities(text_chunk,deps = ["nsubj","dobj","pobj","attr"]) -> "Entities":
        #load spacy model
        nlp = spacy.load("en_core_web_sm")
        doc = nlp(text_chunk)
        #tokens filtreing out stopwords,nums and punct
        tokens = [token.text for token in doc if not token.is_stop and not token.is_punct and not token.like_num]
        #extract entities and relations
        entities = [(ent.text,ent.label) for ent in doc.ents]
        relations = [(token.text,token.dep_,token.head.text) for token in doc if token.dep_ in deps]
        return entities, relations

# COMMAND ----------

def preprocess_doc(text):
    from nltk.corpus import stopwords
    stop_words = set(stopwords.words('english'))
    text = ' '.join([word for word in word_tokenize(text) if word not in punctuation and word not in stop_words])
    return text

# COMMAND ----------

#graph to dataframe
def graph2Df(nodes_list) -> pd.DataFrame:
    ## Remove all NaN entities
    graph_dataframe = pd.DataFrame(nodes_list).replace(" ", np.nan)
    graph_dataframe = graph_dataframe.dropna(subset=["node_1", "node_2"])
    graph_dataframe["node_1"] = graph_dataframe["node_1"].apply(lambda x: x.lower())
    graph_dataframe["node_2"] = graph_dataframe["node_2"].apply(lambda x: x.lower())

    return graph_dataframe

# COMMAND ----------

    def graphPrompt(input: str, metadata={}, model="mistral-openorca:latest"):
        if model == None:
            model = "mistral-openorca:latest"
        # model_info = client.show(model_name=model)
        # print( chalk.blue(model_info))

        SYS_PROMPT = (
            "You are a network graph maker who extracts terms and their relations from a given context. "
            "You are provided with a context chunk (delimited by ```) Your task is to extract the ontology "
            "of terms mentioned in the given context. These terms should represent the key concepts as per the context. \n"
            "Thought 1: While traversing through each sentence, Think about the key terms mentioned in it.\n"
                "\tTerms may include object, entity, location, organization, person, \n"
                "\tcondition, acronym, documents, service, concept, etc.\n"
                "\tTerms should be as atomistic as possible\n\n"
            "Thought 2: Think about how these terms can have one on one relation with other terms.\n"
                "\tTerms that are mentioned in the same sentence or the same paragraph are typically related to each other.\n"
                "\tTerms can be related to many other terms\n\n"
            "Thought 3: Find out the relation between each such related pair of terms. \n\n"
            "Format your output as a list of json. Each element of the list contains a pair of terms"
            "and the relation between them, like the follwing: \n"
            "[\n"
            "   {\n"
            '       "node_1": "A concept from extracted ontology",\n'
            '       "node_2": "A related concept from extracted ontology",\n'
            '       "edge": "relationship between the two concepts, node_1 and node_2 in one or two sentences"\n'
            "   }, {...}\n"
            "]"
        )

        USER_PROMPT = f"context: ```{input}``` \n\n output: "
        response, _ = client.generate(model_name=model, system=SYS_PROMPT, prompt=USER_PROMPT)
        try:
            result = json.loads(response)
            result = [dict(item, **metadata) for item in result]
        except:
            print("\n\nERROR ### Here is the buggy response: ", response, "\n\n")
            result = None
        return result

# COMMAND ----------

def df2Graph(dataframe: pd.DataFrame, model=None) -> list:
    # dataframe.reset_index(inplace=True)
    results = dataframe.apply(
        lambda row: graphPrompt(row.text, {"chunk_id": row.chunk_id}, model), axis=1
    )
    # invalid json results in NaN
    results = results.dropna()
    results = results.reset_index(drop=True)

    ## Flatten the list of lists to one single list of entities.
    concept_list = np.concatenate(results).ravel().tolist()
    return concept_list

# COMMAND ----------

def get_entities(sent):
    ## chunk 1
    ent1 = ""
    ent2 = ""

    prv_tok_dep = ""  # dependency tag of previous token in the sentence
    prv_tok_text = ""  # previous token in the sentence

    prefix = ""
    modifier = ""

    #############################################################

    for tok in nlp(sent):
        ## chunk 2
        # if token is a punctuation mark then move on to the next token
        if tok.dep_ != "punct":
            # check: token is a compound word or not
            if tok.dep_ == "compound":
                prefix = tok.text
                # if the previous word was also a 'compound' then add the current word to it
                if prv_tok_dep == "compound":
                    prefix = prv_tok_text + " " + tok.text

            # check: token is a modifier or not
            if tok.dep_.endswith("mod") == True:
                modifier = tok.text
                # if the previous word was also a 'compound' then add the current word to it
                if prv_tok_dep == "compound":
                    modifier = prv_tok_text + " " + tok.text

            ## chunk 3
            if tok.dep_.find("subj") == True:
                ent1 = modifier + " " + prefix + " " + tok.text
                prefix = ""
                modifier = ""
                prv_tok_dep = ""
                prv_tok_text = ""

                ## chunk 4
            if tok.dep_.find("obj") == True:
                ent2 = modifier + " " + prefix + " " + tok.text

            ## chunk 5  
            # update variables
            prv_tok_dep = tok.dep_
            prv_tok_text = tok.text
    #############################################################

    return [ent1.strip(), ent2.strip()]


# COMMAND ----------

def get_relation(sent):

    doc = nlp(sent)

    # Matcher class object 
    matcher = Matcher(nlp.vocab)

    #define the pattern 
    pattern = [{'DEP':'ROOT'},
            {'DEP':'prep','OP':"?"},
            {'DEP':'agent','OP':"?"},  
            {'POS':'ADJ','OP':"?"}] 
    
    print(pattern)

    matcher.add("matching_1", [pattern]) 
    matches = matcher(doc)
    k = len(matches) - 1

    span = doc[matches[k][1]:matches[k][2]] 

    return(span.text)

# COMMAND ----------

# from langchain.chat_models import AzureChatOpenAI

# llm = AzureChatOpenAI(model=os.getenv("OPENAI_CHAT_MODEL"),
#                       deployment_name=os.getenv("OPENAI_CHAT_MODEL"),
#                       temperature=0)


#LLM Params: {'deployment_name': 'text-davinci-002', 'model_name': 'text-davinci-002', 'temperature': 0.7, 'max_tokens': 256, 'top_p': 1, 'frequency_penalty': 0, 'presence_penalty': 0, 'n': 1, 'best_of': 1}



#LLM template
# from langchain.prompts.chat import ChatPromptTemplate

# final_prompt = ChatPromptTemplate.from_messages(
#     [
#         ("system", 
#          """
#           You are a helpful AI assistant expert in querying SQL Database to find answers to user's question about Categories, Products and Orders.
#          """
#          ),
#         ("user", "{question}\n ai: "),
#     ]
# )

# COMMAND ----------

#ASYNC RETRY
import asyncio
def async_retry(max_retries: int = 3, delay: int = 1):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    # Call the decorated asynchronous function
                    result = await func(*args, **kwargs)
                    return result  # If successful, return the result
                except Exception as e:
                    print(f"Attempt {attempt} failed: {str(e)}")
                    await asyncio.sleep(delay)  # Introduce a delay before retrying

            raise ValueError(f"Failed after {max_retries} attempts")

        return wrapper

    return decorator

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


LOGGER = logging.getLogger(__name__)