# Databricks notebook source
# MAGIC %md
# MAGIC #### Functions to mount and unmount Azure blobs
# MAGIC When files are mounted, they can be accessed like local files. The path format is '/mnt/blob/file.csv'

# COMMAND ----------

import inspect #Needed to get list of parameters for functions
  

# COMMAND ----------

def mount_blob(storage_account,kv_scope, kv_secret, blob):
    if not any(mount.mountPoint == '/mnt/'+blob+'/' 
               for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
                source = 'wasbs://'+blob+'@'+storage_account
                +'.blob.core.windows.net',
                mount_point = '/mnt/'+blob,
                extra_configs = {'fs.azure.account.key.'
                                 +storage_account+
                                 '.blob.core.windows.net':
                                 dbutils.secrets.get(scope=kv_scope,
                                                     key=kv_secret)
                                }
            )
        except Exception as e:
            print('Could not mount. Possible already mounted')

# COMMAND ----------

def unmount_blob(blob):
    if any(mount.mountPoint == '/mnt/'+blob
           for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount('/mnt/'+blob)
    else:
        print(blob + ' is already demounted')
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function information

# COMMAND ----------

  print("Parameters for mount_blob function:")
  print(inspect.signature(mount_blob)) 

  print("\nParameters for unmount_blob function:")
  print(inspect.signature(unmount_blob)) 
