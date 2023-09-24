#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
from csv import DictReader
import pandas as pd
from flask import Flask, request, render_template,session,redirect,url_for,jsonify
import jsonpickle
import os
from pathlib import Path
db_name = "edfs"
db_file_table_name = "EDFS_FILE"
db_path_table_name = "EDFS_PATH"


class EDFS_PARTITION:
    def __init__(self, partition_key, partition_count):
        self.partition_key = partition_key
        self.partition_count = partition_count


class EDFS:
    def __init__(self, edfs_host, edfs_user, edfs_password, edfs_port=None) -> None:
        self.edfs_host = edfs_host
        self.edfs_user = edfs_user
        self.edfs_password = edfs_password
        self.edfs_port = edfs_port
        self.edfs_db = None
        self.edfs_cursor = None
        self._create_db()
        self._create_tab()
   
    def mkdir(self, path):
        path_list = path.split("/")
        parent_id = -1
        for dir in path_list:
            if dir == "":
                continue
            dataList = self._query_dir(parent_id, dir)
            if len(dataList) == 0:
                dataList = self._insert_dir(parent_id, dir)
            parent_id = dataList[0]["id"]
        return ('mkdir command success:')
        

    def put(self, file_name, path, k: EDFS_PARTITION = None):
        
        file_type = file_name.split(".")
        if (len(file_type) == 2 and (file_type[1] == "csv" or file_type[1] == "CSV")) == False:
            return("Error: Please upload a file in CSV format")
            
        path_id = self._check_dir_path(path)
        
        if (path_id == False):
            return("Error: Path does not exist")
          
        dataList = self._query_file_pathId_name(path_id, file_name)
        if len(dataList) != 0:
            return("Error: A file with the same name exists in this directory")
           
        fileId = self._inster_file(path_id, file_name)
        fileId = fileId[0]["id"]
        print("file_name",file_name)
      
        cwd = os.getcwd()
        with open(str(cwd)+'/'+file_name, mode="r", encoding='utf-8') as read_obj:
            print("debugging")
            dict_reader = DictReader(read_obj)
            list_of_dict = list(dict_reader)
            table_name = file_type[0] + "_" + str(fileId)
            self._create_file_tab(table_name, list_of_dict[0].keys(), k)
            for dic in list_of_dict:
                self._insert_file_data(table_name, dic)
                #return insert

        return("put file success")

    def cat(self, file_path):
        dataList, file_name, path_id = self.getDataList(file_path)
        if len(dataList) == 0:
            print("Error: file does not exist")
            return
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        dataList = self._query_file_data(table_name)
        df = pd.DataFrame(dataList)
        return(df)

    

    def rm(self, file_path):
        dataList, file_name, path_id = self.getDataList(file_path)
        print(dataList)
        if len(dataList) == 0:
            print("Error: file does not exist")
            return ("Error: file does not exist, remove file failed")
        self._del_file_pathId_name(path_id, file_name)
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        self._del_file_data_table(table_name)
        return("remove file success")

    def ls(self, path):
        path_id = self._check_dir_path(path)
        try:
            file_list = self._query_file_pathId_name(path_id)
            #dir_list = self._query_dir(path_id)
            return_list = []
            for data in file_list:
                
                #print(type(data['name']))
                return_list.append(data['name'])
            return return_list
        
        except TypeError:
            if path_id == False:
                return("Error: Path does not exist")
            


    def getPartitionLocations(self, file):
        dataList, file_name, path_id = self.getDataList(file)
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        dataList = self._query_file_data_partition_info(table_name)
        df = pd.DataFrame(dataList)
        print(df)
        return df

    def getDataList(self, file):
        path_list = file.split("/")
        file_name = path_list[-1]
        dir_path = "/".join(path_list[:-1])
        path_id = self._check_dir_path(dir_path)
        if not path_id:
            print("Error: file does not exist")
            return [], file_name, path_id
        dataList = self._query_file_pathId_name(path_id, file_name)
        return dataList, file_name, path_id

    def readPartition(self, file, partition):
        dataList, file_name, path_id = self.getDataList(file)
        if len(dataList) == 0:
            print("Error: file does not exist")
            return ("Error: file does not exist")
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        dataList = self._query_file_data_partition_data(table_name, partition)
        return dataList

    def mapPartititon(self, table_name, partition, where_dic=None):
        partitionData = self._query_file_data_partition_data(table_name, partition, where_dic)
        return partitionData

    def reduce(self, file, where_dic=None):
        dataList, file_name, path_id = self.getDataList(file)
        if len(dataList) == 0:
            print("Error: file does not exist")
            return
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        infoList = self._query_file_data_partition_info(table_name)

        output = []
        # if table has no partition
        if not infoList[0]['PARTITION_NAME']:
            output = self._query_file_data(table_name, where_dic)
        else:
            # table has partitions
            for data in infoList:
                mapData = self.mapPartititon(table_name, data['PARTITION_NAME'], where_dic)
                # combine data
                if len(mapData) > 0:
                    output += mapData
        return output

    def close(self):
        self.edfs_db.close()
        self.edfs_cursor.close()

    def _check_dir_path(self, dir_path):
        path_list = dir_path.split("/")
        parent_id = -1
        for dir in path_list:
            if dir == "":
                continue
            dataList = self._query_dir(parent_id, dir)
            if len(dataList) == 0:
                return False
            parent_id = dataList[0]["id"]
        return parent_id

    def _query_file_pathId_name(self, path_id, name=None):
        userSql = "SELECT * FROM {table} WHERE path_id={path_id} AND name='{name}'".format(table=db_file_table_name,
                                                                                           path_id=path_id, name=name)
        if name == None:
            userSql = "SELECT * FROM {table} WHERE path_id={path_id}".format(table=db_file_table_name, path_id=path_id)
        self._execute(userSql)
        dataList = self._dataList()
        return dataList

    def _query_dir(self, parent_id, name=None):
        userSql = "SELECT * FROM {table} WHERE parent_id={parent_id} AND name='{name}'".format(table=db_path_table_name,
                                                                                               parent_id=parent_id,
                                                                                               name=name)
        if name == None:
            userSql = "SELECT * FROM {table} WHERE parent_id={parent_id}".format(table=db_path_table_name,
                                                                                 parent_id=parent_id)
        self._execute(userSql)
        dataList = self._dataList()
        return dataList

    def _query_file_data(self, table_name, where_dic=None):
        userSql = "SELECT * FROM {table}".format(table=table_name)

        userSql = self._add_where_dic(userSql, where_dic)

        self._execute(userSql)
        dataList = self._dataList()
        return dataList

    def _query_file_data_partition_info(self, table_name):
        userSql = "SELECT TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA=SCHEMA() AND TABLE_NAME='{table}'".format(
            table=table_name)
        self._execute(userSql)
        dataList = self._dataList()
        return dataList

    def _add_where_dic(self, userSQL, where_dic=None):
        if not where_dic:
            return userSQL
        where = []
        for key in where_dic.keys():
            where.append(str.format('{} {}', key, where_dic[key]))
        if len(where) > 1:
            userSQL += ' where ' + ' and '.join(where)
        else:
            userSQL += ' where ' + where[0]
        return userSQL

    def _query_file_data_partition_data(self, table_name, partition_name, where_dic=None):
        userSql = "SELECT * FROM {table} PARTITION({partition_name})".format(table=table_name,
                                                                             partition_name=partition_name)

        userSql = self._add_where_dic(userSql, where_dic)

        self._execute(userSql)
        dataList = self._dataList()
        return dataList

    def _inster_file(self, path_id, file_name):
        data = {
            "path_id": path_id,
            "name": file_name
        }
        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=db_file_table_name, keys=keys, values=values)
        self._execute(sql, data.values())
        return self._query_file_pathId_name(path_id, file_name)

    def _insert_dir(self, parent_id, name):
        data = {
            "parent_id": parent_id,
            "name": name
        }
        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=db_path_table_name, keys=keys, values=values)
        self._execute(sql, data.values())
        return self._query_dir(parent_id, name)

    def _insert_file_data(self, table_name, data_dic):
        keys = ', '.join(data_dic.keys())
        values = ', '.join(['%s'] * len(data_dic))
        userSql = 'INSERT INTO {table} ({keys}) VALUES ({values})'.format(table=table_name, keys=keys, values=values)
        self._execute(userSql, data_dic.values())

    def _del_file_pathId_name(self, path_id, name):
        userSql = "DELETE FROM {table} WHERE path_id={path_id} AND name='{name}'".format(table=db_file_table_name,
                                                                                         path_id=path_id, name=name)
        self._execute(userSql)

    def _del_file_data_table(self, table_name):
        userSql = "DROP TABLE {table_name};".format(table_name=table_name)
        self._execute(userSql)

    def _create_db(self):
        self.edfs_db = pymysql.connect(
            host=self.edfs_host,
            user=self.edfs_user,
            password=self.edfs_password,
            port=self.edfs_port
        )
        self.edfs_cursor = self.edfs_db.cursor()
        if (self.edfs_cursor.execute("show databases like '{db_name}'".format(db_name=db_name)) == 0):
            self._execute("CREATE DATABASE {db_name}".format(db_name=db_name))
        self._link_db()

    def _link_db(self):
        self.edfs_db = pymysql.connect(
            host=self.edfs_host,
            user=self.edfs_user,
            password=self.edfs_password,
            port=self.edfs_port,
            db=db_name
        )
        self.edfs_cursor = self.edfs_db.cursor()

    def _create_file_tab(self, tableName, keys, partition: EDFS_PARTITION):

        sql = "CREATE TABLE `{table}`  (`edfs_table_id` int(0) NOT NULL AUTO_INCREMENT".format(table=tableName)
        for key in keys:

            if partition != None and partition.partition_key == key:
                sql += ",`{key}` varchar(255) NOT NULL".format(key=key)
            else:
                sql += ",`{key}` varchar(500)".format(key=key)
        if partition != None:
            sql += ",PRIMARY KEY (`edfs_table_id`,`{key}`))".format(key=partition.partition_key)
            sql += "PARTITION BY KEY(`{name}`)".format(name=partition.partition_key)
            if partition.partition_count != None:
                sql += "PARTITIONS {count}".format(count=partition.partition_count)
        else:
            sql += ",PRIMARY KEY (`edfs_table_id`))"
        sql += ";"

        self._execute(sql)

    def _create_tab(self):
        self._execute("SHOW TABLES LIKE '{table}'".format(table=db_file_table_name))
        is_file_table = len(self.edfs_cursor.fetchall())
        if is_file_table == 0:
            self._execute(
                "CREATE TABLE `{table}`  (`id` int(0) NOT NULL AUTO_INCREMENT,`path_id` int(0) NOT NULL,`name` varchar(255) NOT NULL,PRIMARY KEY (`id`));".format(
                    table=db_file_table_name))
            self._execute(
                "CREATE TABLE `{table}`  (`id` int(0) NOT NULL AUTO_INCREMENT,`parent_id` varchar(255) NOT NULL,`name` varchar(255) NOT NULL,PRIMARY KEY (`id`));".format(
                    table=db_path_table_name))

    def _get_key_dict(self, cursor):
        key_dict = dict()
        index = 0
        for desc in cursor.description:
            key_dict[desc[0]] = index
            index = index + 1
        return key_dict

    def _execute(self, exsql, values=None):
        try:
            if values != None:
                self.edfs_cursor.execute(exsql, tuple(values))
            else:
                self.edfs_cursor.execute(exsql)
            self.edfs_db.commit()
        except Exception as error:
            self.edfs_db.rollback()
            # self.close()

    def _dataList(self):
        data = self.edfs_cursor.fetchall()
        dataList = []
        keys = self._get_key_dict(self.edfs_cursor)
        for datarow in data:
            listRow = dict()
            for key in keys:
                listRow[key] = datarow[keys[key]]
            dataList.append(listRow)
        return dataList

    def __exit__(self):
        self.close()

# Flask constructor
app = Flask(__name__)  
##command_list = []
## once the user hit the submit button, the process_command will get the form data
@app.route('/',methods = ['GET','POST'])
def process_command():
    
    edfs = EDFS("localhost", "root", "Qiqi140624..", 3306)
    if request.method == 'POST':
        k = EDFS_PARTITION("IATA",2)
        userinput = request.form.get('projectFilepath')
        command = userinput.split()[0]
        path = userinput.split()[1]
        my_input = userinput

        if command == "mkdir":
            var1 = edfs.mkdir(path)
            return render_template('index.html',mkdirname = var1, mkdirname2 = path)
        
        if command == 'ls':
            var2 = edfs.ls(path)
            #return (str(var2))
            
            return render_template('index.html', name = var2)
        
        if command == 'put':
            filename = my_input.split()[1]
            pathname = my_input.split()[2]
            var3 = edfs.put(filename,pathname,k)
            return render_template('index.html', value = var3)
        
        #edfs.cat("/user/path/airports.csv") only 1 parameter
        #html input: cat /user/path/airports.csv
        if command == 'cat':
            file_path = my_input.split()[1]
            var4 = edfs.cat(file_path) 
            display_form = var4.to_html(classes='data')
            #return render_template('index.html', form = display_form)
            return display_form
        # edfs.rm("/user/path/airports.csv")
        ##html input: rm /user/path/airports.csv
        if command == 'rm':
            file_name_to_rm = my_input.split()[1]
            print(file_name_to_rm)

            var5 = edfs.rm(file_name_to_rm)
            print(var5)
            #return var5
            return render_template('index.html',result = var5)
        #  edfs.getPartitionLocations("/user/path/airports.csv")
    #     data_list = edfs.readPartition("/user/path/airports.csv","p0")
        if command == 'partition':
            file_name_to_partition = my_input.split()[1]

            partLoc = edfs.getPartitionLocations(file_name_to_partition)
            #list_data = edfs.readPartition(file_name_to_partition,"p0")

            return(partLoc.to_html(classes='data'))
    return render_template("index.html")

    
    

if __name__ == '__main__':
    app.secret_key = 'super secret key'
    app.config['SESSION_TYPE'] = 'filesystem'
    app.run(debug=True,port=9005)
    process_command()
    
    #edfs = EDFS("localhost", "root", "Qiqi140624..", 3306)

    # edfs.mkdir("./dataset")
    # k = EDFS_PARTITION("IATA", 2)
    #put_cmd()
    #cat_cmd()
    # edfs.cat("./dataset/airports.csv")
    # edfs.rm("./dataset/airports.csv")
    # edfs.ls("./dataset")
    # edfs.getPartitionLocations("./dataset/airports.csv")
    # data_list = edfs.readPartition("./dataset/airports.csv", "p0")
    # where_dic = {
    #     "edfs_table_id > ": 100,
    #     "edfs_table_id < ": 200,
    # }
    # data_list = edfs.reduce("./dataset/airports.csv", where_dic)
    # print(data_list)
    
    #edfs.close()
