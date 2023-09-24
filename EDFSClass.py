#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
from csv import DictReader
import pandas as pd
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
        print("mkdir success")

    def put(self, file_name, path, k: EDFS_PARTITION = None):
        file_type = file_name.split(".")
        if (len(file_type) == 2 and (file_type[1] == "csv" or file_type[1] == "CSV")) == False:
            print("Error: Please upload a file in CSV format")
            return
        path_id = self._check_dir_path(path)
        if (path_id == False):
            print("Error: Path does not exist")
            return
        dataList = self._query_file_pathId_name(path_id, file_name)
        if len(dataList) != 0:
            print("Error: A file with the same name exists in this directory")
            return
        fileId = self._inster_file(path_id, file_name)
        fileId = fileId[0]["id"]
        cwd = Path.cwd()
        print(type(cwd))
        print(cwd)
        with open(path + "/" + file_name, mode="r", encoding='utf-8') as read_obj:
            dict_reader = DictReader(read_obj)
            list_of_dict = list(dict_reader)
            table_name = file_type[0] + "_" + str(fileId)
            self._create_file_tab(table_name, list_of_dict[0].keys(), k)
            for dic in list_of_dict:
                self._insert_file_data(table_name, dic)
        print("put file success")

    def cat(self, file_path):
        dataList, file_name, path_id = self.getDataList(file_path)
        if len(dataList) == 0:
            print("Error: file does not exist")
            return
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        dataList = self._query_file_data(table_name)
        df = pd.DataFrame(dataList)
        print(df)

    def rm(self, file_path):
        dataList, file_name, path_id = self.getDataList(file_path)
        if len(dataList) == 0:
            print("Error: file does not exist")
            return
        self._del_file_pathId_name(path_id, file_name)
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        self._del_file_data_table(table_name)
        print("remove file success")

    def ls(self, path):
        file_names = []
        path_id = self._check_dir_path(path)
        if path_id == False:
            print("Error: Path does not exist")
            return file_names
        file_list = self._query_file_pathId_name(path_id)
        dir_list = self._query_dir(path_id)
        for data in file_list:
            file_names.append(data['name'])
            print(data["name"], end="    ")
        for data in dir_list:
            print(data["name"], end="    ")
        print("")
        return file_names

    def getPartitionLocations(self, file):
        dataList, file_name, path_id = self.getDataList(file)
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        dataList = self._query_file_data_partition_info(table_name)
        df = pd.DataFrame(dataList)
        print(df)

    def get_tb_columns(self, table_name):
        columns = []
        sql = f"show columns from {table_name} from {db_name}".format(table_name=table_name, db_name=db_name)
        self._execute(sql)
        data_list = self._dataList()
        for data in data_list:
            columns.append(data['Field'])
        return columns

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
            return
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(dataList[0]["id"])
        dataList = self._query_file_data_partition_data(table_name, partition)
        return dataList

    def mapPartititon(self, table_name, partition, where_dic=None):
        partitionData = self._query_file_data_partition_data(table_name, partition, where_dic)
        return partitionData

    def reduce(self, file, where_dic=None):
        data_list, file_name, path_id = self.getDataList(file)
        if len(data_list) == 0:
            print("Error: file does not exist")
            return
        file_type = file_name.split(".")
        table_name = file_type[0] + "_" + str(data_list[0]["id"])
        table_data = self.query_data_from_table(table_name, where_dic)
        return table_data['reduce']

    def query_data_from_table(self, table_name, where_dic=None):
        info_list = self._query_file_data_partition_info(table_name)
        output = {
            'partitions': [],
            'reduce': []
        }
        # if table has no partition
        if not info_list or not info_list[0]['PARTITION_NAME']:
            output['reduce'] = self._query_file_data(table_name, where_dic)
        else:
            # table has partitions
            for data in info_list:
                map_data = self.mapPartititon(table_name, data['PARTITION_NAME'], where_dic)
                output['partitions'].append({
                    'name': data['PARTITION_NAME'],
                    'data': map_data
                })
                # combine data
                output['reduce'] += map_data
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



