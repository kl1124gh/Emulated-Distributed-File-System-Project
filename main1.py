from EDFSClass import EDFS, EDFS_PARTITION
if __name__ == '__main__':
    edfs = EDFS("localhost", "root", "Qiqi140624..", 3306)
    edfs.mkdir("./root/dataset")
    k = EDFS_PARTITION("IATA", 2)
    #edfs.put("airports.csv", "./dataset", k)
    edfs.cat("./dataset/airports.csv")
    #edfs.rm("./dataset/airports.csv")
    #edfs.ls("./dataset")
    #edfs.getPartitionLocations("./dataset/airports.csv")
    # data_list = edfs.readPartition("./dataset/airports.csv", "p0")
    where_dic = {
        "edfs_table_id > ": 100,
        "edfs_table_id < ": 200,
    }
    #data_list = edfs.reduce("./dataset/airports.csv", where_dic)
    #print(data_list)
    edfs.close()