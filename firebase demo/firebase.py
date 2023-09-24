# -*- coding: utf-8 -*-
import requests
import sys
import pandas as pd
import pyrebase

Config = {
  'apiKey': "AIzaSyDkW3aORjjODW5mB4b9VSYjSQPJTTi4YFs",
  'authDomain': "dsci551-group47-project.firebaseapp.com",
  'databaseURL': "https://dsci551-group47-project-default-rtdb.firebaseio.com",
  'projectId': "dsci551-group47-project",
  'storageBucket': "dsci551-group47-project.appspot.com",
  'messagingSenderId': "732356400242",
  'appId': "1:732356400242:web:7c538b48d914d213751eeb",
  'measurementId': "G-EX56TCKHE7"
}

class EDFS_PARTITION:
    def __init__(self,partition_key,partition_count):
        self.partition_key = partition_key
        self.partition_count =partition_count


firebase = pyrebase.initialize_app(Config)
database = firebase.database()
## load the csv file to firebase
baseURL = 'https://dsci551-group47-project-default-rtdb.firebaseio.com/'


dataframe = pd.read_csv('dummycsv.csv',header = 0)
result = dataframe.to_json(orient='index')

dummy_data={"name":"john","name":"john"}
#requests.put(url + userpath + '/.json',dummy_dict)

def mkdir():
    userfilePath = sys.argv[1]
    print(userfilePath)
    requests.put(baseURL+ userfilePath + '/.json',result)

def ls():
    userfilePath = sys.argv[1]
    try:
        nodes = database.child(userfilePath).get().val().keys()
        print(list(nodes))
    except AttributeError:
        print('The path you enter is wrong, cannot perform ls on a file')
        
def rm():
    userfilePath = sys.argv[1]
    #nodes = database.child('root').child('user3').child('0').get().val()
    nodes = database.child(userfilePath).remove()
    #print(nodes)
def put():
    userfile = sys.argv[1]
    userfilePath = sys.argv[2]
    dataframe = pd.read_csv(userfile,header = 0)
    result = dataframe.to_json(orient='index')
    print(baseURL+ userfilePath + '/.json')
    requests.put(baseURL+ userfilePath + '/.json',result)

##cat("/user/path/airports.csv")
def cat():
    userinput = sys.argv[1] #"/user/path/airports.csv"
    split = userinput.split('/')
    filename = split[-1]
    name = filename.split('.')[0]
    path = '/'.join(split[:-1])
    userfilePath  = path + '/'+ name
    nodes = database.child(userfilePath).get().val()
    print(list(nodes))
    #print(userfilepath)

if __name__ == '__main__':
    #python3 firebase.py root/user0/john -- create a directory, under root/user0
    #mkdir()
    ## python3 firebase.py root/user0 
    #ls()
    #rm()
    #python3 firebase.py airports.csv root/user0
    #put()
    cat()