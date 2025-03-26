from pymongo import MongoClient, UpdateOne


class DatabaseManager:
    def __init__(self, host:str, db:str=None, collection:str=None):
        self.host = host
        self.db_name = db
        self.collection_name = collection
        self.client = MongoClient(self.host)
        self.db = self.client[self.db_name] if db is not None else None
        self.collection = self.db[self.collection_name] if (self.db_name is not None and self.collection_name is not None) else None

    def insert_one(self, doc:dict, delete:bool=False, filter:dict={}):
        try:
            if delete and filter != {}: self.collection.delete_one(filter)
            self.collection.insert_one(doc)
        except Exception as e:
            raise Exception('Erro interno no servidor')
        
    def insert_many(self, docs:list, delete:bool=False, filter:dict={}):
        try:
            if delete: self.delete_many(filter)
            self.collection.insert_many(docs)
        except: raise Exception('Erro interno no servidor')

    def handle_db(self, db:str):
        try:
            self.collection = None
            self.db_name = db
            self.db = self.client[self.db]
        except:
            raise Exception('Erro interno no servidor')

    def handle_collection(self, collection:str):
        try:
            self.collection_name = collection
            self.collection = self.db[self.collection_name]
        except:
            raise Exception('Erro interno no servidor')

    def update_one(self, doc:dict, filter:dict, upsert:bool=False):
        try:
            self.collection.update_one(filter, {'$set': doc}, upsert)
        except:
            raise Exception('Erro interno no servidor')
    
    def update_many(self, docs:list, filter:dict, upsert:bool=False):
        try:
            operations = [UpdateOne(filter, {'$set': doc}, upsert=upsert) for doc in docs]
            if operations: self.collection.bulk_write(operations)
        except: raise Exception('Erro interno no servidor')

    def delete_many(self, filter:dict):
        try:
            self.collection.delete_many(filter)
        except:
            raise Exception('Erro interno no servidor')
    
    def find(self, filter:dict):
        try:
            return list(self.collection.find(filter, {'_id': 0}))
        except: raise Exception('Erro interno no servidor')

    def find_one(self, filter:dict, remove:dict={'_id': 0}):
        try:
            doc = self.collection.find_one(filter, remove)
            return {} if not doc else doc
        except: raise Exception('Erro interno no servidor')

    def close(self):
        if self.client: self.client.close()