import pymongo

class MongoProvider(object):
    
    def __init__(self, uri, db):
        self.mongo_uri = uri
        self.mongo_db = db
    
    def get_db(self):
        self.client = pymongo.MongoClient(self.mongo_uri)
        return self.client[self.mongo_db]
    
    def close_connection(self):
        self.client.close()