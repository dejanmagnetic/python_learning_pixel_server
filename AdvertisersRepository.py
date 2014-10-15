import pymongo

# in mongodb pixel database execute this couple times to create advertisers
# db.advertiser.insert({"active" : True})

class Repository(object):
    def __init__(self):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['pixel']
        self.advertisers = self.db['advertiser']
        self.cached = set()
        self.reload_active_advertisers()


    def reload_active_advertisers(self):
        advertisers_find = self.advertisers.find({"active": True})
        self.cached = set([str(record['_id']) for record in advertisers_find])

    def get_active_advertisers(self):
        return self.cached

    def close_connection(self):
        self.client.close()