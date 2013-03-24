# coding=utf-8

import requests
import json
import types
import re

class Cloudant():
    '''
    Connect to cloudant
    '''

    def __init__(self, authname, authpasswd, username = None, db = None):
        '''
        Initalize a cloudant connection/auth
        '''
        self.authname = authname
        self.authpasswd = authpasswd
        '''
        When authenicating with a username and password, authname becomes the username (which is used in the base url). But when authenicating with api credentials, you must specify a username.
        '''
        if username == None:
            self.username = authname
        else:
            self.username = username
        self.db = db

    def httpcall(self, d, calltype = "GET"):
        '''
        Make HTTP calls 
        '''
        url = 'https://%s.cloudant.com%s' % (self.username, d['url'])
        auth = (self.authname,self.authpasswd)
        headers = {'content-type': 'application/json'}

        if calltype == "GET":
            r = requests.get(url, auth=auth)
        elif calltype == "POST":
            if d['data'] != None:
                r = requests.post(url, auth=auth, headers=headers, data=json.dumps(d['data']))
            else:
                r = requests.post(url, auth=auth)
        elif calltype == "PUT":
            if d['data'] != None:
                r = requests.put(url, auth=auth, headers=headers, data=json.dumps(d['data']))
            else:
                r = requests.put(url, auth=auth)
        elif calltype == "DELETE":
            if d['data'] != None:
                r = requests.delete(url, auth=auth, headers=headers, data=json.dumps(d['data']))
            else:
                r = requests.delete(url, auth=auth)
        elif calltype == "HEAD":
                r = requests.head(url, auth=auth)
        if calltype == "HEAD":
            d = r.headers
        else:
            d = r.json()
            if "error" in d: print "Error: %s" % d['reason']
        r.raise_for_status()
        return d

    def get(self, url = ''):
        ''' 
        Method for making an GET http request
        '''
        return self.httpcall({ 'url': url}, "GET")

    def post(self, url = '', data = None):
        ''' 
        Method for making an POST http request
        '''
        return self.httpcall({ 'url': url, 'data': data}, "POST")

    def put(self, url = '', data = None):
        ''' 
        Method for making an PUT http request
        '''
        return self.httpcall({ 'url': url, 'data': data}, "PUT")

    def delete(self, url = '', data = None):
        ''' 
        Method for making an DELETE http request
        '''
        return self.httpcall({ 'url': url, 'data': data}, "DELETE")

    def head(self, url = ''):
        ''' 
        Method for making an HEAD http request
        '''
        return self.httpcall( { 'url': url }, "HEAD")

    def hasDB(self):
        '''
        Tests to see if self has a db value
        '''
        if self.db == None:
            raise RuntimeError("This Cloudant object doesn't have a database name set")
            return False
        else:
            return True

    def getVersion(self):
        '''
        Get Cloudant version/build
        '''
        r = self.get()
        return { 'version': r['version'], 'build': r['cloudant_build'] }

    def listDBs(self):
        '''
        Get list of databases
        '''
        return self.get('/_all_dbs')

    def createDB(self, name = None):
        '''
        Create a new database
        '''
        if name == None: name = self.db
        if name != None:
            r = self.put('/%s' %name)
            return True
        else:
            return False

    def deleteDB(self, name = None):
        '''
        Delete a database
        '''
        if name == None: name = self.db
        if name != None:
            r = self.delete('/%s' %name)
            return True
        else:
            return False

    def insert(self, data, key = None):
        '''
        Insert a document into the database
        '''
        if self.hasDB:
            if isinstance(data, types.ListType):
                # is list, therefore, is bulk insert
                data = { 'docs': data }
                return self.post('/%s/_bulk_docs' % self.db, data)
            else:
                # is single doc
                if key == None:
                    return self.post('/%s' % self.db, data)
                else:
                    # key provided, using it
                    return self.put('/%s/%s' % (self.db, key), data)
        else:
            return False

    def read_doc(self, key):
        '''
        Read a doc from the db via key
        '''
        if self.hasDB:
            return self.get('/%s/%s' % (self.db, key))
        else:
            return False

    def delete_doc(self, key, rev = None):
        '''
        Delete a doc from the db via key & doc
        If a rev is not specified, one is found and used
        '''
        if self.hasDB:
            if rev == None:
                r = self.head('/%s/%s' % (self.db, key))
                rev = re.sub('"', '', r['etag'])
            return self.delete('/%s/%s?rev=%s' % (self.db, key, rev))
        else:
            return False

    def all_docs(self, name = None):
        '''
        Get all documents in a database
        '''
        if name == None: name = self.db
        if name != None:
            return self.get('/%s/_all_docs' % name)
        else:
            return False

