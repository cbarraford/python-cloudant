# coding=utf-8

import requests
import urllib
import json
import types
import re, os
import mimetypes

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
        When authenicating with a username and password, authname becomes the 
        username (which is used in the base url). But when authenicating with 
        api credentials, you must specify a username.
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
            print url
            r = requests.get(url, auth=auth)
        elif calltype == "POST":
            if d['data'] != None:
                r = requests.post(url, \
                                  auth=auth, \
                                  headers=headers, \
                                  data=json.dumps(d['data']))
            else:
                r = requests.post(url, auth=auth)
        elif calltype == "PUT":
            if d['data'] != None:
                r = requests.put(url, \
                                 auth=auth, \
                                 headers=headers, \
                                 data=json.dumps(d['data']))
            else:
                r = requests.put(url, auth=auth)
        elif calltype == "DELETE":
            if d['data'] != None:
                r = requests.delete(url, \
                                    auth=auth, \
                                    headers=headers, \
                                    data=json.dumps(d['data']))
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

    def has_db(self):
        '''
        Tests to see if self has a db value
        '''
        if self.db == None:
            e = "This Cloudant object doesn't have a database name set"
            raise RuntimeError(e)
            return False
        else:
            return True

    def get_version(self):
        '''
        Get Cloudant version/build
        '''
        r = self.get()
        return { 'version': r['version'], 'build': r['cloudant_build'] }

    def list_dbs(self):
        '''
        Get list of databases
        '''
        return self.get('/_all_dbs')

    def create_db(self, name = None):
        '''
        Create a new database
        '''
        if name == None: name = self.db
        if name != None:
            r = self.put('/%s' %name)
            return True
        else:
            return False

    def delete_db(self, name = None):
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
        if self.has_db:
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
        if self.has_db:
            return self.get('/%s/%s' % (self.db, key))
        else:
            return False

    def delete_doc(self, key, rev = None):
        '''
        Delete a doc from the db via key & doc
        If a rev is not specified, one is found and used
        '''
        if self.has_db:
            if rev == None:
                r = self.head('/%s/%s' % (self.db, key))
                rev = re.sub('"', '', r['etag'])
            return self.delete('/%s/%s?rev=%s' % (self.db, key, rev))
        else:
            return False

    def all_docs(self, name = None, params = {}):
        '''
        Get all documents in a database
        For a list of acceptable parameters, see https://cloudant.com/for-developers/all_docs/
        '''
        if name == None: name = self.db
        if name != None:
            return self.get('/%s/_all_docs?%s' % ( name, urllib.urlencode(params) ))
        else:
            return False

    def upload_file(self, key, content_type, the_file):
        '''
        Upload a file to the database FIXME: needs lots of work (NON-FUNCTIONAL)
        '''
        f = open(the_file, 'rb')
        fname = os.path.basename(the_file)
        data = {}
        data['_id'] = key
        tmp = {'content_type': 'image/png', 'data': f.read()}
        data['_attachments'] = { fname: tmp }
        url = 'https://%s.cloudant.com/%s' % (self.username, self.db)
        auth = (self.authname,self.authpasswd)
        headers = {'content-type': 'application/json'}
        r = requests.post(url, auth=auth, headers=headers, data=data)
        d = r.json()
        if "error" in d: print "Error: %s" % d['reason']
        r.raise_for_status()
        return d

    def get_secondary_indexes(self):
        '''
        Returns all design docs and their content
        '''
        if self.has_db:
            docs = self.all_docs( params = { 'startkey': '"_design"', "endkey": '"_design0"', 'include_docs': 'true' } )
            if docs['rows']:
                for i in docs['rows']:
                   pass 
            else:
                return False
        else:
            return False

class Secondary_Index():
    '''
    A Class for secondary indexes
    '''    
    def __init__(self, data):
        '''
        Initalize a secondary index
        '''
        _id = data['_id']
        _rev = data['_rev']
        indexes = data['indexes']
        views = data['views']

    def save(self, name):
        '''
        Save a secondary view
        '''
        if name == None: name = self.db
        if name != None:
            return self.put('/%s' % ( self._id, name ))
        else:
            return False

    def get(self, name, params = {}):
        '''
        Get results of a secondary index
        '''
        if name == None: name = self.db
        if name != None:
            return self.get('/%s/_view/%s?%s' % ( self._id, name, urllib.urlencode(params) ))
        else:
            return False


