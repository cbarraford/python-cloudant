try:
    import unittest2 as unittest
except ImportError:
    import unittest
import types
import os, sys
import requests

# add classes location to sys.path
cmd_folder = os.path.dirname(os.path.dirname((os.path.abspath(__file__))))
if (cmd_folder + "/cloudant") not in sys.path:
    sys.path.insert(0, cmd_folder + "/cloudant")

import cloudant as c

class TestCloudant(unittest.TestCase):
    '''
    Test units for Cloudant
    '''

    def setUp(self):
        self.username = os.environ['Cloudant_authusername']
        self.password = os.environ['Cloudant_authpasswd']
        self.db = "testunit_db"
        self.testinsert = { "season": "summer", "weather": "usually warm and sunny" }
        self.c = c.Cloudant(self.username, self.password, self.username, self.db)

    def test_1_get_version(self):
        self.version = self.c.get_version()
        assert isinstance(self.version, types.DictType)
        assert len(self.version['version']) > 0
        assert len(self.version['build']) > 0

    def test_2_list_dbs(self):
        self.dbs = self.c.list_dbs()
        assert isinstance(self.dbs, types.ListType)

    def test_3_create_db(self):
        t = self.c.create_db(self.db)
        assert t == True
        assert self.db in self.c.list_dbs()

    def test_4_insert(self):
        t = self.c.insert(self.testinsert)
        self.insertkey = t['id']
        s = self.c.read_doc(t['id'])
        del s['_id']
        del s['_rev']
        assert self.testinsert == s

    def test_5_all_docs(self):
        t = self.c.all_docs(self.db, {'limit':1})
        assert "rows" in t
        assert "total_rows" in t
        assert "offset" in t

    def test_6_delete_doc(self):
        all_docs = self.c.all_docs()
        doc = all_docs['rows'][0]
        t = self.c.delete_doc(doc['id'])
        r = requests.get("https://%s.cloudant.com/%s/%s" %(self.username, self.db, doc['id']), auth=(self.username, self.password))
        d = r.json()
        assert d['error'] == "not_found"
        assert d['reason'] == "deleted"

#    def test_7_insert_attachment(self):
#        r = self.c.upload_file('file', "image/png", "tests/cloudant.png")
#        print r
#        assert r['ok'] == True

    def test_8_list_design_docs(self):
        view_docs = self.c.listDesignDocs()
        assert isinstance(view_docs['rows'], types.ListType)

    def test_9_get_design_docs(self):
        views = self.c.getDesignDocs()
        assert isinstance(views['rows'], types.ListType)

    def test_zLast_delete_db(self):
        t = self.c.delete_db(self.db)
        assert t == True
        assert self.db not in self.c.list_dbs()

if __name__ == '__main__':
    unittest.main()
