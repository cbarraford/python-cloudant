try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import setup, Command

import sys
from imp import load_source

class TestCommand(Command):
    user_options = [ ]

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        '''
        Finds all the tests modules in tests/, and runs them.
        '''
        import tests
        import unittest
        unittest.main()

version_tuple = ( 0,0,1 )

if version_tuple[2] is not None:
    version = "%d.%d_%s" % version_tuple
else:
    version = "%d.%d" % version_tuple[:2]

setup(
    name = "python-cloudant",
    version = version,
    url = 'https://github.com/CBarraford/python-cloudant',
    author = 'Chad Barraford',
    author_email = 'cbarraford@gmail.com',
    maintainer = 'Chad Barraford',
    maintainer_email = 'cbarraford@gmail.com',
    description = 'A python module for interacting with Cloudant',
    license = "MIT",
    cmdclass = {'test': TestCommand}
)
