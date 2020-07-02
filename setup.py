from setuptools import setup
import streamsx.standard._version
setup(
  name = 'streamsx.standard',
  packages = ['streamsx.standard'],
  include_package_data=True,
  version = streamsx.standard._version.__version__,
  description = 'Standard toolkit integration for IBM Streams',
  long_description = open('DESC.txt').read(),
  author = 'IBM Streams @ github.com',
  author_email = 'hegermar@de.ibm.com',
  license='Apache License - Version 2.0',
  url = 'https://github.com/IBMStreams/pypi.streamsx.standard',
  keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics'],
  classifiers = [
    'Development Status :: 5 - Production/Stable',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
  ],
  install_requires=['streamsx>=1.14.6,<2.0'],
  
  test_suite='nose.collector',
  tests_require=['nose']
)
