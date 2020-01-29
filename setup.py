from setuptools import setup
import streamsx.standard._version
setup(
  name = 'streamsx.standard',
  packages = ['streamsx.standard'],
  include_package_data=True,
  version = streamsx.standard._version.__version__,
  description = 'IBM Streams standard toolkit',
  long_description = open('DESC.txt').read(),
  author = 'IBM Streams @ github.com',
  author_email = 'debrunne@us.ibm.com',
  license='Apache License - Version 2.0',
  url = 'https://github.com/IBMStreams/streamsx.standard',
  keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics'],
  classifiers = [
    'Development Status :: 3 - Alpha',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
  install_requires=['streamsx>=1.14.2a0,<2.0'],
  
  test_suite='nose.collector',
  tests_require=['nose']
)
