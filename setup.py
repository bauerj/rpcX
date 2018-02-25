import setuptools

import rpcx


setuptools.setup(
    name='rpcX',
    version=rpcx.rpcX_version_str,
    python_requires='>=3.6',
    packages=setuptools.find_packages(exclude=['tests']),
    description='Generic RPC implementation, including JSON-RPC',
    author='Neil Booth',
    author_email='kyuupichan@gmail.com',
    license='MIT Licence',
    url='https://github.com/kyuupichan/rpcX/',
    long_description=(
        'Transport, protocol and framing-independent RPC client and server '
        'implementation.  '
    ),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        "Programming Language :: Python :: 3.6",
        'Topic :: Internet',
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
