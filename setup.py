from setuptools import setup, find_packages

setup(
    name='unify',
    version='0.1.0',
    author='Unify',
    author_email='hello@unify.com',
    description='A Python package for interacting with the Unify API',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/unifyai/unify',
    packages=find_packages(),
    install_requires=[
        'openai'  # Make sure to specify the correct version of openai library
    ],
    #not sure about license
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache-2 License',
        'Operating System :: OS Independent',
    ],
)
