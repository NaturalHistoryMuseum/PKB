
from setuptools import find_packages, setup

setup(
    name="pkb",
    version="0.1",
    description='pkb',
    # url='http://github.com/benscott/pkb',
    author='Ben Scott',
    author_email='ben@benscott.co.uk',    
    packages=["pkb"],
    entry_points={
        'console_scripts': [
            # 'pkb = pkb.cli:cli',
        ],
    },


)
