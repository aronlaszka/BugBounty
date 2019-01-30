from setuptools import setup, find_packages
setup(
    name='crawler',
    version='0.5',
    packages=find_packages(),
    py_modules=['cli.py'],
    author='Taha Eghtesad',
    author_email='tahaeghtesad@gmail.com',
    install_requires=[
        'Click',
        'elasticsearch',
        'neo4j-driver',
        'python-twitter',
        'requests-aws4auth',
    ],
    dependency_links=[
        'git+https://github.com/bear/python-twitter#egg=python_twitter'
    ],
    entry_points={
        'console_scripts': [
            'crawl = cli.cli:cli'
        ]
    }

)