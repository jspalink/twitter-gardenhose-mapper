from setuptools import setup

setup(
    name='twitter_gardenhose_mapper',
    version='0.0.1',
    packages=['twitter_gardenhose_mapper'],
    package_dir={'': 'src'},
    url='',
    license='',
    author='Jonathan Spalink',
    author_email='jspalink@econtext.ai',
    description='Filter the twitter stream, and simply run it through the eContext API in order to publish content to eContext Kafka Stream',
    install_requires=['tweepy', 'econtext.util'],
    dependency_links=[
        'git+ssh://git@github.com/info-com/econtext.util#egg=econtext.util-1.0.3'
    ],
    
    entry_points={
        'console_scripts': [
            'twitter-gardenhose = twitter_gardenhose_mapper.main:main'
        ]
    }
)
