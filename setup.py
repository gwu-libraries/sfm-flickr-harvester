from setuptools import setup

setup(
    name='sfmflickrharvester',
    version='1.8.0',
    url='https://github.com/gwu-libraries/sfm-flickr-harvester',
    author='Justin Littman',
    author_email='justinlittman@gmail.com',
    description="Social Feed Manager Flickr Harvester",
    platforms=['POSIX'],
    test_suite='tests',
    scripts=['flickr_harvester.py',
             'flickr_warc_iter.py'],
    install_requires=['sfmutils'],
    tests_require=['mock>=1.3.0'],
    classifiers=[
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2.7',
        'Development Status :: 4 - Beta',
    ],
)