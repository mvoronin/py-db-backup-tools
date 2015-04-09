from setuptools import setup

setup(name='dbbackuptools',
      version='0.1',
      description='',
      url='http://github.com/mvoronin/py-db-backup-tools',
      author='Mikhail Voronin',
      author_email='contact@mvoronin.pro',
      license='Apache License, Version 2.0',
      classifiers=[
          'Development Status :: 4 - Beta',
          'License :: OSI Approved :: Apache Software License',

          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7'
      ],
      packages=['dbbackuptools'],
      zip_safe=False,
      install_requires=['Fabric'],)