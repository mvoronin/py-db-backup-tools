# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import sys
import subprocess
import logging
from awshelpers.s3 import AWSS3
from fabric.context_managers import settings
from fabric.operations import sudo, run, put
from fabric.state import env
from fabric.tasks import execute


def local_backup_create(backup_path, db_name):
    exit_code = subprocess.call(["pg_dump", "-j", "3", "-F", "d", "-f", backup_path, db_name])
    if exit_code > 0:
        logging.error("Error occurred during creating a backup!")
        sys.exit(1)


def backup_upload(backup_path, str_aws_access_key_id, str_aws_secret_access_key, str_bucket, key_prefix):
    s3 = AWSS3(str_aws_access_key_id, str_aws_secret_access_key)
    s3.upload_directory1(str_bucket, backup_path)


def backup_download(str_aws_access_key_id, str_aws_secret_access_key, str_bucket, key_prefix, destination='temp'):
    s3 = AWSS3(str_aws_access_key_id, str_aws_secret_access_key)
    s3.download_directory1(str_bucket, key_prefix, path_destination=destination)


def local_database_restore(path_to_backup, dbname):
    exit_code = subprocess.call(["pg_restore", "-d", "postgres", "-j", "3",
                                 "--create", "--exit-on-error", "--verbose", "-F", "d", path_to_backup])
    # -d dbname
    if exit_code > 0:
        logging.error("Error occurred during creating a backup!")
        sys.exit(1)


def remote_database_restore(path_to_backup, dbname, host, login, passwd):
    env.user = login
    env.password = passwd
    execute(task_database_restore, path_to_backup, hosts=[host])


def task_database_restore(path_to_backup):
    remote_path_to_backup = '/home/%s/backup' % env.user
    put(path_to_backup, remote_path_to_backup)
    with settings(sudo_user='postgres'):
        sudo("pg_restore -d postgres -j 3 --create --exit-on-error --verbose -F d %s" % remote_path_to_backup)
    run("rm -Rf %s" % remote_path_to_backup)
