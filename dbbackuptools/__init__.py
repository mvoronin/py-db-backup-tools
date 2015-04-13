# -*- coding: utf-8 -*-

import sys
import os
import subprocess
import logging
from awshelpers.s3 import AWSS3
from fabric.context_managers import settings
from fabric.operations import sudo, run, local, put
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


def backup_download(aws_access_key_id, aws_secret_access_key, bucket, key_prefix, destination='temp'):
    """
    :type aws_access_key_id: str
    :param aws_access_key_id: AWS access key ID.

    :type aws_secret_access_key: str
    :param aws_secret_access_key: AWS secret access key.

    :type bucket: str
    :param bucket: Bucket name.

    :type key_prefix: str
    :param key_prefix: S3 key prefix.

    :type destination: str
    :param destination: Destination path for saving backup.

    :return: None
    """

    s3 = AWSS3(aws_access_key_id, aws_secret_access_key)
    s3.download_directory1(bucket, key_prefix, path_destination=destination)


def local_database_restore(path_to_backup, dbname):
    exit_code = subprocess.call(["pg_restore", "-d", "postgres", "-j", "3",
                                 "--create", "--exit-on-error", "--verbose", "-F", "d", path_to_backup])
    # -d dbname
    if exit_code > 0:
        logging.error("Error occurred during creating a backup!")
        sys.exit(1)


def database_restore(backup_path, db_host, db_user, db_password, db_name):
    exit_code = subprocess.call(["pg_restore", "-h", db_host, "-U", db_user, ""
                                 "-d", "postgres", "-j", "3",
                                 "--create", "--exit-on-error", "--verbose", "-F", "d", backup_path])

    if exit_code > 0:
        logging.error("Error occurred during creating a backup!")
        sys.exit(1)


def database_restore_locally(host, user, password, dbname, dba, backup_path):
    """
    :type host: str
    :param host: Host

    :type user: str
    :param user: SSH user name.

    :type password: str
    :param password: SSH user password.

    :type dbname: str
    :param dbname: Database name.

    :type dba: str
    :param dba: Database administrator user name.

    :type backup_path: str
    :param backup_path: Path to backup.

    :return: None
    """

    env.user = user
    env.password = password
    execute(task_database_restore_locally,
            dbname, dba,
            backup_path, '/home/%s/backup/' % user,
            hosts=[host])


def task_database_restore_locally(db_name, dba, backup_path, remote_backup_path):
    run('mkdir -p %s' % remote_backup_path)
    backup_dir_name = os.path.basename(backup_path)

    print backup_path, remote_backup_path
    put(backup_path, remote_backup_path)
    remote_backup_path = os.path.join(remote_backup_path, backup_dir_name)

    with settings(sudo_user='postgres'):
        sudo('createuser -S -D -R -P %s' % dba, warn_only=True)
        sudo("pg_restore -d postgres -j 3 --create --exit-on-error --verbose -F d %s" % remote_backup_path)
    local("rm -Rf %s" % backup_path)
    sudo("rm -Rf %s" % remote_backup_path)


def database_restore_from_s3(aws_access_key_id, aws_secret_access_key, bucket, key_prefix,
                             tmpdir, host, user, password, db, dba):
    """
    :type aws_access_key_id: str
    :param aws_access_key_id:

    :type aws_secret_access_key: str
    :param aws_secret_access_key:

    :type bucket: str
    :param bucket:

    :type key_prefix: str
    :param key_prefix:

    :type tmpdir: str
    :param tmpdir:

    :type host: str
    :param host:

    :type user: str
    :param user:

    :type password: str
    :param password:

    :type db: str
    :param db:

    :type dba: str
    :param dba:

    :return: None
    """

    #backup_download(aws_access_key_id, aws_secret_access_key, bucket, key_prefix, tmpdir)

    path_backup = ''
    prefix_length = len(key_prefix)
    for dirname in os.listdir(tmpdir):
        if dirname[:prefix_length] == key_prefix:
            print('%s == %s' % (dirname[:prefix_length], key_prefix))
            path_backup = os.path.join(tmpdir, dirname)
            print path_backup

    # database_restore(os.path.join(tmpdir, key_prefix), db_host, db_user, db_password, db_name)
    database_restore_locally(host, user, password, db, dba, path_backup)
