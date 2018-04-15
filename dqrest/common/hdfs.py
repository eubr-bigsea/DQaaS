import json
import os
import shlex
import subprocess

from snakebite.client import Client

hdfs_client = Client('10.0.0.21', 9000)
HDFSURL = 'hdfs://10.0.0.21:9000'
HDFSHOMEDIR = '/tmp'
HDFSLOGDIR = HDFSHOMEDIR + '/logs'
HDFSCONFIGDIR = HDFSHOMEDIR + '/config_files'

"""
HDFS I/O
"""


def cat_file(path):
    """Wrapper for snakebite cat function.

    This function provides a simple wrapper to snakebite to simply get the content of a specific file from hdfs,
    it is intended as a mean to quickly access *small* files.

    Args:
        path: Complete path to the file.

    Returns:
        A string with the file content, empty in case of missing file.
    """
    try:
        generator_list = list(hdfs_client.cat([path]))
        for elems in generator_list:
            for elem in elems:
                assessment_file = elem
        return assessment_file
    except:
        return ""


def ls_files(path):
    """Wrapper for snakebite ls function.

    This function provides a simple wrapper to snakebite to simply get the names of the files contained in an hdfs folder.

    Args:
        path: Complete path of the folder.

    Returns:
        A list of strings corresponding to the names of the files in the requested folder, empty in case of missing folder.
    """
    try:
        generator_list = list(hdfs_client.ls([path]))
        dict_list = [json.loads(d) for d in generator_list]
        return [d['path'] for d in dict_list]
    except:
        return []


def save_to_hdfs(local_path, hdfs_path):
    """Moves a file from local to hdfs

    Attention: the file will be removed from is current local position.

    Args:
        local_path: Source complete path.
        hdfs_path: Destination path.
    """
    command = 'hdfs dfs -moveFromLocal %s %s' % (local_path, hdfs_path)
    args = shlex.split(command)
    proc = subprocess.Popen(args)
    proc.wait()


def create_config_dir():
    if not os.path.exists('/temp_config_files'):
        os.makedirs('/temp_config_files')
    list(hdfs_client.delete([HDFSCONFIGDIR], True))
    list(hdfs_client.mkdir([HDFSCONFIGDIR]))


def create_log_dir():
    if not os.path.exists('/logs'):
        os.makedirs('/logs')
    list(hdfs_client.delete([HDFSLOGDIR], True))
    list(hdfs_client.mkdir([HDFSLOGDIR]))
