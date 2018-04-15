import os
import subprocess
import threading

from dqrest import app
from dqrest.common.hdfs import HDFSLOGDIR, save_to_hdfs


def popen_and_call(on_exit, on_exit_args, popen_args):
    """Execute a Popen with <popen_args> and then calls <on_exit> function with <on_exit_args> and the exit code of the process.

    The function also stores on hdfs the log of the process at HDFSLOGDIR.

    Args:
    on_exit(function): function to call after popen process
    on_exit_args(list): on_exit function args
    popen_args(list): args to pass to popen function
    """
    def runInThread(on_exit, on_exit_args, popen_args):
        with app.app_context():
            log_file_path = '/logs/%s.txt' % (str(on_exit_args))
            log_file = open(log_file_path, 'w')
            proc = subprocess.Popen(popen_args, stdout=log_file)
            proc.wait()
            error = proc.returncode
            log_file.flush()
            log_file_remote_path = HDFSLOGDIR + \
                '/' + str(on_exit_args) + '.txt'
            save_to_hdfs(log_file_path, log_file_remote_path)
            log_file.close()
            on_exit(on_exit_args, error)
            return
    thread = threading.Thread(target=runInThread,
                              args=(on_exit, on_exit_args, popen_args))
    thread.start()
    return thread
