#!/usr/bin/env python3

"""
Subpackage to handle rsyncing from within Python.
"""

import os
import subprocess


def rsync(source, dest):
    """
    rsync the data from a remote server to the local server.

    @param source The source file/path.
    @param dest The destination file/path.
    """

    print(f"Copying {source} to {dest}.\n")
    subprocess.run(['rsync', '-zvth', source, dest], check=True)
    print("")


class RsyncContext():
    """
    Holds the information for an rsync transfer context.
    """

    def __init__(self, remote_user, remote_server, remote_base_path,
                 local_base_path):
        """
        Initialize the RsyncContext.

        @param remote_user The remote username.
        @param remote_server The remote server we are transferring to/from.
        @param remote_base_path The remote base path for file transfers.
        @param local_base_path The local base path for file transfers.
        """

        self.remote_user = remote_user
        self.remote_server = remote_server
        self.remote_base_path = remote_base_path
        self.local_base_path = local_base_path
        self.remote_spec = f'{remote_user}@{remote_server}:{remote_base_path}'

    def pull(self, source, dest):
        """
        Pulls the given source file (or dir) at remote in `remote_base_path` to
        local file (or dir) dest path. Follows rsync semantics with trailing
        slashes and so on.

        @param source The source file/path.
        @param dest The destination file/path.
        """

        full_source_path = os.path.join(self.remote_spec, source)
        full_dest_path = os.path.join(self.local_base_path, dest)
        os.makedirs(full_dest_path, exist_ok=True)
        rsync(full_source_path, full_dest_path)

    def push(self, source, dest):
        """
        Pushes the given source file (or dir) at `local_base_path` to remote
        file (or dir) dest path at remote. Follows rsync semantics with
        trailing slashes and so on.

        @param source The source file/path.
        @param dest The destination file/path.
        """

        full_source_path = f'{self.local_base_path}/{source}'
        full_dest_path = f'{self.remote_spec}/{dest}'
        rsync(full_source_path, full_dest_path)

    def pull_list(self, file_list: list):
        """
        Pulls the given files as a list of dictionaries corresponding to
        `remote_base_path` + `source` as the source, and `local_base_path`
        + `dest` as the destination.

        @param file_list The list of dictionaries of files to pull.
        """

        for i in file_list:
            self.pull(i['source'], i['dest'])

    def push_list(self, file_list: list):
        """
        Pushes the given files as a list of dictionaries corresponding to
        `local_base_path` + `source` as the source and `remote_base_path` +
        `dest` as the destination.

        @param file_list The list of dictionaries of files to push.
        """

        for i in file_list:
            self.push(i['source'], i['dest'])
