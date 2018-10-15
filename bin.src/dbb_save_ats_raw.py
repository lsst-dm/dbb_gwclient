#!/usr/bin/env python
#
# This file is part of dbb_gwclient.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

""" Prototype program to manually save ATS raw files to the
    gateway of the Data Backbone
"""

import argparse
import copy
import io
import logging
import os
import re
import socket
import subprocess
import shutil
import shlex
import sys
import tarfile
import time
import uuid

import psutil
import yaml

from lsst.dbb.gwclient.chksum_utils import calc_chksum


def parse_args(argv=None):
    """Parse command line, and test for required arguments

    Parameters
    ----------
    argv : `list`
        List of strings containing the command-line arguments.

    Returns
    -------
    args : `Namespace`
        Command-line arguments converted into an object with attributes.
    """
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("files", action="store", nargs="+", metavar="FILE",
                        help="ATS raw files or directories of ATS raw files to save to Data Backbone")

    # Making provmsg named argument to help ensure that a first filename is not
    # used for a provmsg accidentally
    required = parser.add_argument_group('required arguments')
    required.add_argument("-m", "--provmsg", action="store", dest="provmsg", required=True,
                          help="Message to be stored in DBB as part of ingestion provenance")

    parser.add_argument("-p", "--prefix", action="store", dest="prefix", required=False,
                          help="Data Backbone Gateway HTTP URL prefix\n(default:%(default)s)",
                          default="https://lsst-dbb-gw.ncsa.illinois.edu")
    parser.add_argument("-n", "--num_tries", action="store", dest="num_tries", type=int, required=False,
                          help="Number of times to retry transfer", default=5)
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", required=False,
                          help="Print file level output useful for watching progress")
    parser.add_argument("-d", "--debug", action="store_true", dest="debug", required=False,
                          help="Print very verbose output for debugging")
    parser.add_argument("--dryrun", action="store_true", dest="dryrun", required=False,
                          help="If set, does not actually transfer file")

    return parser.parse_args(argv)


def get_lsst_user():
    """Get canonical lsst username

    Returns
    -------
    user : `str`
        LSST user name.

    Raises
    ------
    FileNotFoundError
        Raised when klist command is not in path or could not find
        Kerberos ticket.
    ValueError
        Raised when klist output doesn't have an NCSA.EDU default principal.
    """
    # since local logins can be different than LSST user names
    # parse Kerberos information for user name
    # (can assume Kerberos's klist should exist as Kerberos is the
    # authentication method for the file transfer)

    klist_path = shutil.which("klist")
    if klist_path is None:
        raise FileNotFoundError("klist command is not in path")

    process = subprocess.Popen("klist",
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)

    out = process.communicate()[0].decode("utf-8")
    if process.returncode != 0 and "not found" in out:
        raise FileNotFoundError("Could not find valid Kerberos ticket")
    match = re.search(r"Default principal: (\S+)@NCSA.EDU", out)
    if match is None:
        raise ValueError("Output of klist didn't have an NCSA.EDU default principal")
    user = match.group(1)
    return user


def create_physical_data(filename, chksum, chksum_type, common_info):
    """Create a dictionary storing information about the chksum digest file

    Parameters
    ----------
    filename : `str`
        Name of file to save to Data Backbone.   Includes any local path.
    chksum : `str`
        Hexdigest string for file to transfer to Data Backbone.
    chksum_type : `str`
        Which method to use for calculating the chksum.
    common_info : `dict`
        Dictionary containing information common to all files being saved.

    Returns
    -------
    afile : `dict`
        Data needed for saving virtual file containing physical/transfer
        metadata to tarball.
    """
    data_filename = "%s.info" % os.path.basename(filename)

    afile = {"filename": data_filename}

    data = copy.deepcopy(common_info)
    data["filename"] = os.path.basename(filename)
    data["filesize"] = os.path.getsize(filename)
    data["chksum"] = chksum
    data["chksum_type"] = chksum_type
    logging.debug("File data filename = %s", data_filename)
    logging.debug("File data contents = %s", data)

    tmp_yaml = yaml.dump(data, default_flow_style=False).encode("utf-8")
    afile["chksum"] = calc_chksum(bstream=io.BytesIO(tmp_yaml))

    tarinfo = tarfile.TarInfo(data_filename)
    tarinfo.mtime = time.time()
    tarinfo.size = len(tmp_yaml)

    afile["tarinfo"] = tarinfo
    afile["data"] = io.BytesIO(tmp_yaml)

    return afile


def create_digest_data(filename, digest):
    """Create a dictionary storing information about the chksum digest file

    Parameters
    ----------
    filename : `str`
        Name of file to save to Data Backbone.   Includes any local path.
    digest : `dict`
        Mapping filenames to chksums.

    Returns
    -------
    afile : `dict`
        Data needed for saving virtual digest file containing chksum
        information to tarball.
    """
    digest_filename = "%s.digest" % os.path.basename(filename)
    logging.debug("chksum digest filename = %s", digest_filename)
    for fname in digest:
        logging.debug("chksum digest contents: %s = %s", fname, digest[fname])

    digest_str = "\n".join(["%s\t%s" % (digest[x], x) for x in digest])

    afile = {"filename": digest_filename}
    tarinfo = tarfile.TarInfo(digest_filename)
    tarinfo.mtime = time.time()
    tarinfo.size = len(digest_str)
    afile["tarinfo"] = tarinfo
    afile["data"] = io.BytesIO(digest_str.encode("utf-8"))

    return afile


def create_transfer_cmd(filename, trans_opts, uuid_str):
    """Creates a string containing the transfer command

    Parameters
    ----------
    filename: `str`
        Name of output tarball.
    trans_opts : `dict`
        Options for the transfer command (e.g., dest http url prefix).
    uuid_str : `str`
        A unique id string to use in tarfile name to avoid collisions in
        DBB delivery area.

    Returns
    -------
    transcmd : `str`
        Command taking stdin so can pipe tar directly to curl.
    """
    tarfilename = "%s_%s.tar" % (os.path.splitext(os.path.basename(filename))[0], uuid_str)
    logging.info("Tar filename = %s", tarfilename)

    if not trans_opts["prefix"].startswith('https://'):
        raise ValueError("Prefix must start with https://")

    transcmd = "curl -s -S --fail -u : --negotiate -X PUT -T - %s/%s" % (trans_opts["prefix"], tarfilename)
    logging.debug("Transfer command = %s", transcmd)

    return transcmd


def check_gw_node(trans_opts):
    """Check can connect to transfer service

    Parameters
    ----------
    trans_opts : `dict`
        Options for the transfer command (e.g., dest http url prefix).
    """
    logging.debug("Checking connection to DBB GW node")

    if not trans_opts["prefix"].startswith('https://'):
        raise ValueError("Prefix must start with https://")

    # check valid hostname using socket
    match = re.match('https://([^/:]+)', trans_opts["prefix"])
    if match:
        desthost = match.group(1)
        logging.debug("desthost = %s", desthost)
        try:
            socket.gethostbyname(desthost)
        except socket.error:
            raise ValueError("Invalid hostname in prefix (%s)" % desthost) from None
    else:
        raise ValueError("Invalid prefix.   Could not determine hostname.")


    # check service up and authentication
    transcmd = "curl -s -S -u : --negotiate --head --fail %s" % (trans_opts["prefix"])
    logging.debug("Transfer command = %s", transcmd)

    process_trans = subprocess.Popen(shlex.split(transcmd),
                                     shell=False,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT)
    out = process_trans.communicate()[0].decode("utf-8")

    if process_trans.returncode != 0:
        logging.info("Cmd: %s", transcmd)
        logging.debug("Cmd output: %s", out)
        print("\nError connecting to DBB GW node.")
        print("Check prefix, authorization, and status of network and http service\n\n")
        raise RuntimeError(";   ".join(out.split('\n')[:2]))


def save_file(filename, common_info, trans_opts, dryrun):
    """Transfers a single file along with file information to the
        Data Backbone"s staging area

    Parameters
    ----------
    filename : `str`
        Name of file to save to Data Backbone.   Includes any local path.
    common_info : `dict`
        File information common to all files (like LSST user name).
    trans_opts : `dict`
        Options for the transfer command (e.g., dest http url prefix).
    dryrun : `bool`
        Controls whether file is actually transferred.
    """
    logging.info("Saving %s", filename)
    all_files = []
    digest = {}

    # add actual file to save to the collection of files to put in the tarball
    afile = {"filename": filename,
             "arcname": os.path.basename(filename),
             "chksum": calc_chksum(name=filename)}
    all_files.append(afile)
    digest[afile["arcname"]] = afile["chksum"]

    afile = create_physical_data(filename, afile["chksum"], "md5", common_info)
    all_files.append(afile)
    digest[afile["filename"]] = afile["chksum"]

    all_files.append(create_digest_data(filename, digest))

    transcmd = create_transfer_cmd(filename, trans_opts, common_info['uuid'])

    if not dryrun:
        for i in range(1, trans_opts['num_tries'] + 1):
            try:
                logging.info("Transfer attempt %d of %d", i, trans_opts['num_tries'])

                # send output of tar as stdin to curl
                process_trans = subprocess.Popen(shlex.split(transcmd),
                                                 shell=False,
                                                 stdin=subprocess.PIPE,
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.STDOUT)
                with tarfile.open(fileobj=process_trans.stdin, mode="w|") as tar:
                    for afile in all_files:
                        logging.debug("%s afile keys = %s", afile["filename"], afile.keys())
                        if "data" in afile:
                            tar.addfile(afile["tarinfo"], afile["data"])
                        else:
                            tar.add(afile["filename"], arcname=afile["arcname"])

                # Explicit close to address problems in https://jira.lsstcorp.org/browse/DM-16181
                process_trans.stdin.close()
                logging.debug("Tar done, waiting for curl to end")
                process_trans.wait()
                logging.debug("process_trans.returncode = %s" % process_trans.returncode)
                if process_trans.returncode != 0:
                    raise RuntimeError("Non-zero transfer returncode (%s)" % process_trans.returncode)
            except:
                out = process_trans.communicate()[0].decode("utf-8")
                if process_trans.returncode != 0:
                    msg = ";   ".join(out.split('\n')[:2])
                    logging.warning(msg)

                if i < trans_opts['num_tries']:
                    logging.warning("Transfer problem (Try: %d of %d).   Trying again.",
                                    i, trans_opts['num_tries'])
                else:
                    logging.error("Aborting transfer due to problems.")
                    if process_trans.returncode != 0:
                        logging.info("Transfer command = %s", transcmd)

                        # Skip printing broken pipe traceback as problem happened in curl
                        raise RuntimeError(msg) from None
                    else:
                        raise
            else:
                break

        logging.info("Completed transfer of %s", filename)
    else:
        logging.info("Dryrun.  Skipped transfer of %s", filename)


def get_ip_address():
    """Gets public ip address for machine running this program

    Returns
    -------
    address : `str` containing ip address
    """
    # At least one of the test stand machines did not have a public hostname
    # so the usual socket.gethostbyname(socket.gethostname()) doesn't work
    # Private IP address ranges from Internet Engineering Task Force RFC1918

    nifa = psutil.net_if_addrs()

    results = []
    for card in nifa.values():
        for addr in card:
            if addr.family == socket.AF_INET:
                # avoid known local and private IP addresses
                if (addr.address != '127.0.0.1'
                        and not '10.0.0.0' <= addr.address <= '10.255.255.255'
                        and not '172.16.0.0' <= addr.address <= '172.31.255.255'
                        and not '192.168.0.0' <= addr.address <= '192.169.255.255'):
                    results.append(addr.address)
    address = results[0]

    return address


def main(argv):
    """Program entry point.  Control process that iterates over each file

    Parameters
    ----------
    argv : `list`
        List of strings containing command line arguments.
    """
    start = time.time()

    args = parse_args(argv)

    logging.basicConfig(format="%(levelname)s::%(asctime)s::%(message)s", datefmt="%m/%d/%Y %H:%M:%S")
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    logging.debug("Cmdline args = %s", args)

    lsst_user = get_lsst_user()

    trans_opts = {"prefix": args.prefix,
                  "num_tries": args.num_tries}
    check_gw_node(trans_opts)

    uuid_str = str(uuid.uuid4())
    common_info = {"dataset_type": "raw",
                   "exec_name": os.path.basename(sys.argv[0]),
                   "exec_host": get_ip_address(),
                   "timestamp": time.time(),
                   "user": lsst_user,
                   "prov_msg": args.provmsg,
                   "uuid": uuid_str
                  }

    # create list of files
    fileset = set()
    for afile in args.files:
        if os.path.isfile(afile):
            fileset.add(afile)
        else:
            for (dirpath, _, filenames) in os.walk(afile):
                for fname in filenames:
                    fileset.add("%s/%s" % (dirpath, fname))

    if len(fileset) == 0:
        print("0 files to save.   Exiting")
    else:
        print("\nSaving %d file(s)\n" % len(fileset))

        # Loop through list saving files
        for fname in fileset:
            save_file(fname, common_info, trans_opts, args.dryrun)

        print("\nFinished saving %d file(s) in %0.3f seconds\n" % (len(fileset), time.time() - start))


if __name__ == "__main__":
    main(sys.argv[1:])
