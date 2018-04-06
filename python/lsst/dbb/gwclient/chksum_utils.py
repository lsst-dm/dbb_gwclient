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

"""Helper functions for calculating chksums to be shared by the
   client side (dbb_gwclient) and service side (dbb_gateway)
"""
import hashlib

DEFAULT_BLKSIZE = 2**15


def calc_md5sum(name=None, bstream=None, blksize=2**15):
    """Calculate the chksum for a file in chunks

    Parameters
    ----------
    name : `str`
        Fullname, includes path where needed, to the file for which to
        calculate md5sum
    bstream : `io.BufferedReader` or `io.BytesIO`
        Buffered stream for which to calculate md5sum
    blksize : `int`
        Number of bytes to read from file allowing large files to be read in chunks

    Returns
    -------
    digest : `str`
        Chksum for given file or data stream
    """
    if name is not None:
        bstream = open(name, "rb")

    # currently using md5
    md5 = hashlib.md5()

    for chunk in iter(lambda: bstream.read(blksize), b""):
        md5.update(chunk)

    if name is not None:
        bstream.close()

    return md5.hexdigest()


def calc_chksum(name=None, bstream=None, chksum_type="md5", blksize=DEFAULT_BLKSIZE):
    """Calculate chksum for given file or byte stream

    Parameters
    ----------
    name : `str`
        Fullname, includes path where needed, to the file for which to
        calculate md5sum
    bstream : `io.BufferedReader` or `io.BytesIO`
        Buffered stream for which to calculate md5sum
    chksum_type : `str`
        Which method to use for calculating the chksum
    blksize : `int`
        Number of bytes to read in a single chunk from the file

    Returns
    -------
    chksum : `str`
        Chksum for given file or byte stream

    Raises
    ------
    KeyError
        Raised when unsupported chksum_type
    """
    chksum_methods = {"md5": calc_md5sum}

    try:
        chksum = chksum_methods[chksum_type](name, bstream, blksize)
    except KeyError:
        raise KeyError("Unsupported chksum type: %s" % chksum_type)

    return chksum
