# Licensed under a 3-clause BSD style license - see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
===============
desida.metadata
===============

Utilities for working with filesystem metadata.

Some code is currently copied from the ``decamUtil`` product.
"""
import datetime
import os
import re
import sys
import gzip
import json
from argparse import ArgumentParser
from collections import namedtuple
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from desiutil.log import get_logger, DEBUG


log = None


class _Directory(object):
    """Simple class to store directory information.

    Parameters
    ----------
    name : :class:`str`
        Directory name.
    flags : :class:`int`, optional
        Integer warning flag relating to unexpected permissions or directories.
    files : :class:`int`, optional
        Number of files.
    size : :class:`int`, optional
        Number of bytes in files.
    """

    def __init__(self, name, flags=0, files=0, size=0):
        self.parent, self.name = os.path.split(name)
        self.flags = flags
        self.files = files
        self.size = size
        self.growth = dict()
        self._children = set()
        self._level = len(name.split('/'))

    def __repr__(self):
        return "Directory('{0.name}', flags={0.flags:d}, files={0.files:d}, size={0.size:d})".format(self)

    def to_json(self):
        """Convert object to a dictionary for easy JSON encoding.
        """
        j = {'__D': 1,
             'n': self.path,
             'l': self.flags,
             'f': self.files,
             's': self.size,
             'c': list(self._children)}
        if self.growth:
            j['g'] = self.growth
        return j

    def add_child(self, child):
        """Add a child directory.
        """
        d, b = os.path.split(child)
        if d.endswith(self.path):
            self._children.add(b)
        else:
            raise ValueError('{0} is not a valid child of {1}!'.format(child, self.path))

    def add_growth(self, other):
        """Copy growth data from another Directory object.
        """
        if isinstance(other, _Directory):
            o = other.growth
        elif isinstance(other, dict):
            o = other
        else:
            raise ValueError("Unknown type for growth data!")
        for Ymd in o:
            if Ymd in self.growth:
                self.growth[Ymd][0] += o[Ymd][0]
                self.growth[Ymd][1] += o[Ymd][1]
            else:
                self.growth[Ymd] = list()
                self.growth[Ymd].append(o[Ymd][0])
                self.growth[Ymd].append(o[Ymd][1])

    @property
    def children(self):
        return sorted([os.path.join(self.parent, self.name, c) for c in self._children])

    @property
    def path(self):
        return os.path.join(self.parent, self.name)

    @property
    def level(self):
        return self._level

    @property
    def h(self):
        hr = self.size
        if hr < 1024:
            return '{0:d} bytes'.format(hr)
        hr /= 1024.0
        if hr < 1024:
            return '{0:.1f} kB'.format(hr)
        hr /= 1024.0
        if hr < 1024:
            return '{0:.1f} MB'.format(hr)
        hr /= 1024.0
        if hr < 1024:
            return '{0:.1f} GB'.format(hr)
        hr /= 1024.0
        return '{0:.1f} TB'.format(hr)

    @property
    def readable_flags(self):
        rf = ''
        if self.flags & 2 != 0:
            rf += 'G'
        if self.flags & 1 != 0:
            rf += 'P'
        return rf


def _DirectoryDecoder(d):
    """Convert JSON-serialized data into a :class:`~desida.metadata.Directory`.
    """
    if '__D' in d:
        dd = _Directory(d['n'], flags=d['l'], files=d['f'], size=d['s'])
        dd._children |= set(d['c'])
        if 'g' in d:
            dd.growth = d['g']
        return dd
    return d


class AnalyzeGrowth(dict):
    """Load data growth metadata and analyze it.

    This class provides a container for a set of
    :class:`~desida.metadata._Directory` objects, plus tools to analyze
    the data.

    Parameters
    ----------
    start_date : :class:`datetime.date`
        First day of the data set.
    end_date : :class:`datetime.date`
        Last day of the data set.
    cadence : :class:`int`, optional
        Number of days to skip after `start_date`, default 1 day.
    """

    def __init__(self, start_date, end_date, cadence=1):
        super().__init__()
        self._user_directories = re.compile(r'desicollab/users/[^/]+$')
        self._start_date = start_date
        self._end_date = end_date
        self._cadence_delta = datetime.timedelta(days=cadence)
        self._actual_start_date = None
        self._actual_end_date = None
        self._load()

    def _load(self):
        """Read in JSON-serialized intermedate files.

        Returns
        -------
        :class:`dict`
            A container for a set of :class:`~desida.metadata.Directory` objects.
        """
        log.debug("self._start_date = %s", repr(self._start_date))
        date = self._start_date
        while date <= self._end_date:
            d = date.strftime('%Y-%m-%d')
            log.debug("d = '%s'", d)
            if os.path.isfile(os.path.join(os.environ['DESI_ROOT'], 'metadata', f'{d}.json.gz')):
                with gzip.open(os.path.join(os.environ['DESI_ROOT'], 'metadata', f'{d}.json.gz')) as j:
                    self[d] = json.load(j, object_hook=_DirectoryDecoder)
            elif os.path.isfile(os.path.join(os.environ['DESI_ROOT'], 'metadata', f'{d}.json')):
                with open(os.path.join(os.environ['DESI_ROOT'], 'metadata', f'{d}.json')) as j:
                    self[d] = json.load(j, object_hook=_DirectoryDecoder)
            else:
                log.warning("No file matching %s!", d)
            date += self.cadence

    @property
    def start_date(self):
        """The real first day of data.
        """
        if self._actual_start_date is None:
            self._actual_start_date, self._actual_end_date = self._actual_range()
        return self._actual_start_date

    @property
    def end_date(self):
        """The real first day of data.
        """
        if self._actual_end_date is None:
            self._actual_start_date, self._actual_end_date = self._actual_range()
        return self._actual_end_date

    @property
    def cadence(self):
        """Cadence converted.
        """
        return self._cadence_delta

    def _actual_range(self):
        """Find the real start and end date.

        Because of missing data, the real start and end dates may not
        necessarily correspond to the requested start and end.

        Returns
        -------
        :class:`tuple`
            The real start and end dates.
        """
        actual_dates = list(sorted(self.keys()))
        return (actual_dates[0], actual_dates[-1])

    def plot(self, directory, start_index=0, xlim=None, log=False, model=None):
        """Creates a cumulative plot of data volume and number of files (inodes)
        for a particular data set.

        Parameters
        ----------
        directory : :class:`str`
            The directory path to plot. The directory path should start with the
            "filesystem", *e.g." ``desi`` or ``desicollab``.
        start_index : :class:`int`, optional
            If set, start plotting the growth data with this index.
        xlim : :class:`tuple`, optional
            If set, use these *two* dates for the x range of the plot.
        log : :class:`boolean`, optional
            If ``True`` use a logarithmic Y-axis.
        model : :class:`tuple`, optional
            A tuple of x, y, label data to overplot.

        Returns
        -------
        :class:`tuple`
            A tuple of figure and axis handles. In particular, the figure handle
            (first item) can be used to save a copy of the figure.
        """
        filesystem = directory.split('/')[0]
        growth_dates = np.array(sorted(self.keys()), dtype=np.datetime64)
        growth_files = np.array([self[d][filesystem][directory].files
                                 for d in sorted(self.keys())])
        growth_bytes = np.array([self[d][filesystem][directory].size
                                 for d in sorted(self.keys())])
        GB = 1024*1024*1024
        fig, ax1 = plt.subplots(nrows=1, ncols=1, figsize=(16,9), dpi=100)
        if log:
            p1 = ax1.semilogy(growth_dates[start_index:], growth_bytes[start_index:]/GB, color='black', label='Data Volume')
        else:
            p1 = ax1.plot(growth_dates[start_index:], growth_bytes[start_index:]/GB, color='black', label='Data Volume')
        ax2 = ax1.twinx()
        if log:
            p2 = ax2.semilogy(growth_dates[start_index:], growth_files[start_index:], color='blue', label='Number of Files')
        else:
            p2 = ax2.plot(growth_dates[start_index:], growth_files[start_index:], color='blue', label='Number of Files')
        if model is not None:
            p3 = ax1.plot(model[0], model[1], color='black', linestyle='--', label=model[2])
        foo = ax1.set_xlabel('Date')
        foo = ax1.set_ylabel('Cumulative Data [GB]')
        foo = ax1.set_title(f'{directory} as of {self.end_date}')
        foo = ax2.set_ylabel('Cumulative Number of Files', color='blue')
        foo = ax2.tick_params(axis='y', labelcolor='blue')
        # Major ticks every 6 months.
        # fmt_half_year = mdates.MonthLocator(interval=3)
        # foo = ax1.xaxis.set_major_locator(fmt_half_year)
        # Major ticks every week.
        fmt_week = mdates.WeekdayLocator(byweekday=1)
        foo = ax1.xaxis.set_major_locator(fmt_week)
        # Minor ticks every month.
        # fmt_month = mdates.MonthLocator()
        # foo = ax1.xaxis.set_minor_locator(fmt_month)
        # Minor ticks every day.
        # fmt_day = mdates.DayLocator()
        # foo = ax1.xaxis.set_major_locator(fmt_day)
        # Format the coords message box, i.e. the numbers displayed as the cursor moves
        # across the axes within the interactive GUI.
        # Text in the x axis will be displayed in 'YYYY-mm' format.
        foo = ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        foo = ax1.format_xdata = mdates.DateFormatter('%Y-%m-%d')
        foo = ax1.grid(True)
        foo = ax2.yaxis.grid(linestyle=':')
        # Rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them.
        foo = fig.autofmt_xdate()
        if xlim is not None:
            foo = ax1.set_xlim(np.datetime64(xlim[0]), np.datetime64(xlim[1]))
            foo = ax2.set_xlim(np.datetime64(xlim[0]), np.datetime64(xlim[1]))
        # Data Volume legend in upper left
        foo = ax1.legend(loc=2, numpoints=1)
        # Number of files legend in lower right
        foo = ax2.legend(loc=4, numpoints=1)
        return (fig, ax1, ax2)

    def users(self):
        """Compute per-user total use and changes in the directory ``desicollab/users``.

        Returns
        -------
        :func:`collections.namedtuple`
            A named tuple containing entries for ``total_bytes``, ``total_inodes``,
            ``delta_bytes`` and ``delta_inodes``. Each entry contains a :class:`list` of
            :class:`tuple` with the value for each username.

        Notes
        -----
        Assumes that directories are very rarely deleted, so all users will be present at the end.
        """
        UserGrowth = namedtuple('UserGrowth', ['total_bytes', 'total_inodes', 'delta_bytes', 'delta_inodes'])
        all_users = set([d for d in self[self.end_date]['desicollab'].keys()
                         if self._user_directories.match(d) is not None])
        common_users = set([d for d in self[self.start_date]['desicollab'].keys()
                            if self._user_directories.match(d) is not None]) & all_users
        delta_files = sorted([(u.split('/')[-1], self[self.end_date]['desicollab'][u].files - self[self.start_date]['desicollab'][u].files) for u in common_users], key=lambda x: x[1], reverse=True)
        delta_bytes = sorted([(u.split('/')[-1], self[self.end_date]['desicollab'][u].size - self[self.start_date]['desicollab'][u].size) for u in common_users], key=lambda x: x[1], reverse=True)
        total_files = sorted([(u.split('/')[-1], self[self.end_date]['desicollab'][u].files) for u in all_users], key=lambda x: x[1], reverse=True)
        total_bytes = sorted([(u.split('/')[-1], self[self.end_date]['desicollab'][u].size) for u in all_users], key=lambda x: x[1], reverse=True)
        return UserGrowth(total_bytes=total_bytes, total_inodes=total_files,
                          delta_bytes=delta_bytes, delta_inodes=delta_files)


def _options():
    """Parse command-line options.
    """
    prsr = ArgumentParser(prog=os.path.basename(sys.argv[0]),
                          description="Analyze and plot data growth over time.")
    prsr.add_argument('-c', '--cadence', metavar='N', type=int, default=1,
                      help="Sample the data every N days (default %(default)s day).")
    prsr.add_argument('-o', '--output', metavar='DIR', default=os.getcwd(),
                      help='Write output to DIR. If not specified the current directory will be used.')
    prsr.add_argument('-p', '--plot', metavar='DIR', nargs='+',
                      help="Plot growth of one or more DIR.")
    prsr.add_argument('-u', '--users', action='store_true',
                      help='Produce a summary report of user directory use.')
    prsr.add_argument('-v', '--verbose', action='store_true',
                      help='Print debug messages.')
    prsr.add_argument('start', metavar='YYYY-MM-DD',
                      help='Start analysis with this date.')
    prsr.add_argument('end', metavar='YYYY-MM-DD',
                      help='End analysis with this date.')
    return prsr.parse_args()


def main():
    """Entry-point for command-line scripts.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    global log
    options = _options()
    if options.verbose:
        log = get_logger(DEBUG)
    else:
        log = get_logger()
    try:
        start_date = datetime.datetime.strptime(options.start, '%Y-%m-%d').date()
    except ValueError as e:
        log.critical(str(e))
        return 1
    try:
        end_date = datetime.datetime.strptime(options.end, '%Y-%m-%d').date()
    except ValueError as e:
        log.critical(str(e))
        return 1
    if options.plot is None and not options.users:
        log.critical("No analysis request specified, exiting!")
        return 1
    log.debug("growth = AnalyzeGrowth(%s, %s, %d)",
              repr(start_date), repr(end_date), options.cadence)
    growth = AnalyzeGrowth(start_date, end_date, options.cadence)
    if options.plot is not None:
        for directory in options.plot:
            filesystem = directory.split('/')[0]
            if filesystem not in growth[growth.start_date]:
                log.critical("Root filesystem '%s' is not present in the data!", filesystem)
                return 1
            if directory not in growth[growth.start_date][filesystem]:
                log.critical("Requested directory '%s' is not present in the data!", directory)
                return 1
            log.debug('f, ax1, ax2 = growth.plot("%s")', directory)
            f, ax1, ax2 = growth.plot(directory)
            filename = f"{directory.replace('/', '_')}_{growth.end_date.replace('-', '')}.png"
            log.debug('f.savefig(%s/%s)', options.output, filename)
            f.savefig(os.path.join(options.output, filename))
    if options.users:
        top = 20
        user_growth = growth.users()
        output_lines = ['# Report on use of desicollab/users',
                        '',
                        f'Top {top:d} users from {growth.start_date} to {growth.end_date}.',
                        '',
                        '## Total bytes per user',
                        '']
        for entry in user_growth.total_bytes[:top]:
            output_lines.append(f"* {entry[0]}: {entry[1]:d}")
        output_lines += ['', '## Total inodes per user', '']
        for entry in user_growth.total_inodes[:top]:
            output_lines.append(f"* {entry[0]}: {entry[1]:d}")
        output_lines += ['', '## Change in bytes per user', '']
        for entry in user_growth.delta_bytes[:top]:
            output_lines.append(f"* {entry[0]}: {entry[1]:d}")
        output_lines += ['', '## Change in inodes per user', '']
        for entry in user_growth.delta_inodes[:top]:
            output_lines.append(f"* {entry[0]}: {entry[1]:d}")
        filename = os.path.join(options.output, f'desicollab_users_{growth.end_date.replace('-', '')}.md')
        with open(filename, 'w') as MD:
            MD.write('\n'.join(output_lines) + '\n')
    return 0


if __name__ == '__main__':
    import matplotlib
    matplotlib.use("Agg")
    sys.exit(main())
