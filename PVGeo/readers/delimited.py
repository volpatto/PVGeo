__all__ = [
    'DelimitedTextReader',
    'DelimitedPointsReaderBase',
    'XYZTextReader'
]

__displayname__ = 'Delimited File I/O'

import numpy as np
import pandas as pd

# Import Helpers:
from ..base import ReaderBase
from .. import _helpers
from .. import interface


class DelimitedTextReader(ReaderBase):
    """This reader will take in any delimited text file and make a ``vtkTable``
    from it. This is not much different than the default .txt or .csv reader in
    ParaView, however it gives us room to use our own extensions and a little
    more flexibility in the structure of the files we import.
    """
    __displayname__ = 'Delimited Text Reader'
    __category__ = 'reader'
    extensions = 'dat csv txt text ascii xyz tsv ntab'
    description = 'PVGeo: Delimited Text Files'
    def __init__(self, nOutputPorts=1, outputType='vtkTable', **kwargs):
        ReaderBase.__init__(self,
            nOutputPorts=nOutputPorts, outputType=outputType, **kwargs)

        # Parameters to control the file read:
        #- if these are set/changed, we must reperform the read
        self.__delimiter = kwargs.get('delimiter', ' ')
        self.__useTab = kwargs.get('useTab', False)
        self.__skipRows = kwargs.get('skiprows', 0)
        self.__comments = kwargs.get('comments', '!')
        self.__hasTitles = kwargs.get('hasTitles', True)
        # Data objects to hold the read data for access by the pipeline methods
        self._data = []
        self._titles = []

    def _GetDeli(self):
        """For itenral use
        """
        if self.__useTab:
            return None
        return self.__delimiter

    def GetSplitOnWhiteSpace(self):
        return self.__useTab

    def _readline(self, handle):
        """This reads the next line from a file handle ignoring comments."""
        ln = handle.readline()
        while (ln[0] == self.__comments):
            ln = handle.readline()
        ln = ln.split(self.__comments)[0].strip()
        return ln

    def _previewline(self, handle):
        last_pos = handle.tell()
        ln = self._readline(handle)
        handle.seek(last_pos)
        return ln

    #### Methods for performing the read ####

    def _GetFileHandles(self, idx=None):
        """This opens the input data file(s). This allows us to load the file
        contents, parse the header then use numpy or pandas to parse the data.
        These handles are closed by `_FileContentsToDataFrame`.
        """
        if idx is not None:
            fileNames = [self.GetFileNames(idx=idx)]
        else:
            fileNames = self.GetFileNames()
        handles = []
        for f in fileNames:
            try:
                #contents.append(np.genfromtxt(f, dtype=str, delimiter='\n', comments=self.__comments)[self.__skipRows::])
                handle = open(f, 'r')
                for n in range(self.__skipRows):
                    handle.readline()
                handles.append(handle)
            except (IOError, OSError) as fe:
                raise _helpers.PVGeoError(str(fe))
        if idx is not None: return handles[0]
        return handles

    def _ExtractHeader(self, handle):
        """Removes header from the given file's handle and moves the file iter
        forward ignoring comments.
        """
        # if len(np.shape(content)) > 2:
        #     raise _helpers.PVGeoError("`_ExtractHeader()` can only handle a sigle file's content")
        if self.__hasTitles:
            titles = self._readline(handle).split(self._GetDeli())
        else:
            cols = len(self._previewline(handle).split(self._GetDeli()))
            titles = []
            for i in range(cols):
                titles.append('Field %d' % i)
        return titles

    def _ExtractHeaders(self, handles):
        """Should NOT be overriden. This is a convienance methods to iteratively
        get all file contents. Your should override ``_ExtractHeader``.
        """
        ts = []
        for i in range(len(handles)):
            titles = self._ExtractHeader(handles[i])
            ts.append(titles)
        # Check that the titles are the same across files:
        ts = np.unique(np.asarray(ts), axis=0)
        if len(ts) > 1:
            raise _helpers.PVGeoError('Data array titles varied across file timesteps. This data is invalid as a timeseries.')
        return ts[0]


    def _FileContentsToDataFrame(self, handles):
        """Should NOT need to be overriden. After ``_ExtractHeaders`` removes
        the file header from the file hendle, this method will parse
        the remainder of the file from each handle in ``handles`` into a pandas
        DataFrame with column names generated from the titles resulting from in
        ``_ExtractHeaders``. This makes sure to close each of the file handles;
        if you override this, then you MUST besure to close each file handle!
        """
        data = []
        for handle in handles:
            if self.GetSplitOnWhiteSpace():
                df = pd.read_table(handle, names=self.GetTitles(), delim_whitespace=self.GetSplitOnWhiteSpace(), comment=self.GetComments())
            else:
                df = pd.read_table(handle, names=self.GetTitles(), sep=self._GetDeli(), comment=self.GetComments())
            data.append(df)
            handle.close()
        return data

    def _ReadUpFront(self):
        """Should not need to be overridden. This runs the file read routine.
        """
        # Perform Read
        handles = self._GetFileHandles()
        self._titles = self._ExtractHeaders(handles)
        self._data = self._FileContentsToDataFrame(handles)
        self.NeedToRead(flag=False)
        return 1

    #### Methods for accessing the data read in #####

    def _GetRawData(self, idx=0):
        """This will return the proper data for the given timestep as a dataframe
        """
        return self._data[idx]


    #### Algorithm Methods ####

    def RequestData(self, request, inInfo, outInfo):
        """Used by pipeline to get data for current timestep and populate the
        output data object.
        """
        # Get output:
        output = self.GetOutputData(outInfo, 0)
        # Get requested time index
        i = _helpers.getRequestedTime(self, outInfo)
        if self.NeedToRead():
            self._ReadUpFront()
        # Generate the data object
        interface.dataFrameToTable(self._GetRawData(idx=i), output)
        return 1


    #### Seters and Geters ####


    def SetDelimiter(self, deli):
        """The input file's delimiter. To use a tab delimiter please use
        ``SetSplitOnWhiteSpace()``

        Args:
            deli (str): a string delimiter/seperator
        """
        if deli != self.__delimiter:
            self.__delimiter = deli
            self.Modified()

    def SetSplitOnWhiteSpace(self, flag):
        """Set a boolean flag to override the ``SetDelimiter()`` and use any
        white space as a delimiter.
        """
        if flag != self.__useTab:
            self.__useTab = flag
            self.Modified()


    def SetSkipRows(self, skip):
        """The integer number of rows to skip at the top of the file.
        """
        if skip != self.__skipRows:
            self.__skipRows = skip
            self.Modified()

    def GetSkipRows(self):
        return self.__skipRows

    def SetComments(self, identifier):
        """The character identifier for comments within the file.
        """
        if identifier != self.__comments:
            self.__comments = identifier
            self.Modified()

    def GetComments(self):
        """Get the string identifier for comments"""
        return self.__comments

    def SetHasTitles(self, flag):
        """A boolean for if the delimited file has header titles for the data
        arrays.
        """
        if self.__hasTitles != flag:
            self.__hasTitles = flag
            self.Modified()

    def HasTitles(self):
        return self.__hasTitles

    def GetTitles(self):
        return self._titles


################################################################################


class DelimitedPointsReaderBase(DelimitedTextReader):
    """A base class for delimited text readers that produce ``vtkPolyData``
    points.
    """
    __displayname__ = 'Delimited Points Reader Base'
    __category__ = 'base'
    # extensions are inherrited from DelimitedTextReader
    description = 'PVGeo: Delimited Points' # Should be overriden
    def __init__(self, **kwargs):
        DelimitedTextReader.__init__(self, outputType='vtkPolyData', **kwargs)
        self.__copy_z = kwargs.get('copy_z', False)

    def SetCopyZ(self, flag):
        if self.__copy_z != flag:
            self.__copy_z = flag
            self.Modified()

    def GetCopyZ(self):
        return self.__copy_z

    #### Algorithm Methods ####

    def RequestData(self, request, inInfo, outInfo):
        """Used by pipeline to get data for current timestep and populate the
        output data object.
        """
        # Get output:
        output = self.GetOutputData(outInfo, 0)
        # Get requested time index
        i = _helpers.getRequestedTime(self, outInfo)
        if self.NeedToRead():
            self._ReadUpFront()
        # Generate the PolyData output
        data = self._GetRawData(idx=i)
        output.DeepCopy(interface.pointsToPolyData(data, copy_z=self.GetCopyZ()))
        return 1


################################################################################


class XYZTextReader(DelimitedTextReader):
    """A makeshift reader for XYZ files where titles have comma delimiter and
    data has space delimiter.
    """
    __displayname__ = 'XYZ Text Reader'
    __category__ = 'reader'
    # extensions are inherrited from DelimitedTextReader
    description = 'PVGeo: XYZ Delimited Text Files where header has comma delimiter.'
    def __init__(self, **kwargs):
        DelimitedTextReader.__init__(self, **kwargs)
        self.SetComments(kwargs.get('comments', '#'))

    # Simply override the extract titles functionality
    def _ExtractHeader(self, handle):
        titles = handle.readline().split('! ')[1].strip().split(', ') # first two characers of header is '! '
        return titles
