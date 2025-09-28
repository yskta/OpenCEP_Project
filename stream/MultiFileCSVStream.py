import os
import glob
import csv
from stream.Stream import InputStream
from stream.CitiBikeDataFormatter import CitiBikeDataFormatter

class CSVFileInputStream(InputStream):
    """
    Reads events from a CSV file with lazy loading support.
    Unlike FileInputStream, this doesn't load the entire file into memory at once.
    """
    
    def __init__(self, file_path: str, data_formatter: CitiBikeDataFormatter = None, has_header: bool = True):
        """
        Initialize the CSV input stream.
        
        Args:
            file_path: Path to the CSV file
            data_formatter: Data formatter that knows how to parse CSV rows (optional)
            has_header: Whether the CSV file has a header row (default: True)
        """
        super().__init__()
        self._file_path = file_path
        self._data_formatter = data_formatter
        self._has_header = has_header
        self._file = None
        self._csv_reader = None
        self._is_initialized = False
        
    def _initialize(self):
        """
        Lazy initialization of file reading.
        """
        if self._is_initialized:
            return
 
        self._file = open(self._file_path, 'r', newline='')
        self._csv_reader = csv.reader(self._file)
        
        # Read header if present
        if self._has_header:
            headers = next(self._csv_reader)
            if self._data_formatter and hasattr(self._data_formatter, 'set_headers'):
                self._data_formatter.set_headers(headers)
                
        self._is_initialized = True
    
    def __next__(self):
        """
        Get the next line from the CSV file.
        """
        if not self._is_initialized:
            self._initialize()
            
        try:
            row = next(self._csv_reader)
            # Convert row back to CSV string format for compatibility
            return ','.join(row)
        except StopIteration:
            self._cleanup()
            raise
    
    def __iter__(self):
        """
        Return iterator for the stream.
        """
        if not self._is_initialized:
            self._initialize()
        return self
    
    def _cleanup(self):
        """
        Clean up file resources.
        """
        if self._file:
            self._file.close()
            self._file = None
            self._csv_reader = None
    
    def close(self):
        """
        Close the stream and clean up resources.
        """
        self._cleanup()
        super().close()
    
    def __del__(self):
        """
        Ensure file is closed when object is garbage collected.
        """
        self._cleanup()

class MultiFileCSVStream(InputStream):
    """
    Reads events from multiple CSV files in a directory.
    Files are processed in sorted order for consistency.
    """
    
    def __init__(self, directory_path: str, pattern: str = "*.csv", 
                 data_formatter: CitiBikeDataFormatter = None, has_header: bool = True):
        """
        Initialize the multi-file CSV stream.
        
        Args:
            directory_path: Path to the directory containing CSV files
            pattern: Glob pattern for matching files (default: "*.csv")
            data_formatter: Data formatter that knows how to parse CSV rows
            has_header: Whether CSV files have header rows (default: True)
        """
        super().__init__()
        self._directory_path = directory_path
        self._pattern = pattern
        self._data_formatter = data_formatter
        self._has_header = has_header
        
        # Get list of matching files
        self._file_paths = sorted(glob.glob(os.path.join(directory_path, pattern)))
        if not self._file_paths:
            raise Exception(f"No files matching pattern '{pattern}' found in {directory_path}")
        
        self._current_file_index = 0
        self._current_stream = None
        self._headers_read = False
        
    def _open_next_file(self):
        """
        Open the next file in the sequence.
        """
        if self._current_file_index >= len(self._file_paths):
            return False
            
        # Close current stream if any
        if self._current_stream:
            self._current_stream.close()
            
        # Open new stream
        file_path = self._file_paths[self._current_file_index]
        # For subsequent files, skip headers if already read
        skip_header = self._has_header and self._headers_read
        self._current_stream = CSVFileInputStream(
            file_path, 
            self._data_formatter,
            has_header=self._has_header and not skip_header
        )
        
        self._headers_read = True
        self._current_file_index += 1
        return True
    
    def __next__(self):
        """
        Get the next event from the current file or move to next file.
        """
        while True:
            if not self._current_stream:
                if not self._open_next_file():
                    raise StopIteration()
                    
            try:
                return next(self._current_stream)
            except StopIteration:
                # Current file exhausted, try next one
                if not self._open_next_file():
                    raise StopIteration()
    
    def __iter__(self):
        """
        Return iterator for the stream.
        """
        return self
    
    def close(self):
        """
        Close all resources.
        """
        if self._current_stream:
            self._current_stream.close()
        super().close()
    
    def get_file_count(self):
        """
        Get the total number of files to be processed.
        """
        return len(self._file_paths)
    
    def get_current_file_index(self):
        """
        Get the index of the currently processing file.
        """
        return self._current_file_index
    
    def get_file_paths(self):
        """
        Get the list of file paths that will be processed.
        """
        return self._file_paths.copy()


class MultiDirectoryCSVStream(InputStream):
    """
    Reads events from CSV files across multiple directories.
    Useful for processing data organized by time periods (e.g., monthly directories).
    """
    
    def __init__(self, directory_paths: list, pattern: str = "*.csv",
                 data_formatter: CitiBikeDataFormatter = None, has_header: bool = True):
        """
        Initialize the multi-directory CSV stream.
        
        Args:
            directory_paths: List of directory paths containing CSV files
            pattern: Glob pattern for matching files (default: "*.csv")
            data_formatter: Data formatter that knows how to parse CSV rows
            has_header: Whether CSV files have header rows (default: True)
        """
        super().__init__()
        self._directory_paths = directory_paths
        self._pattern = pattern
        self._data_formatter = data_formatter
        self._has_header = has_header
        
        self._current_dir_index = 0
        self._current_stream = None
        
    def _open_next_directory(self):
        """
        Open stream for the next directory.
        """
        if self._current_dir_index >= len(self._directory_paths):
            return False
            
        # Close current stream if any
        if self._current_stream:
            self._current_stream.close()
            
        # Open new directory stream
        directory_path = self._directory_paths[self._current_dir_index]
        try:
            self._current_stream = MultiFileCSVStream(
                directory_path,
                self._pattern,
                self._data_formatter,
                self._has_header
            )
            self._current_dir_index += 1
            return True
        except Exception:
            # Skip directories with no matching files
            self._current_dir_index += 1
            return self._open_next_directory()
    
    def __next__(self):
        """
        Get the next event from the current directory or move to next directory.
        """
        while True:
            if not self._current_stream:
                if not self._open_next_directory():
                    raise StopIteration()
                    
            try:
                return next(self._current_stream)
            except StopIteration:
                # Current directory exhausted, try next one
                if not self._open_next_directory():
                    raise StopIteration()
    
    def __iter__(self):
        """
        Return iterator for the stream.
        """
        return self
    
    def close(self):
        """
        Close all resources.
        """
        if self._current_stream:
            self._current_stream.close()
        super().close()