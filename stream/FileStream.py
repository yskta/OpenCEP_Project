import os

from stream.Stream import InputStream, OutputStream


# FileInputStream:
# - Inherits from InputStream
# - Reads the entire file in the constructor and stores each line in an internal queue (_stream)
# - Calls close() immediately after reading is complete
class FileInputStream(InputStream):
    """
    Reads the objects from a predefined input file.
    """
    def __init__(self, file_path: str):
        super().__init__()
        # TODO: reading the entire content of the input file here is very inefficient
        with open(file_path, "r") as f:
            for line in f.readlines():
                self._stream.put(line)
        self.close()


#   FileOutputStream:Inherits from OutputStream
# - Two write modes:
    # - Asynchronous mode (is_async=True): Writes items to the file immediately upon receiving them
    # - Synchronous mode (is_async=False): Buffers items and writes them all at once when close() is called
# - The close() method, in synchronous mode, writes all buffer contents before closing the file
class FileOutputStream(OutputStream):
    """
    Writes the objects into a predefined output file.
    """
    def __init__(self, base_path: str, file_name: str, is_async: bool = False):
        super().__init__()
        if not os.path.exists(base_path):
            os.makedirs(base_path, exist_ok=True)
        self.__is_async = is_async
        self.__output_path = os.path.join(base_path, file_name)
        if self.__is_async:
            self.__output_file = open(self.__output_path, 'w')
        else:
            self.__output_file = None

    def add_item(self, item: object):
        """
        Depending on the settings, either writes the item to the file immediately or buffers it for future write.
        """
        if self.__is_async:
            self.__output_file.write(str(item))
        else:
            super().add_item(item)

    def close(self):
        """
        If asynchronous write is disabled, writes everything to the output file before closing it.
        """
        super().close()
        if not self.__is_async:
            self.__output_file = open(self.__output_path, 'w')
            for item in self:
                self.__output_file.write(str(item))
        self.__output_file.close()
