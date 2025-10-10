import os

from stream.Stream import InputStream, OutputStream


class FileInputStream(InputStream):
    """Reads events from a file."""
    def __init__(self, file_path: str):
        super().__init__()
        with open(file_path, "r") as f:
            for line in f:
                self._stream.put(line)
        self.close()


class FileOutputStream(OutputStream):
    """Writes events to a file."""
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
        if self.__is_async:
            self.__output_file.write(str(item))
        else:
            super().add_item(item)

    def close(self):
        super().close()
        if not self.__is_async:
            self.__output_file = open(self.__output_path, 'w')
            for item in self:
                self.__output_file.write(str(item))
        self.__output_file.close()
