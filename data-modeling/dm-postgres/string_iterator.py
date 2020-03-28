import io

class StringIteratorIO(io.TextIOBase):
    def __init__(self,iter):
        self._iter = iter
        self._buff = ''

    def readable(self):
        return True
    
    def _read1(self,n = None):
        while (not self._buff):
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n = None):
        line = []
        if (n is None or n < 0):
            while (True):
                m = self._read1()
                if (not m):
                    break
                line.append(m)
        else:
            while (n > 0):
                m = self._read1(n)
                if (not m):
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)