from pathlib import Path
import sys

cr = bytes([13])
for name in sys.argv[1:]:
    path = Path(name)
    path.write_bytes(path.read_bytes().replace(cr, b""))
