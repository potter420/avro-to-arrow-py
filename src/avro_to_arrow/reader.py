import pyarrow as pa
import snappy as snp
from ._reader import read_avro_buf

read_avro_buf = read_avro_buf


def log_memory():
    print(pa.total_allocated_bytes())
