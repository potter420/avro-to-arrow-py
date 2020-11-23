# distutils: language=c++
from __future__ import print_function
from fastavro._read import file_reader, BLOCK_READERS, SYNC_SIZE
from etl_framework_pandas.utils.data_format import AvroMeta
from cython.operator cimport dereference
from pyarrow import input_stream, concat_tables, allocate_buffer, total_allocated_bytes
import json

ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64
ctypedef const uint8_t* const_uint8_t
ctypedef const char* const_char_t

class ReadError(Exception):
    pass

def test(n):
    cdef CMemoryPool* defl_mem_pool = c_default_memory_pool()
    cdef CStringBuilder* stringArrBuilder = new CStringBuilder(defl_mem_pool)
    cdef c_string t = b""
    cdef shared_ptr[CArray] arr
    for i in range(n):
        t = b"a"
        stringArrBuilder.Append(t)
    stringArrBuilder.Finish(&arr)
    return pyarrow_wrap_array(arr)


def test2(n):
    cdef CMemoryPool* defl_mem_pool = c_default_memory_pool()
    cdef unordered_map[int,int] m
    cdef int64_t i
    for i in range(n):
        m[i] = i+10
    return m[0]

cpdef read_long_pub(fo):
    return read_long(fo)

cdef long64 read_long(fo) except? -1:
    """int and long values are written using variable-length, zig-zag
    coding."""
    cdef ulong64 b
    cdef ulong64 n
    cdef int32 shift
    cdef bytes c = fo.read(1)

    # We do EOF checking only here, since most reader start here
    if not c:
        raise StopIteration

    b = <unsigned char>(c[0])
    n = b & 0x7F
    shift = 7

    while (b & 0x80) != 0:
        c = fo.read(1)
        b = <unsigned char>(c[0])
        n |= (b & 0x7F) << shift
        shift += 7

    return (n >> 1) ^ -(n & 1)


cdef bytes read_bytes(fo):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef long64 size = read_long(fo)
    return fo.read(<long>size)

cdef CDecimal128 read_decimal(fo):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef long64 size = read_long(fo)
    # cdef char* _temp_b = <char *> malloc(<size_t> size)
    cdef bytes _temp_py =  fo.read(<long>size)
    cdef char* _temp_b = _temp_py
    # cdef const_uint8_t test = reinterpret_cast[const_uint8_t](_temp_b)
    cdef CResult[CDecimal128] decimal_rs = CDecimal128.FromBigEndian(
            reinterpret_cast[const_uint8_t](_temp_b), <int32_t> size
        )
    # cdef CResult[CDecimal128] decimal_rs = CDecimal128.FromFloat32(<float> size, 10, 0)
    # cdef CResult[CDecimal128] decimal_rs = CDecimal128.FromString(b'123.20')
    # free(_temp_b)
    return GetResultValue[CDecimal128](decimal_rs)

cdef uint8_t read_boolean(fo):
    cdef bytes bytes_temp = fo.read(1)
    cdef uint8_t t = <uint8_t> bytes_temp[0]
    return t

cdef union float_uint32:
    float f
    uint32 n


cdef float read_float(fo):
    """A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    cdef bytes data
    cdef unsigned char ch_data[4]
    cdef float_uint32 fi
    data = fo.read(4)
    if len(data) == 4:
        ch_data[:4] = data
        fi.n = (ch_data[0]
                | (ch_data[1] << 8)
                | (ch_data[2] << 16)
                | (ch_data[3] << 24))
        return fi.f
    else:
        raise ReadError

cdef union double_ulong64:
    double d
    ulong64 n


cdef double read_double(fo):
    """A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    cdef bytes data
    cdef unsigned char ch_data[8]
    cdef double_ulong64 dl
    data = fo.read(8)
    if len(data) == 8:
        ch_data[:8] = data
        dl.n = (ch_data[0]
                | (<ulong64>(ch_data[1]) << 8)
                | (<ulong64>(ch_data[2]) << 16)
                | (<ulong64>(ch_data[3]) << 24)
                | (<ulong64>(ch_data[4]) << 32)
                | (<ulong64>(ch_data[5]) << 40)
                | (<ulong64>(ch_data[6]) << 48)
                | (<ulong64>(ch_data[7]) << 56))
        return dl.d
    else:
        raise ReadError


cdef read_avro(fo, int64_t block_count, arrow_meta):
    cdef CMemoryPool* defl_mem_pool = c_default_memory_pool()
    cdef shared_ptr[CSchema] schema = pyarrow_unwrap_schema(arrow_meta)
    cdef vector[CArrayBuilder*] builder_c
    cdef vector[shared_ptr[CArray]] array_c
    # cdef unordered_map[c_string, int32_t] m
    cdef int n_fields = schema.get().num_fields()
    cdef int test = 0
    cdef vector[c_bool] nullable_
    cdef Type type_
    cdef int index_ = 0
    for i in range(n_fields):
        type_ = schema.get().field(i).get().type().get().id()
        nullable_.push_back(schema.get().field(i).get().nullable())
        if type_ == Type._Type_STRING:
            builder_c.push_back(new CStringBuilder(defl_mem_pool))
        elif type_ == Type._Type_INT64 or type_ == Type._Type_UINT64:
            builder_c.push_back(new CInt64Builder(defl_mem_pool))
        elif type_ == Type._Type_INT32 or type_ == Type._Type_UINT32:
            builder_c.push_back(new CInt32Builder(defl_mem_pool))
        elif type_ == Type._Type_FLOAT:
            builder_c.push_back(new CFloatBuilder(defl_mem_pool))
        elif type_ == Type._Type_DOUBLE:
            builder_c.push_back(new CDoubleBuilder(defl_mem_pool))
        elif type_ == Type._Type_BOOL:
            builder_c.push_back(new CBooleanBuilder(defl_mem_pool))
        elif type_ == Type._Type_DECIMAL:
            builder_c.push_back(new CDecimal128Builder(schema.get().field(i).get().type(), defl_mem_pool))
        elif type_ == Type._Type_DATE32:
            builder_c.push_back(new CDate32Builder(defl_mem_pool))
        elif type_ == Type._Type_TIMESTAMP:
            builder_c.push_back(new CTimestampBuilder(schema.get().field(i).get().type(), defl_mem_pool))
        builder_c[i].Reserve(<int64_t> block_count)
    for i in range(block_count):
        for i in range(n_fields):
            type_ = schema.get().field(i).get().type().get().id()
            if nullable_[i]:
                index_ = read_long(fo)
            else:
                index_ = 0
            if nullable_[i] and index_ == 0:
                builder_c[i].AppendNull()
            else:
                if type_ == Type._Type_STRING:
                    (<CStringBuilder*>builder_c[i]).Append(<c_string> read_bytes(fo))
                elif type_ == Type._Type_INT64 or type_ == Type._Type_UINT64:
                    (<CInt64Builder*>builder_c[i]).Append(<int64_t>read_long(fo))
                elif type_ == Type._Type_INT32 or type_ == Type._Type_UINT32:
                    (<CInt32Builder*>builder_c[i]).Append(<int32_t>read_long(fo))
                elif type_ == Type._Type_FLOAT:
                    (<CFloatBuilder*>builder_c[i]).Append(read_float(fo))
                elif type_ == Type._Type_DOUBLE:
                    (<CDoubleBuilder*>builder_c[i]).Append(read_double(fo))
                elif type_ == Type._Type_BOOL:
                    (<CBooleanBuilder*>builder_c[i]).Append(read_boolean(fo))
                elif type_ == Type._Type_DECIMAL:
                    (<CDecimal128Builder*>builder_c[i]).AppendDecimal(read_decimal(fo))
                elif type_ == Type._Type_DATE32:
                    (<CDate32Builder*>builder_c[i]).Append(<int32_t>read_long(fo))
                elif type_ == Type._Type_TIMESTAMP:
                    (<CTimestampBuilder*>builder_c[i]).Append(<int64_t>read_long(fo))
            
    cdef shared_ptr[CArray] _temp_arr
    for i in range(n_fields):
        builder_c[i].Finish(&_temp_arr)
        array_c.push_back(_temp_arr)
    cdef shared_ptr[CTable] table = CTable.MakeFromArrays(schema, array_c)

    return pyarrow_wrap_table(table)


cpdef skip_sync(fo, sync_marker):
    """Skip an expected sync marker, complaining if it doesn't match"""
    if fo.read(SYNC_SIZE) != sync_marker:
        raise ValueError('expected sync marker not found')

# cpdef snappy_read_block(fo):
#     length = read_long(fo)
#     data = fo.read(length - 4)
#     fo.read(4)  # CRC
#     return BytesIO(snappy.decompress(data))

class avro_arrow_reader(file_reader):
    def __init__(self, fo, reader_schema=None, return_record_name=False):
        file_reader.__init__(self, fo, reader_schema, return_record_name)
        
    def read_test(self):
        sync_marker = self._header['sync']
        if self.writer_schema['type'] == 'record':
            pass
        else:
            raise NotImplementedError('Other record type is not implemented')
        # self.writer_schema,
        # self._named_schemas,
        # self.reader_schema,
        # self.return_record_name
        arrow_meta = AvroMeta.load_meta(self.writer_schema).convert_to('PARQUET')
        read_block = BLOCK_READERS.get(self.codec)
        data = []
        while True:
            try:
                block_count = read_long(self.fo)
            except StopIteration:
                break
            block_fo = read_block(self.fo)
            data.append(read_avro(block_fo, block_count, arrow_meta))
            skip_sync(self.fo, sync_marker)
        return concat_tables(data)
