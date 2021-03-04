# distutils: language=c++
from .common import AvroMeta
from pyarrow import input_stream, allocate_buffer, total_allocated_bytes
import json

class ReadError(Exception):
    pass

cdef union float_uint32:
    float f
    uint32 n

cdef union double_ulong64:
    double d
    ulong64 n

cdef int64_t c_getUncompressedLength(const shared_ptr[CBuffer]& buf):
    cdef:
        CBuffer* c_buf
        size_t uncompressed_len
    c_buf = buf.get()
    GetUncompressedLength(reinterpret_cast[const_char_t](c_buf.data()), <size_t>c_buf.size(), &uncompressed_len)
    
    return <int64_t>uncompressed_len


cdef void snappy_get_buf(const shared_ptr[CBuffer]& buf, shared_ptr[CResizableBuffer] out_buf):
    cdef:
        CCompressionType typ = CCompressionType.CCompressionType_SNAPPY
        CCodec* codec = move(GetResultValue(CCodec.Create(typ))).get()
        CBuffer* c_buf
        int64_t output_size = c_getUncompressedLength(buf)
        uint8_t* output_buffer = NULL
    c_buf = buf.get()
    out_buf.get().Resize(output_size, True)
    output_buffer = out_buf.get().mutable_data()
    GetResultValue(
        codec.Decompress(
            c_buf.size(),
            c_buf.data(),
            output_size,
            output_buffer
        )
    )


cdef long64 read_long(CBufferReader* stream):
    cdef:
        ulong64 b
        ulong64 n
        int32_t shift
        uint8_t * c = <uint8_t *>malloc(<size_t> 1)
    # read_result = stream.ReadBuffer(<int64_t> 1)
    stream.Read(1, c)
    b = c[0]
    n = b & 0x7F
    shift = 7
    while (b & 0x80) != 0:
        stream.Read(1, c)
        b = c[0]
        n |= (b & 0x7F) << shift
        shift += 7
    free(c)
    return (n >> 1) ^ -(n & 1)



cdef const_char_t read_bytes(CBufferReader* stream):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef:
        long64 size = read_long(stream)
        uint8_t * byt = <uint8_t *> malloc(size)
    stream.Read(size, byt)
    cdef const_char_t rs = reinterpret_cast[const_char_t](byt)
    return rs

cdef bytes read_pybytes(CBufferReader* stream):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef:
        long64 size = read_long(stream)
        uint8_t * byt = <uint8_t *> malloc(size)
    stream.Read(size, byt)
    cdef bytes rs = reinterpret_cast[const_char_t](byt)[:size]
    free(byt)
    return rs

cdef unicode read_utf_8(CBufferReader* stream):
    cdef:
        long64 size = read_long(stream)
        uint8_t * byt = <uint8_t *> malloc(size)
    stream.Read(size, byt)
    cdef unicode rs = reinterpret_cast[const_char_t](byt)[:size].decode('utf-8')
    free(byt)
    return rs

cdef void append_string(CBufferReader* stream, CStringBuilder* builder):
    cdef:
        long64 size = read_long(stream)
        CResult[shared_ptr[CBuffer]] read_result = stream.ReadBuffer(<int64_t> size)
    builder.Append(reinterpret_cast[const_char_t](GetResultValue(read_result).get().data()), <int32_t> size)

cdef void unsafe_append_string(CBufferReader* stream, CStringBuilder* builder):
    cdef:
        long64 size = read_long(stream)
        CResult[shared_ptr[CBuffer]] read_result = stream.ReadBuffer(<int64_t> size)
    builder.ReserveData(size)
    builder.UnsafeAppend(reinterpret_cast[const_char_t](GetResultValue(read_result).get().data()), <int32_t> size)


cdef CDecimal128 read_decimal(CBufferReader* stream):
    """Special Reader for bytes"""
    cdef:
        long64 size = read_long(stream)
        # CResult[shared_ptr[CBuffer]] read_result = stream.ReadBuffer(<int64_t> size) 
        uint8_t * raw_bytes = <uint8_t *> malloc(size)
        int64_t n_bytes_read = GetResultValue(stream.Read(size, raw_bytes))
        CResult[CDecimal128] decimal_rs
    if n_bytes_read == size:
        decimal_rs = CDecimal128.FromBigEndian(raw_bytes, <int32_t> size)
        return GetResultValue[CDecimal128](decimal_rs)
    else:
        raise ReadError

cdef uint8_t read_boolean(CBufferReader* stream):
    cdef:
        uint8_t * ch_data = <uint8_t *> malloc(1)
        int64_t n_bytes_read = GetResultValue(stream.Read(1, ch_data))
        # CResult[shared_ptr[CBuffer]] read_result = stream.ReadBuffer(<int64_t> 1) 
    # return GetResultValue(read_result).get().data()[0]
    if n_bytes_read == 1:
        return ch_data[0]
    else:
        raise ReadError

cdef float read_float(CBufferReader* stream):
    """A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    cdef:
        # CResult[shared_ptr[CBuffer]] read_result = stream.ReadBuffer(<int64_t> 4)
        uint8_t * ch_data = <uint8_t *> malloc(4)
        float_uint32 fi
        # const_uint8_t ch_data = GetResultValue(read_result).get().data()
        int64_t n_bytes_read = GetResultValue(stream.Read(4, ch_data))
    if n_bytes_read == 4:
        fi.n = (ch_data[0]
                | (ch_data[1] << 8)
                | (ch_data[2] << 16)
                | (ch_data[3] << 24))
        return fi.f
    else:
        raise ReadError


cdef double read_double(CBufferReader* stream):
    """A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    cdef:
        # CResult[shared_ptr[CBuffer]] read_result = stream.ReadBuffer(<int64_t> 8)
        uint8_t * ch_data = <uint8_t *> malloc(8)
        double_ulong64 dl
        int64_t n_bytes_read = GetResultValue(stream.Read(8, ch_data))
        # const_uint8_t ch_data = GetResultValue(read_result).get().data()
    if n_bytes_read == 8:
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

cdef void append_records(
    CBufferReader* stream, vector[CArrayBuilder*] builder_c,
    vector[c_bool] nullable_, vector[Type] type_,
    int64_t block_count, int n_fields
):
    cdef:
        int _i_f = 0
        int _i_b = 0
        int32_t index_ = 0

    for _i_b in range(block_count):
        for _i_f in range(n_fields):
            if nullable_[_i_f]:
                index_ = read_long(stream)
            else:
                index_ = 0
            if nullable_[_i_f] and index_ == 0:
                builder_c[_i_f].AppendNull()
            else:
                if type_[_i_f] == Type._Type_STRING:
                    append_string(stream, (<CStringBuilder*>builder_c[_i_f]))
                elif type_[_i_f] == Type._Type_INT64 or type_[_i_f] == Type._Type_UINT64:
                    (<CInt64Builder*>builder_c[_i_f]).Append(<int64_t>read_long(stream))
                elif type_[_i_f] == Type._Type_INT32 or type_[_i_f] == Type._Type_UINT32:
                    (<CInt32Builder*>builder_c[_i_f]).Append(<int32_t>read_long(stream))
                elif type_[_i_f] == Type._Type_FLOAT:
                    (<CFloatBuilder*>builder_c[_i_f]).Append(read_float(stream))
                elif type_[_i_f] == Type._Type_DOUBLE:
                    (<CDoubleBuilder*>builder_c[_i_f]).Append(read_double(stream))
                elif type_[_i_f] == Type._Type_BOOL:
                    (<CBooleanBuilder*>builder_c[_i_f]).Append(read_boolean(stream))
                elif type_[_i_f] == Type._Type_DECIMAL128:
                    (<CDecimal128Builder*>builder_c[_i_f]).AppendDecimal(read_decimal(stream))
                elif type_[_i_f] == Type._Type_DATE32:
                    (<CDate32Builder*>builder_c[_i_f]).Append(<int32_t>read_long(stream))
                elif type_[_i_f] == Type._Type_TIMESTAMP:
                    (<CTimestampBuilder*>builder_c[_i_f]).Append(<int64_t>read_long(stream))
    
cdef void unsafe_append_records(
    CBufferReader* stream, vector[CArrayBuilder*] builder_c,
    vector[c_bool] nullable_, vector[Type] type_,
    int64_t block_count, int n_fields
):  
    cdef:
        int _i_f = 0
        int _i_b = 0
        int32_t index_ = 0

    for _i_b in range(block_count):
        for _i_f in range(n_fields):
            if nullable_[_i_f]:
                index_ = read_long(stream)
            else:
                index_ = 0
            if nullable_[_i_f] and index_ == 0:
                builder_c[_i_f].AppendNull()
            else:
                if type_[_i_f] == Type._Type_STRING:
                    unsafe_append_string(stream, (<CStringBuilder*>builder_c[_i_f]))
                elif type_[_i_f] == Type._Type_INT64 or type_[_i_f] == Type._Type_UINT64:
                    (<CInt64Builder*>builder_c[_i_f]).UnsafeAppend(<int64_t>read_long(stream))
                elif type_[_i_f] == Type._Type_INT32 or type_[_i_f] == Type._Type_UINT32:
                    (<CInt32Builder*>builder_c[_i_f]).UnsafeAppend(<int32_t>read_long(stream))
                elif type_[_i_f] == Type._Type_FLOAT:
                    (<CFloatBuilder*>builder_c[_i_f]).UnsafeAppend(read_float(stream))
                elif type_[_i_f] == Type._Type_DOUBLE:
                    (<CDoubleBuilder*>builder_c[_i_f]).UnsafeAppend(read_double(stream))
                elif type_[_i_f] == Type._Type_BOOL:
                    (<CBooleanBuilder*>builder_c[_i_f]).UnsafeAppend(read_boolean(stream))
                elif type_[_i_f] == Type._Type_DECIMAL128:
                    (<CDecimal128Builder*>builder_c[_i_f]).UnsafeAppendDecimal(read_decimal(stream))
                elif type_[_i_f] == Type._Type_DATE32:
                    (<CDate32Builder*>builder_c[_i_f]).UnsafeAppend(<int32_t>read_long(stream))
                elif type_[_i_f] == Type._Type_TIMESTAMP:
                    (<CTimestampBuilder*>builder_c[_i_f]).UnsafeAppend(<int64_t>read_long(stream))


cdef shared_ptr[CTable] read_record(
    CBufferReader* stream, int64_t block_count, shared_ptr[CSchema] schema,
    c_bool Safe, CMemoryPool* defl_mem_pool
):
    cdef:
        vector[CArrayBuilder*] builder_c
        vector[shared_ptr[CArray]] array_c
        int n_fields = schema.get().num_fields()
        vector[c_bool] nullable_
        vector[Type] type_
        int _i_f = 0
        int _i_b = 0
        int32_t index_ = 0
        
    for _i_f in range(n_fields):
        type_.push_back(schema.get().field(_i_f).get().type().get().id())
        nullable_.push_back(schema.get().field(_i_f).get().nullable())
        if type_[_i_f] == Type._Type_STRING:
            builder_c.push_back(new CStringBuilder(defl_mem_pool))
        elif type_[_i_f] == Type._Type_INT64 or type_[_i_f] == Type._Type_UINT64:
            builder_c.push_back(new CInt64Builder(defl_mem_pool))
        elif type_[_i_f] == Type._Type_INT32 or type_[_i_f] == Type._Type_UINT32:
            builder_c.push_back(new CInt32Builder(defl_mem_pool))
        elif type_[_i_f] == Type._Type_FLOAT:
            builder_c.push_back(new CFloatBuilder(defl_mem_pool))
        elif type_[_i_f] == Type._Type_DOUBLE:
            builder_c.push_back(new CDoubleBuilder(defl_mem_pool))
        elif type_[_i_f] == Type._Type_BOOL:
            builder_c.push_back(new CBooleanBuilder(defl_mem_pool))
        elif type_[_i_f] == Type._Type_DECIMAL128:
            builder_c.push_back(new CDecimal128Builder(schema.get().field(_i_f).get().type(), defl_mem_pool))
        elif type_[_i_f] == Type._Type_DATE32:
            builder_c.push_back(new CDate32Builder(defl_mem_pool))
        elif type_[_i_f] == Type._Type_TIMESTAMP:
            builder_c.push_back(new CTimestampBuilder(schema.get().field(_i_f).get().type(), defl_mem_pool))
        builder_c[_i_f].Reserve(<int64_t> block_count)
    if Safe:
        append_records(stream, builder_c, nullable_, type_, block_count, n_fields)
    else:
        unsafe_append_records(stream, builder_c, nullable_, type_, block_count, n_fields)
    cdef shared_ptr[CArray] _temp_arr
    for _i_f in range(n_fields):
        builder_c[_i_f].Finish(&_temp_arr)
        array_c.push_back(_temp_arr)
    cdef shared_ptr[CTable] table = CTable.MakeFromArrays(schema, array_c)
    return table


cdef skip_sync(CBufferReader* stream, const_char_t sync_marker, int64_t sync_size):
    cdef:
        int64_t current_pos = GetResultValue(stream.Tell())
        int64_t max_size = GetResultValue(stream.GetSize())
        uint8_t * sr = <uint8_t *>malloc(<size_t> sync_size)
        const_char_t sync_marker_read = NULL
        uint8_t i = 0
        uint8_t errors = 0
    if max_size - current_pos >= sync_size:
        stream.Read(sync_size, sr)
        sync_marker_read = reinterpret_cast[const_char_t](sr)
        for i in range(sync_size):
            if <char> sr[i] != sync_marker[i]:
                errors = 1
        if errors==1:
            raise ValueError('Expected sync marker not found, sync_size {}, {} compare to {}'\
                    .format(sync_size, sync_marker_read, sync_marker))
        free(sr)
    else:
        free(sr)
        raise EOFError("SkipSync: End of File Reached")

cdef int32_t CheckEOF(CBufferReader* stream, int64_t next_bytes):
    '''
        Check if the stream reaching it end in next few bytes
    '''
    cdef:
        int64_t current_pos = GetResultValue(stream.Tell())
        int64_t max_size = GetResultValue(stream.GetSize())
        int32_t is_eof = 1
    if current_pos < max_size:
        is_eof = 0
    return is_eof


cdef shared_ptr[CTable] c_read_avro_buf(
    const shared_ptr[CBuffer]& c_buf, CBufferReader* stream, shared_ptr[CSchema] schema,
    const_char_t c_sync_marker, int64_t SYNC_SIZE, c_bool Safe,
    CMemoryPool* memory_pool
):
    cdef:
        vector[shared_ptr[CTable]] table_list
        c_bool no_errors = False
        ulong64 block_count
        ulong64 block_length
        int64_t current_pos = GetResultValue(stream.Tell())
        int64_t max_size = GetResultValue(stream.GetSize())
        shared_ptr[CBuffer] compressed_buf
        shared_ptr[CResizableBuffer] uncompressed_buf = to_shared(GetResultValue(AllocateResizableBuffer(10000, memory_pool)))
        CBufferReader* block_stream
        CConcatenateTablesOptions concat_options = CConcatenateTablesOptions.Defaults()
        shared_ptr[CTable] output_table
        const_uint8_t crc
    while current_pos < max_size:
        no_errors = True
        block_count = read_long(stream)
        block_length = read_long(stream)
        current_pos = GetResultValue(stream.Tell())
        if block_length > 4:
            compressed_buf = SliceBuffer(c_buf, current_pos, block_length - 4)
        else:
            raise ValueError("Invalid block length {}".format(block_length))
        stream.Seek(current_pos + block_length)
        # if CheckEOF(stream, 4):
        #     crc = GetResultValue(stream.ReadBuffer(4)).get().data()
        snappy_get_buf(compressed_buf, uncompressed_buf)
        block_stream = new CBufferReader(uncompressed_buf.get().data(), uncompressed_buf.get().size())
        table_list.push_back(read_record(block_stream, block_count, schema, Safe, memory_pool))
        block_stream.Close()
        skip_sync(stream, c_sync_marker, SYNC_SIZE)
        current_pos = GetResultValue(stream.Tell())
    if no_errors:
        output_table = GetResultValue(ConcatenateTables(table_list, concat_options, memory_pool))
        return output_table
    else:
        raise ValueError("End of file, No Data")



cdef c_read_avro_buf_mem_test(
    const shared_ptr[CBuffer]& c_buf, CBufferReader* stream, shared_ptr[CSchema] schema,
    const_char_t c_sync_marker, int64_t SYNC_SIZE, c_bool Safe,
    CMemoryPool* memory_pool
):
    cdef:
        # vector[shared_ptr[CTable]] table_list
        c_bool no_errors = False
        ulong64 block_count
        ulong64 block_length
        int64_t current_pos
        shared_ptr[CBuffer] compressed_buf
        shared_ptr[CResizableBuffer] uncompressed_buf = to_shared(GetResultValue(AllocateResizableBuffer(10000, memory_pool)))
        CBufferReader* block_stream
        CConcatenateTablesOptions concat_options = CConcatenateTablesOptions.Defaults()
        shared_ptr[CTable] output_table
        const_uint8_t crc
    while CheckEOF(stream, 1)!=1:
        no_errors = True
        block_count = read_long(stream)
        block_length = read_long(stream)
        current_pos = GetResultValue(stream.Tell())
        if block_length > 4:
            compressed_buf = SliceBuffer(c_buf, current_pos, block_length - 4)
        else:
            raise ValueError("Invalid block length {}".format(block_length))
        stream.Seek(current_pos + block_length)
        # if CheckEOF(stream, 4):
        #     crc = GetResultValue(stream.ReadBuffer(4)).get().data()
        snappy_get_buf(compressed_buf, uncompressed_buf)
        block_stream = new CBufferReader(uncompressed_buf.get().data(), uncompressed_buf.get().size())
        block_stream.Close()
        skip_sync(stream, c_sync_marker, SYNC_SIZE)

cdef read_meta(CBufferReader* stream):
    cdef:
        int64_t block_count = read_long(stream)
    read_items = {}
    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            read_long(stream)
        for i in range(block_count):
            key = read_utf_8(stream)
            read_items[key] = read_pybytes(stream)
        block_count = read_long(stream)
    return read_items


def read_avro_buf(f, size, Safe=True):
    buf = allocate_buffer(size)
    f.readinto(buf)
    # print(total_allocated_bytes())
    cdef:
        shared_ptr[CBuffer] c_buf = pyarrow_unwrap_buffer(buf)
        int64_t buf_size = c_buf.get().size()
        const_uint8_t data = c_buf.get().data()
        CBuffer * inter_buf = new CBuffer(data, buf_size)
        CBufferReader * stream = new CBufferReader(data, buf_size)
        shared_ptr[CSchema] schema
        shared_ptr[CTable] results
        uint8_t * magic_bytes = <uint8_t *>malloc(4 * sizeof(uint8_t))
        uint8_t * sync_marker = <uint8_t *>malloc(SYNC_SIZE * sizeof(uint8_t))
        CMemoryPool* memory_pool = c_get_memory_pool()
    stream.Read(4, magic_bytes)
    meta = read_meta(stream)
    stream.Read(SYNC_SIZE, sync_marker)
    avro_schema = json.loads(meta['avro.schema'])
    codec = meta['avro.codec']
    arrow_meta = AvroMeta.load_meta(avro_schema).convert_to('PARQUET')
    schema = pyarrow_unwrap_schema(arrow_meta)
    results = c_read_avro_buf(
        shared_ptr[CBuffer](inter_buf), stream, schema, reinterpret_cast[const_char_t](sync_marker), 
        SYNC_SIZE, <c_bool> Safe, memory_pool
    )
    # c_read_avro_buf_mem_test(
    #     shared_ptr[CBuffer](inter_buf), stream, schema, 
    #     reinterpret_cast[const_char_t](sync_marker), SYNC_SIZE, memory_pool
    # )
    stream.Close()
    free(magic_bytes)
    free(sync_marker)
    return pyarrow_wrap_table(results)


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
