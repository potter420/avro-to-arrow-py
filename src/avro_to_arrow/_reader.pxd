# distutils: language = c++
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector
from libcpp.cast cimport reinterpret_cast
from libc.stdlib cimport free, malloc
from libcpp.memory cimport shared_ptr, make_shared
from libc.stdint cimport *
# from pyarrow.lib cimport *
from pyarrow.lib cimport (
    GetResultValue, CMemoryPool, c_default_memory_pool, c_get_memory_pool, move, to_shared, c_bool, c_string,
    CSchema, CArray, CTable, CResult, CDataType, CStatus, Type, CBuffer,
    AllocateBuffer, SliceBuffer, CResizableBuffer, AllocateResizableBuffer,
    pyarrow_unwrap_schema, pyarrow_wrap_table, pyarrow_wrap_array, pyarrow_unwrap_buffer, pyarrow_wrap_buffer,
    CCompressionType, CCodec, CBufferReader, ConcatenateTables, CConcatenateTablesOptions
 )

ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64
ctypedef const uint8_t* const_uint8_t
ctypedef const char* const_char_t

cdef extern from *:
    """
        #define SYNC_SIZE 16
    """
    # avoid name clash with Python-variable
    # in cdef-code the value can be accessed as MYSTRING_DEFINE
    cdef const int64_t SYNC_SIZE "SYNC_SIZE"

cdef extern from "snappy.h" namespace "snappy" nogil:
    cdef cppclass CSource" snappy::Source"
    c_bool GetUncompressedLength" snappy::GetUncompressedLength"(const char* compressed, size_t compressed_length, size_t* result)

cdef extern from "arrow/util/decimal.h" namespace "arrow" nogil:
    cdef cppclass CDecimal128" arrow::Decimal128":
        c_string ToString(int32_t scale) const
        @staticmethod
        CResult[CDecimal128] FromBigEndian(const uint8_t* data, int32_t length)

cdef extern from "arrow/builder.h" namespace "arrow" nogil:
    cdef cppclass CArrayBuilder" arrow::ArrayBuilder":
        CArrayBuilder(shared_ptr[CDataType], CMemoryPool* pool)

        int64_t length()
        int64_t null_count()
        CStatus AppendNull()
        CStatus Finish(shared_ptr[CArray]* out)
        CStatus Reserve(int64_t additional_capacity)

    cdef cppclass CBooleanBuilder" arrow::BooleanBuilder"(CArrayBuilder):
        CBooleanBuilder(CMemoryPool* pool)
        CStatus Append(const bint val)
        CStatus Append(const uint8_t val)
        void UnsafeAppend(const uint8_t val)

    cdef cppclass CInt8Builder" arrow::Int8Builder"(CArrayBuilder):
        CInt8Builder(CMemoryPool* pool)
        CStatus Append(const int8_t value)
        void UnsafeAppend(const uint8_t val)

    cdef cppclass CInt16Builder" arrow::Int16Builder"(CArrayBuilder):
        CInt16Builder(CMemoryPool* pool)
        CStatus Append(const int16_t value)
        void UnsafeAppend(const int16_t val)

    cdef cppclass CInt32Builder" arrow::Int32Builder"(CArrayBuilder):
        CInt32Builder(CMemoryPool* pool)
        CStatus Append(const int32_t value)
        void UnsafeAppend(const int32_t val)

    cdef cppclass CInt64Builder" arrow::Int64Builder"(CArrayBuilder):
        CInt64Builder(CMemoryPool* pool)
        CStatus Append(const int64_t value)
        void UnsafeAppend(const int64_t val)

    cdef cppclass CUInt8Builder" arrow::UInt8Builder"(CArrayBuilder):
        CUInt8Builder(CMemoryPool* pool)
        CStatus Append(const uint8_t value)
        void UnsafeAppend(const uint8_t val)

    cdef cppclass CUInt16Builder" arrow::UInt16Builder"(CArrayBuilder):
        CUInt16Builder(CMemoryPool* pool)
        CStatus Append(const uint16_t value)
        void UnsafeAppend(const uint16_t val)

    cdef cppclass CUInt32Builder" arrow::UInt32Builder"(CArrayBuilder):
        CUInt32Builder(CMemoryPool* pool)
        CStatus Append(const uint32_t value)
        void UnsafeAppend(const uint32_t val)

    cdef cppclass CUInt64Builder" arrow::UInt64Builder"(CArrayBuilder):
        CUInt64Builder(CMemoryPool* pool)
        CStatus Append(const uint64_t value)
        void UnsafeAppend(const uint64_t val)

    cdef cppclass CHalfFloatBuilder" arrow::HalfFloatBuilder"(CArrayBuilder):
        CHalfFloatBuilder(CMemoryPool* pool)

    cdef cppclass CFloatBuilder" arrow::FloatBuilder"(CArrayBuilder):
        CFloatBuilder(CMemoryPool* pool)
        CStatus Append(const float value)
        void UnsafeAppend(const float val)

    cdef cppclass CDoubleBuilder" arrow::DoubleBuilder"(CArrayBuilder):
        CDoubleBuilder(CMemoryPool* pool)
        CStatus Append(const double value)
        void UnsafeAppend(const double val)

    cdef cppclass CBinaryBuilder" arrow::BinaryBuilder"(CArrayBuilder):
        CArrayBuilder(shared_ptr[CDataType], CMemoryPool* pool)
        CStatus ReserveData(int64_t elements)
        CStatus Append(const char* value, int32_t length)
        void UnsafeAppend(const char* value, int32_t length)

    cdef cppclass CStringBuilder" arrow::StringBuilder"(CBinaryBuilder):
        CStringBuilder(CMemoryPool* pool)
        CStatus Append(const c_string& value)

    cdef cppclass CTimestampBuilder "arrow::TimestampBuilder"(CArrayBuilder):
        CTimestampBuilder(const shared_ptr[CDataType] typ, CMemoryPool* pool)
        CStatus Append(const int64_t value)
        void UnsafeAppend(const int64_t value)

    cdef cppclass CDate32Builder "arrow::Date32Builder"(CArrayBuilder):
        CDate32Builder(CMemoryPool* pool)
        CStatus Append(const int32_t value)
        void UnsafeAppend(const int32_t value)

    cdef cppclass CDate64Builder "arrow::Date64Builder"(CArrayBuilder):
        CDate64Builder(CMemoryPool* pool)
        CStatus Append(const int64_t value)
        void UnsafeAppend(const int64_t value)

    cdef cppclass CFixedSizeBinaryBuilder" arrow::FixedSizeBinaryBuilder"(CArrayBuilder):
        CFixedSizeBinaryBuilder(shared_ptr[CDataType], CMemoryPool* pool)
        CStatus Append(const char* value)
    
    cdef cppclass CDecimal128Builder" arrow::Decimal128Builder"(CFixedSizeBinaryBuilder):
        CDecimal128Builder(shared_ptr[CDataType], CMemoryPool* pool)
        CStatus AppendBytes" Append"(const char* value)
        CStatus AppendDecimal" Append"(CDecimal128 val)
        void UnsafeAppendDecimal" UnsafeAppend"(CDecimal128 value)

cdef int64_t c_getUncompressedLength(const shared_ptr[CBuffer]& buf)
cdef void snappy_get_buf(const shared_ptr[CBuffer]& buf, shared_ptr[CResizableBuffer] out_buf)
cdef c_read_avro_buf_mem_test(
    const shared_ptr[CBuffer]& c_buf, CBufferReader* stream, shared_ptr[CSchema] schema,
    const_char_t c_sync_marker, int64_t SYNC_SIZE, c_bool Safe,
    CMemoryPool* memory_pool
)
