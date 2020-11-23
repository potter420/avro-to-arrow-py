# distutils: language=c++
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector
from libcpp.cast cimport reinterpret_cast
from libc.stdlib cimport free, malloc
from libcpp.memory cimport shared_ptr, make_shared
from pyarrow.lib cimport (
    uint8_t, c_string, int32_t, int64_t, int32_t, c_bool, uint64_t, uint32_t,
    GetResultValue, CMemoryPool, c_default_memory_pool, c_get_memory_pool, move, to_shared,
    CArrayBuilder, CStringBuilder, CDate32Builder, CInt32Builder, CInt64Builder, CBooleanBuilder,
    CTimestampBuilder, CFloatBuilder, CDoubleBuilder,
    CSchema, CArray, CTable, CResult, CDataType, CStatus, Type, CBuffer,
    AllocateBuffer, SliceBuffer, CResizableBuffer, AllocateResizableBuffer,
    pyarrow_unwrap_schema, pyarrow_wrap_table, pyarrow_wrap_array, pyarrow_unwrap_buffer, pyarrow_wrap_buffer,
    CCompressionType, CCodec, CBufferReader, ConcatenateTables, CConcatenateTablesOptions
 )

cdef extern from "snappy.h" namespace "snappy" nogil:
    cdef cppclass CSource" snappy::Source"
    c_bool GetUncompressedLength" snappy::GetUncompressedLength"(const char* compressed, size_t compressed_length, size_t* result)

cdef extern from "arrow/util/decimal.h" namespace "arrow" nogil:
    cdef cppclass CDecimal128" arrow::Decimal128":
        c_string ToString(int32_t scale) const
        @staticmethod
        CResult[CDecimal128] FromBigEndian(const uint8_t* data, int32_t length)

cdef extern from "arrow/builder.h" namespace "arrow" nogil:
    cdef cppclass CFixedSizeBinaryBuilder" arrow::FixedSizeBinaryBuilder"(CArrayBuilder):
        CFixedSizeBinaryBuilder(shared_ptr[CDataType], CMemoryPool* pool)
        CStatus Append(const char* value)
    
    cdef cppclass CDecimal128Builder" arrow::Decimal128Builder"(CFixedSizeBinaryBuilder):
        CDecimal128Builder(shared_ptr[CDataType], CMemoryPool* pool)
        CStatus AppendBytes" Append"(const char* value)
        CStatus AppendDecimal" Append"(CDecimal128 val)