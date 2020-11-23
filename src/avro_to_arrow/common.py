
import json
from abc import ABC, abstractmethod
import hashlib
import re
from typing import List
from datetime import datetime


class BaseMeta(ABC):

    _meta_name = 'BASE'

    def __init__(self, metadata, meta_version):
        self._version = meta_version
        self._metadata = metadata

    @property
    def digest(self):
        return hashlib.sha1(json.dumps(self._metadata))

    @staticmethod
    def load_meta(metadata):
        raise NotImplementedError('Must Implement')

    @staticmethod
    def process_type_priority(current_type, new_type):
        raise NotImplementedError('Must Implement')

    @abstractmethod
    def convert_to(self, target_meta):
        raise NotImplementedError('Must Implement')


class HiveMeta(BaseMeta):

    _meta_name = 'HIVE'

    @staticmethod
    def load_meta(metadata, meta_version='1.0'):
        return HiveMeta(metadata, meta_version=meta_version)

    @staticmethod
    def process_type_priority(current_type, new_type):
        COMPATIBLE_NUMBER_CAST = {
            'integer': 0,
            'bigint': 1,
            'float': 2
        }
        _COM = list(COMPATIBLE_NUMBER_CAST.keys())
        final_type = 'string'
        if current_type is None:
            final_type = new_type
        elif current_type == new_type:
            final_type = current_type
        else:
            if new_type in COMPATIBLE_NUMBER_CAST and current_type in COMPATIBLE_NUMBER_CAST:
                final_type_id = max(COMPATIBLE_NUMBER_CAST[current_type], COMPATIBLE_NUMBER_CAST[new_type])
                final_type = _COM[final_type_id]
        return final_type

    def _convert_to_parquet(self):
        import pyarrow as pa
        # from .utils import PandasPyArrowCursor
        pq_schema = []
        GLUE_HIVE_TO_PQ_MAPPING = {
            'bigint': pa.int64(),
            'int': pa.int32(),
            'integer': pa.int32(),
            'smallint': pa.int32(),
            'string': pa.utf8(),
            'boolean': pa.bool_(),
            'float': pa.float32(),
            'double': pa.float64()
        }
        for e in self._metadata['StorageDescriptor']['Columns']:
            comment = json.loads(e.get('Comment', '{}'))
            writer_type = e['Type']
            nullable = comment.get('nullable', True) and (not comment.get('is_pk', False))
            if writer_type in GLUE_HIVE_TO_PQ_MAPPING:
                final_type = GLUE_HIVE_TO_PQ_MAPPING[writer_type]
            elif 'decimal' in writer_type:
                precision, scale = re.match(r'decimal\((\d+),(\d+)\)', writer_type).groups()
                final_type = pa.decimal128(int(precision), int(scale))
            elif 'timestamp' in writer_type:
                unit = comment.get('unit', 'us')
                final_type = pa.timestamp(unit)
            elif 'date' in writer_type:
                final_type = pa.date32()
            else:
                raise NotImplementedError('{} is not supported'.format(writer_type))
            pq_schema.append(pa.field(e['Name'], final_type, nullable=nullable))
        final_schema = pa.schema(pq_schema)
        # final_schema = PandasPyArrowCursor.make_pandas_meta(final_schema)
        return final_schema

    def _convert_to_dask(self):
        dask_meta = {}
        GLUE_HIVE_TO_DASK_MAPPING = {
            'bigint': 'Int64',
            'int': 'Int32',
            'integer': 'Int32',
            'smallint': 'Int32',
            'timestamp': 'datetime64[ns]',
            'date': 'datetime64[D]'
        }
        special_columns = []
        for e in self._metadata['StorageDescriptor']['Columns']:
            comment = json.loads(e.get('Comment', '{}'))
            dtype = GLUE_HIVE_TO_DASK_MAPPING.get(e['Type'], e['Type'])
            if 'decimal' in dtype:
                dtype = 'Float64'
            if e['Type'] == 'timestamp' and 'timezone' in comment:
                special_columns.append({'name': e['Name'], 'type': dtype, 'timezone': comment['timezone']})
                dtype = 'string'
            dask_meta[e['Name']] = dtype
        return dask_meta, special_columns

    def convert_to(self, target_meta):
        if target_meta == 'PANDAS':
            return self._convert_to_dask()
        elif target_meta == 'PARQUET':
            return self._convert_to_parquet()
        else:
            raise NotImplementedError('Convert from {} to {} is not yet implemented'.format(self._meta_name, target_meta))


class AvroMeta(BaseMeta):

    def __init__(self, metadata, meta_version, included_schema_digest: List[str] = []):
        super().__init__(metadata=metadata, meta_version=meta_version)
        self._included_meta = included_schema_digest

    @staticmethod
    def load_meta(metadata, meta_version='1.0'):
        return AvroMeta(metadata, meta_version=meta_version)

    def _convert_to_hive(self):
        final_schema = {}
        PRIMITIVE = {
            'string': 'string',
            'int': 'integer',
            'long': 'bigint',
            'boolean': 'boolean',
            'double': 'double',
            'float': 'float'
        }
        for field in self._metadata['fields']:
            new_field = {'Name': field['name']}
            writer_type = field['type']
            if not isinstance(writer_type, list):
                writer_type = [writer_type]
            comment = {}
            new_field['nullable'] = False
            final_type = None
            for nntype in writer_type:
                if isinstance(nntype, str):
                    if nntype == 'null':
                        new_field['nullable'] = True
                    else:
                        final_type = HiveMeta.process_type_priority(final_type, PRIMITIVE[nntype])
                elif isinstance(nntype, dict):
                    if 'logicalType' in nntype:
                        logical_type = nntype['logicalType']
                        if logical_type == 'decimal':
                            scale = nntype.get('scale', 10)
                            precision = nntype.get('precision', 38)
                            final_type = 'decimal(%d,%d)' % (precision, scale)
                        elif logical_type == 'timestamp-millis':
                            final_type = 'timestamp'
                            comment['timeunit'] = 'ms'
                        elif logical_type == 'timestamp-micro':
                            final_type = 'timestamp'
                            comment['timeunit'] = 'us'
                        elif logical_type == 'date':
                            final_type = 'date'
                        else:
                            raise NotImplementedError('logicalType {} is not implemented'.format(logical_type))
                    elif 'type' in nntype:
                        if nntype['type'] in PRIMITIVE:
                            final_type = PRIMITIVE[nntype['type']]
                            if 'connect.name' in nntype:
                                comment['connect_type'] = nntype['connect.name']
                        else:
                            raise NotImplementedError('Type {} is not implemented'.format(nntype))
                    else:
                        raise NotImplementedError('Type {} is not implemented'.format(nntype))
                else:
                    raise NotImplementedError('Type {} is not implemented'.format(nntype))
            new_field['Type'] = final_type
            new_field['Comment'] = json.dumps(comment)
            final_schema[field['name']] = new_field
        return {'StorageDescriptor': {'Columns': list(final_schema.values())}}

    def _convert_to_parquet(self):
        import pyarrow as pa
        final_schema = []
        PRIMITIVE = {
            'string': pa.utf8(),
            'int': pa.int32(),
            'long': pa.int64(),
            'boolean': pa.bool_(),
            'double': pa.float64(),
            'float': pa.float32()
        }
        for field in self._metadata['fields']:
            name = field['name']
            writer_type = field['type']
            if not isinstance(writer_type, list):
                writer_type = [writer_type]
            comment = {}
            nullable = False
            final_type = None
            for nntype in writer_type:
                if isinstance(nntype, str):
                    if nntype == 'null':
                        nullable = True
                    else:
                        final_type = PRIMITIVE[nntype]
                elif isinstance(nntype, dict):
                    if 'logicalType' in nntype:
                        logical_type = nntype['logicalType']
                        if logical_type == 'decimal':
                            scale = nntype.get('scale', 10)
                            precision = nntype.get('precision', 38)
                            final_type = pa.decimal128(precision, scale)
                        elif logical_type == 'timestamp-millis':
                            final_type = pa.timestamp('ms')
                        elif logical_type == 'timestamp-micro':
                            final_type = pa.timestamp('us')
                            comment['timeunit'] = 'us'
                        elif logical_type == 'date':
                            final_type = pa.date32()
                        else:
                            raise NotImplementedError('logicalType {} is not implemented'.format(logical_type))
                    elif 'type' in nntype:
                        if nntype['type'] in PRIMITIVE:
                            final_type = PRIMITIVE[nntype['type']]
                            if 'connect.name' in nntype:
                                comment['connect_type'] = nntype['connect.name']
                        else:
                            raise NotImplementedError('Type {} is not implemented'.format(nntype))
                    else:
                        raise NotImplementedError('Type {} is not implemented'.format(nntype))
                else:
                    raise NotImplementedError('Type {} is not implemented'.format(nntype))
            final_schema.append(pa.field(name, final_type, nullable, comment))
        return pa.schema(final_schema)

    def convert_to(self, target_meta):
        if target_meta == 'HIVE':
            return self._convert_to_hive()
        if target_meta == 'PARQUET':
            return self._convert_to_parquet()
        else:
            raise NotImplementedError('Convert from {} to {} is not yet implemented'.format(self._meta_name, target_meta))

    def process_update(self, new_meta):

        if self.digest() == new_meta.digest():
            return self
        elif new_meta.digest() in self._included_meta:
            return self
        else:
            old_cols = set(self.keys()) - set(new_meta['schema'].keys())
            same_cols = set(self._metadata['schema'].keys()).intersection(set(self['schema'].keys()))
            if old_cols:
                for col in old_cols:
                    new_meta['schema'][col] = self['schema'][col]
            if same_cols:
                for col in same_cols:
                    if new_meta['schema'][col]['Type'] != self['schema'][col]['Type']:
                        new_meta['schema'][col]['Type'] = HiveMeta.process_type_priority(self['schema'][col]['Type'], new_meta['schema'][col]['Type'])
                        comment = json.loads(new_meta['schema'][col]['Comment'])
                        comment['changed_from'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S:%f')
                        new_meta['schema'][col]['Comment'] = json.dumps(comment)
            new_meta['digest'] = hashlib.sha1(json.dumps(new_meta['schema']).encode('utf-8')).hexdigest()
            new_meta['included_schema_digest'].append(self['digest'])
            return new_meta
