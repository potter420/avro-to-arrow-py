from setuptools import setup, Extension
import os
import pyarrow as pa
import numpy as np

USE_CYTHON = True
ext_modules = [
    Extension(
        "avro_to_arrow._reader",
        ["src/avro_to_arrow/_reader.cpp"]
    )
]
if USE_CYTHON:
    from Cython.Build import cythonize
    ext_modules = cythonize(
        ["src/avro_to_arrow/_reader.pyx"],
        language_level="3", annotate=True
    )

for ext in ext_modules:
    # The Numpy C headers are currently required
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    if 'CONDA_DEFAULT_ENV' in os.environ.keys():
        ext.include_dirs.append('/opt/mamba/envs/dev/include')
    else:
        ext.define_macros.append(("_GLIBCXX_USE_CXX11_ABI", "0"))
    ext.libraries.extend(['snappy']+pa.get_libraries())
    ext.library_dirs.extend(pa.get_library_dirs())
    if os.name == 'posix':
        ext.extra_compile_args.append('-std=c++11')

setup(
    ext_modules=ext_modules, zip_safe=False
)
