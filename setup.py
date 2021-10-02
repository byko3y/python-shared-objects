#!/usr/bin/env python3
# encoding: utf-8

import os, sys
import distutils.ccompiler
# import distutils.util
from distutils.core import setup, Extension
from test_ext import build_test, TestExtension

from distutils.command.install_lib import install_lib

platform_specifics = []

def libsys(file):
    return os.path.join('plibsys-mini', file)

def safeclib(file):
    return os.path.join('safeclib-mini', file)

def src(file):
    return os.path.join('src', file)

def test(file):
    return os.path.join('tests', file)

# if distutils.util.get_platform().startswith('linux'): // 'win'
if distutils.ccompiler.get_default_compiler(os.name) == 'msvc':
    platform_specifics.extend((
        'shm_memory_win.c', 'shm_event_win.c', libsys('patomic-win.c'), libsys('pspinlock-win.c'), libsys('puthread-win.c')))
else:
    platform_specifics.extend((
        'shm_memory_linux.c', 'shm_event_linux.c', libsys('patomic-c11.c'), libsys('pspinlock-c11.c'), libsys('puthread-posix.c'),
        safeclib('abort_handler_s.c'), safeclib('ignore_handler_s.c'), safeclib('safe_str_constraint.c'),
        safeclib('strcpy_s.c'), safeclib('strncpy_s.c'),
    ))

if distutils.ccompiler.get_default_compiler(os.name) == 'msvc':
    extra_compile_args = []
    extra_combo_flags = []
    extra_libs = []
else:
    extra_compile_args = []
    extra_combo_flags = ['-fno-strict-aliasing']
    if '--debug' in sys.argv or '-g' in sys.argv:
        extra_compile_args += ['-Og']
    extra_libs = ['rt']

pso_ext_sources = ['_pso.c']
pso_common_sources = ['shm_base.c', 'shm_types.c', 'shm_utils.c', 'coordinator.c', 'MM.c', 'unordered_map.c',
    'shm_event.c', libsys('puthread.c')] + pso_ext_sources

# XXX: for some reason pso.c is also compiled into test application
pso_module = TestExtension(
    name = '_pso',
    sources = list(map(src, platform_specifics + pso_common_sources)),
    ext_sources = list(map(src, pso_ext_sources)),
    test_name= 'pso_test',
    test_sources = [test('test.c')],
    include_dirs = [src('.'), src('plibsys-mini'), src('safeclib-mini')],
    library_dirs = [],
    libraries = extra_libs,
    extra_compile_args = extra_compile_args + extra_combo_flags,
    extra_link_args = extra_combo_flags,
    )

setup(name='pso',
    version='0.1.0',
    description='Python Shared Objects library',
    package_dir = {'': 'src'},
    py_modules = ['pso'],
    ext_modules=[pso_module],
    cmdclass={
        'build_test': build_test,
    }
)
