#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""BuildTest
Compiles test executable.
"""

import os

from distutils.command.build_ext import build_ext
from distutils.errors import *
from distutils.core import Extension
from distutils.dep_util import newer_group
from distutils import log

class TestExtension(Extension):
    def __init__(self, name, sources,
              include_dirs=None,
              define_macros=None,
              undef_macros=None,
              library_dirs=None,
              libraries=None,
              runtime_library_dirs=None,
              extra_objects=None,
              extra_compile_args=None,
              extra_link_args=None,
              export_symbols=None,
              swig_opts = None,
              depends=None,
              language=None,
              optional=None,
							test_name=None,
              test_sources=None,
              ext_sources=None,
              **kw                      # To catch unknown keywords
             ):
        if not test_sources:
            raise DistutilsSetupError("Running build_test, but no test_sources are specified in the Extension.");
        self.test_sources = test_sources
        self.ext_sources = ext_sources
        self.test_name = test_name
        Extension.__init__(self, name, sources, include_dirs, define_macros, undef_macros, library_dirs,
            libraries, runtime_library_dirs, extra_objects, extra_compile_args, extra_link_args,
            export_symbols, swig_opts, depends, language, optional, **kw)

class build_test(build_ext):
    description = "Compile test executable."

    def finalize_options(self):
        build_ext.finalize_options(self)
        # self.extensions = self.distribution.test_modules

    def build_extension(self, ext):
        if ext.sources is None or not isinstance(ext.sources, (list, tuple)):
            raise DistutilsSetupError(
                  "in 'test_modules' option (test '%s'), "
                  "'sources' must be present and must be "
                  "a list of source filenames" % ext.test_name)
        if (ext.ext_sources):
            sources = [x for x in ext.sources if x not in ext.ext_sources] + ext.test_sources
        else:
            sources = list(ext.sources) + ext.test_sources

        ext_path = self.get_ext_fullpath(ext.test_name)
        depends = sources + ext.depends
        if not (self.force or newer_group(depends, ext_path, 'newer')):
            log.debug("skipping '%s' test (up-to-date)", ext.test_name)
            return
        else:
            log.info("building '%s' test", ext.test_name)

        # First, scan the sources for SWIG definition files (.i), run
        # SWIG on 'em to create .c files, and modify the sources list
        # accordingly.
        sources = self.swig_sources(sources, ext)

        # Next, compile the source code to object files.

        # XXX not honouring 'define_macros' or 'undef_macros' -- the
        # CCompiler API needs to change to accommodate this, and I
        # want to do one thing at a time!

        # Two possible sources for extra compiler arguments:
        #   - 'extra_compile_args' in Extension object
        #   - CFLAGS environment variable (not particularly
        #     elegant, but people seem to expect it and I
        #     guess it's useful)
        # The environment variable should take precedence, and
        # any sensible compiler will give precedence to later
        # command line args.  Hence we combine them in order:
        extra_args = ext.extra_compile_args or []

        macros = ext.define_macros[:]
        for undef in ext.undef_macros:
            macros.append((undef,))

        objects = self.compiler.compile(sources,
                                        output_dir=self.build_temp,
                                        macros=macros,
                                        include_dirs=ext.include_dirs,
                                        debug=self.debug,
                                        extra_postargs=extra_args,
                                        depends=ext.depends)

        # XXX outdated variable, kept here in case third-part code
        # needs it.
        self._built_objects = objects[:]

        # Now link the object files together into a "shared object" --
        # of course, first we have to figure out all the other things
        # that go into the mix.
        if ext.extra_objects:
            objects.extend(ext.extra_objects)
        extra_args = ext.extra_link_args or []

        # Detect target language, if not provided
        language = ext.language or self.compiler.detect_language(sources)

        self.compiler.link_executable(
            objects, ext_path,
            libraries=self.get_libraries(ext),
            library_dirs=ext.library_dirs,
            runtime_library_dirs=ext.runtime_library_dirs,
            extra_postargs=extra_args,
            debug=self.debug,
            target_lang=language)

    def get_ext_filename(self, ext_name):
        r"""Convert the name of an extension (eg. "foo.bar") into the name
        of the file from which it will be loaded (eg. "foo/bar.so", or
        "foo\bar.pyd").
        """
        from distutils.sysconfig import get_config_var
        ext_path = ext_name.split('.')
        # ext_suffix = get_config_var('EXE') or ''
        # for MSVC it becomes ".exe.exe" in the end, thus I removed the suffix
        ext_suffix = ''
        return os.path.join(*ext_path) + ext_suffix
