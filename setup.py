import os
import re
import sys
import platform
import subprocess

from setuptools import setup, Extension, find_namespace_packages
from setuptools.command.build_ext import build_ext
from distutils.version import LooseVersion
from distutils import dir_util

setup_path = os.path.abspath(__file__)
os.chdir(os.path.normpath(os.path.join(setup_path, os.pardir)))

dir_util.copy_tree("data", "python/mspasspy/data")

class CMakeExtension(Extension):
    def __init__(self, name, sourcedir='cxx'):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        try:
            out = subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError("CMake must be installed to build the following extensions: " +
                               ", ".join(e.name for e in self.extensions))

        if platform.system() == "Windows":
            cmake_version = LooseVersion(re.search(r'version\s*([\d.]+)', out.decode()).group(1))
            if cmake_version < '3.1.0':
                raise RuntimeError("CMake >= 3.1.0 is required on Windows")

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        cmake_args = ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
                      '-DPYTHON_EXECUTABLE=' + sys.executable]

        cfg = 'Debug' if self.debug else 'Release'
        build_args = ['--config', cfg]

        if platform.system() == "Windows":
            cmake_args += ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}'.format(cfg.upper(), extdir)]
            if sys.maxsize > 2**32:
                cmake_args += ['-A', 'x64']
            build_args += ['--', '/m']
        elif platform.system() == "Linux":
            cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]
            build_args += ['--', '-j$(nproc)']
        else:
            cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]
            build_args += ['--', '-j2']

        env = os.environ.copy()
        env['CXXFLAGS'] = '{} -DVERSION_INFO=\\"{}\\"'.format(env.get('CXXFLAGS', ''),
                                                              self.distribution.get_version())
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)
        subprocess.check_call(['cmake', ext.sourcedir] + cmake_args, cwd=self.build_temp, env=env)
        subprocess.check_call(['cmake', '--build', '.'] + build_args, cwd=self.build_temp)

ENTRY_POINTS = {
    'console_scripts': [
        'mspass-dbclean = mspasspy.db.script.dbclean:main',
        'mspass-dbverify = mspasspy.db.script.dbverify:main',
    ],
}

setup(
    name='mspasspy',
    version='0.0.1',
    author='Ian Wang',
    author_email='yinzhi.wang.cug@gmail.com',
    description='Massive Parallel Analysis System for Seismologists',
    long_description='',
    ext_modules=[CMakeExtension('mspasspy.ccore')],
    cmdclass=dict(build_ext=CMakeBuild),
    entry_points=ENTRY_POINTS,
    zip_safe=False,
    package_dir={"": "python"},
    packages=find_namespace_packages(where="python", include=["mspasspy", "mspasspy.*"]),
    package_data={'': ['*.yaml', '*.pf']},
    include_package_data=True,
    install_requires=['pyyaml']
)
