#!/usr/bin/env python3
#
# gen_setup.py
#
#-------------------------------------------------------------------------
# Copyright (c) Microsoft.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------------
'''
Generate setup.py
'''

import argparse
import inspect
import os
import pprint
import shutil
import sys
import uuid

def _parse_requirements_from_pip_req():
    '''
    Attempt to get parse_requirements from pip.req
    '''
    try:
        print("attempt to import pip.req")
        import pip.req # pylint: disable=import-error,no-name-in-module,useless-suppression
        print("imported pip.req from %s" % inspect.getfile(pip.req)) # pylint: disable=no-member,useless-suppression
        return pip.req.parse_requirements # pylint: disable=no-member,useless-suppression
    except ModuleNotFoundError:
        print("cannot import pip.req")
    except AttributeError:
        print("cannot get parse_requirements from pip.req")
    return None

def _parse_requirements_from_pip_internal():
    '''
    Attempt to get parse_requirements from pip._internal
    '''
    try:
        import pip # pylint: disable=import-error,no-name-in-module,useless-suppression
        print("got pip from %s" % inspect.getfile(pip))
    except ModuleNotFoundError:
        print("cannot import pip")
        return None
    try:
        import pip._internal # pylint: disable=import-error,no-name-in-module,useless-suppression
        print("got pip._internal from %s" % inspect.getfile(pip._internal)) # pylint: disable=protected-access
    except ModuleNotFoundError:
        return None
    try:
        return pip._internal.req.parse_requirements # pylint: disable=protected-access
    except AttributeError:
        print("cannot find parse_requirements in pip._internal.req.parse_requirements")
    return None

def load_requirements(filename):
    '''
    Read the requirements and return them in list form
    '''
    parse_requirements = _parse_requirements_from_pip_req()
    if not parse_requirements:
        parse_requirements = _parse_requirements_from_pip_internal()
    if parse_requirements:
        try:
            return [x.name+str(x.req.specifier) for x in parse_requirements(filename, session=str(uuid.uuid4()))]
        except Exception as e:
            err = "cannot parse requirements using pip: %s" % type(e).__name__
            if str(e):
                err += ' ' + str(e)
            print(err)
    print("fallback to non-pip parsing for %s" % filename)
    reqs = list()
    with open(filename, 'r') as f:
        for line in f.readlines():
            line = line.strip()
            if (not line) or line.startswith('#'):
                continue
            reqs.append(line)
    return reqs

def gen_setup(setup_tmpl, requirements_txt, output_filename):
    '''
    Read setup_tmpl and requirements_txt. Convolve them to produce output_filename.
    Raises SystemExit on error.
    '''
    try:
        shutil.rmtree(output_filename)
    except (FileNotFoundError, NotADirectoryError):
        pass
    try:
        os.unlink(output_filename)
    except FileNotFoundError:
        pass
    try:
        requirements = load_requirements(requirements_txt)
        print("requirements:\n%s" % pprint.pformat(requirements))
        requirements = ['"%s",' % x for x in requirements]
        result = list()
        with open(setup_tmpl, 'r') as f_setup:
            for line in f_setup.readlines():
                line = line.rstrip()
                if (line == 'requirements = [') and requirements:
                    result.append(line+requirements[0])
                    result.extend(['                '+x for x in requirements[1:]])
                else:
                    result.append(line)
    except FileNotFoundError as e:
        print("%s not found" % str(e))
        raise SystemExit(1) from e
    with open(output_filename, 'w') as f_output:
        f_output.write('\n'.join(result))
        f_output.write('\n')

def main(*args):
    '''
    Entrypoint for command-line execution
    '''
    ap_parser = argparse.ArgumentParser()
    ap_parser.add_argument("--setup_in", default="setup.py.tmpl",
                           help="path to setup.py.tmpl (input)")
    ap_parser.add_argument("--requirements", default="requirements.txt",
                           help="path to requirements.txt (input)")
    ap_parser.add_argument("--setup_out", default="setup.py",
                           help="path to setup.py (output)")
    ap_parser.add_argument("--show_environment", action="store_true",
                           help="show environment information")
    ap_args = ap_parser.parse_args(args=args)
    if ap_args.show_environment:
        print("sys.executable: %s" % sys.executable)
        print("sys.path:\n%s" % pprint.pformat(sys.path))
        print("env:\n%s" % pprint.pformat(dict(os.environ)))
    gen_setup(ap_args.setup_in, ap_args.requirements, ap_args.setup_out)
    sys.exit(0)

if __name__ == '__main__':
    main(*sys.argv[1:])
    sys.exit(1)
