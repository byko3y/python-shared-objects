import _pso
from _pso import *
import sys
import os
import os.path
import copy
import types
import ast

import inspect
from contextlib import AbstractContextManager

__all__ = ['PsoLoader', 'install', 'transaction_debug', 'transaction', 'transform_module',
    'Transacted', 'Transient'] + [m for m in dir(_pso) if not m.startswith('__')]

from importlib.abc import Loader, MetaPathFinder
from importlib.util import spec_from_file_location

dump_ast = False
if dump_ast:
    try:
        import astunparse
    except:
        pass

    try:
        import Tools.parser.unparse as unparser
    except:
        pass

def unparse_ast(ast):
    if 'astunparse' in globals():
        print(astunparse.unparse(ast))
    if 'unparser' in globals():
        return unparser.Unparser(ast)

# requires previous __getattr__ to be renamed into __getattr_old__
getattr_footer = ast.parse('''
def __getattr__(name):
    if not __vars__:
        with transaction:
            if __module__ not in pso.get_root():
                pso.get_root()[__module__] = pso.ShmDict()
            __vars__ = pso.get_root()[__module__]

    with pso.transient():
        if name not in __vars__:
            if '__getattr_old__' in globals():
                return __getattr_old__(name)
            else:
                raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        return __vars__[name]
''')

transaction_template = ast.parse('''
with pso.Transacted() as __transaction_context__:
    while True:
        try:
            block()
        except pso.ShmAbort:
            if __transaction_context__.is_nested: # partial commit/rollback is not supported
                raise
            else:
                pso.transaction_rollback_retaining()
                continue;
        except:
            raise
        else:
            break # success, commit
''')

class SetLineNo(ast.NodeTransformer):
    lineno = 1
    col_offset = 0
    def generic_visit(self, node):
        rslt = super().generic_visit(node)
        if 'lineno' in rslt._attributes:
            if hasattr(node, 'lineno'):
                rslt.lineno = self.lineno
        if 'col_offset' in rslt._attributes:
            if hasattr(rslt, 'col_offset'):
                rslt.col_offset = self.col_offset
        return rslt

class WithReplacer(ast.NodeTransformer):
    def visit_With(self, with_node: ast.With):
        with_node = self.generic_visit(with_node)
        if with_node.items and len(with_node.items) == 1:
            item = with_node.items[0]
            if isinstance(item, ast.withitem) and isinstance(item.context_expr, ast.Name) and item.context_expr.id == 'transaction':
                # find the while block within the module AST
                new_template = copy.deepcopy(transaction_template.body[0])
                ast.copy_location(new_template, with_node)
                trans = SetLineNo()
                trans.lineno = with_node.lineno
                trans.col_offset = with_node.col_offset
                trans.visit(new_template)
                # find the "block()" placeholder
                for block_parent_node in ast.walk(new_template):
                    if isinstance(block_parent_node, ast.Try) and len(block_parent_node.body) == 1:
                        block_node = block_parent_node.body[0]
                        if isinstance(block_node, ast.Expr) and isinstance(block_node.value, ast.Call):
                            if isinstance(block_node.value.func, ast.Name) and block_node.value.func.id == 'block':
                                # replace the block() placeholder with actual code from "with" block
                                block_parent_node.body[:] = with_node.body

                return new_template

        return with_node

class VarsReplacer(ast.NodeTransformer):
    def visit_FunctionDef(self, func_node):
        if func_node.name == '__getattr__':
            func_node.name = '__getattr_old__'
        return func_node
    def visit_AsyncFunctionDef(self, node):
        return node # stop processing here
    def visit_DictComp(self, node):
        return node
    def visit_ListComp(self, node):
        return node
    def visit_SetComp(self, node):
        return node
    def visit_GeneratorExp(self, node):
        return node
    def visit_Lambda(self, node):
        return node
    def visit_ClassDef(self, node):
        return node

    def visit_Module(self, module_node):
      new_footer = copy.deepcopy(getattr_footer.body[0])
      # new_footer.lineno = 0
      # new_footer.col_offset = 0
      # ast.fix_missing_locations(new_footer)
      module_node.body.append(new_footer)
      return module_node

    def visit_Name(self, name_node: ast.Name):
        name = ast.Name("__vars__", ast.Load())
        ast.copy_location(name, name_node)
        attr = ast.Attribute(name, name_node.id, name_node.ctx)
        ast.copy_location(attr, name_node)
        return attr

class RemoveDecorator(ast.NodeTransformer):
    def visit_FunctionDef(self, function_node: ast.FunctionDef):
        # remove our decorator from the list
        new_decorators = []
        for decorator in function_node.decorator_list:
            if decorator.id != 'decor':
                new_decorators.append(decorator)
            else:
                new_decorators.append(decorator)
                decorator.id = 'transformed_function'

        function_node.decorator_list[:] = new_decorators
        return function_node

def transform_module(source, filename):
    rslt = ast.parse(source, filename, 'exec')

    # VarsReplacer().visit(rslt) # VarReplacer creates "with transaction" section, so it stay before WithReplacer
    WithReplacer().visit(rslt)
    # RemoveDecorator().visit(rslt)

    if dump_ast:
        print('Code after modification:')
        unparse_ast(rslt)
        # print(ast.dump(rslt, include_attributes = True))

    return rslt

def _compile_func(func, tree, filename):
    code = compile(tree, filename, 'exec')
    exec(code, func.__globals__)

class PsoMetaFinder(MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if 'ast' not in globals():
            return None # module is not yet initialized, loaders cannot work correctly
        if path is None or path == "":
            # top level import
            # path = [os.getcwd()]
            path = sys.path
        name = fullname
        for entry in path:
            if entry == '':
                entry = os.getcwd()
            if not os.path.isdir(os.path.join(entry, name)):
                # print('Trying ', os.path.join(entry, name + ".pso.py"), ' module import')
                filename = os.path.join(entry, name + ".pso.py")
                if os.path.exists(filename):
                    return spec_from_file_location(fullname, filename, loader=PsoLoader(filename))

        return None

 
class PsoLoader(Loader):
    def __init__(self, filename):
        self.filename = filename

    def create_module(self, spec):
        return None # use default module creation semantics

    def exec_module(self, module):
        with open(self.filename) as f:
            data = f.read()

        tree = transform_module(data, module.__name__)
        code = compile(tree, self.filename, 'exec')
        module.__ast__ = tree
        exec(code, vars(module))

def install():
    sys.meta_path.insert(1, PsoMetaFinder())

install()

def decorator(cls):
    class PsoProxy(object):
        def __init__(self, *args):
            self.__pso__ = cls(*args) 

        def __getattr__(self, name):
            print('Getting the {} attr of {}'.format(name, self.wrapped))
            return getattr(self.__pso__, name)

        def __getattribute__(self, name):
            print('Getting the {} attribute of {}'.format(name, self.wrapped))
            return getattr(self.wrapped, name)

    return Wrapper

def transaction_debug(func):
    def wrapper(*args, **kwargs):
        with Transacted() as __transaction_context__:
            counter = 0
            while True:
                try:
                    counter += 1
                    return func(*args, **kwargs)
                except ShmAbort:
                    if __transaction_context__.is_nested: # partial commit/rollback is not supported
                        raise
                    else:
                        if counter > 200:
                            global_debug_stop_on_contention()
                        transaction_rollback_retaining()
                        continue;
                except:
                    raise
                else:
                    break # success, commit

    return wrapper

def transaction(func):
    def wrapper(*args, **kwargs):
        with Transacted() as __transaction_context__:
            counter = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except ShmAbort:
                    if __transaction_context__.is_nested: # partial commit/rollback is not supported
                        raise
                    else:
                        transaction_rollback_retaining()
                        continue;
                except:
                    raise
                else:
                    break # success, commit

    return wrapper

class Transacted(AbstractContextManager):
    def __enter__(self):
        """Return `self` upon entering the runtime context."""
        self.is_nested = transaction_active()
        transaction_start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Returning None raises any exception triggered within the runtime context."""
        if exc_type is None:
            transaction_commit();
        else:
            transaction_rollback();
        return None

class Transient(AbstractContextManager):
    def __enter__(self):
        """Return `self` upon entering the runtime context."""
        transient_start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Returning None raises any exception triggered within the runtime context."""
        transient_end();
        return None

def class_fullname(o):
    klass = o.__class__
    module = klass.__module__
    if module == 'builtins':
        return klass.__qualname__ # avoid outputs like 'builtins.str'
    return module + '.' + klass.__qualname__

def pso(obj):
    # We cannot "replace" the slots, so we need to recreate the object
    if hasattr(obj, '__slots__'):
        raise Exception('Objects with slots are not supported')
    if isinstance(obj, ShmValue) or isinstance(obj, ShmList) or isinstance(obj, ShmDict):
        return obj
    if hasattr(obj, '__pso__'):
        return obj
    val = try_object_to_shm_value(obj)
    if val is not None:
        return val # simple immutable value

    if isinstance(obj, dict):
        pso_dict = ShmDict()
        for key in obj.__dict__:
            pso_dict[key] = pso(obj.__dict__[key])
        return pso_dict

    if isinstance(obj, list):
        pso_list = ShmDict()
        for item in obj:
            list.append(item)
        return pso_list

    if isinstance(obj, ShmObject):
        return obj

    return None # we don't support dynamic conversion of regular objects into shared yet.
    if not hasattr(obj, '__dict__'):
        return None

    # There's no other way to know for sure the __dict__ setter is blocked except actually calling this setter
    # Example for bound method (PyMethod_Type):
    # >> hasattr(meth, '__dict__')
    # True
    # >> meth.__dict__ = {}
    # AttributeError: 'method' object has no attribute '__dict__'
    obj.__dict__ = obj.__dict__

    # object translation via __dict__ assignment is not working, need to use ShmObject instead
    if isinstance(obj.__dict__, dict):
        pso_dict = ShmDict()
        for key in obj.__dict__:
            pso_dict[key] = pso(obj.__dict__[key])
        pso_dict['__pso__'] = pso_dict # kind of a "success" flag
        # Classic "pythonic" monkey patching.
        # Could've implemented a more correct pickle-style object copying, however that would disjoin references to old objects
        obj.__dict__ = pso_dict
        return obj
    else:
        raise Exception("I don't know what happened, but __dict__ is not dict")

if __name__ == '__main__':
    if not sys.argv[1]:
        Exception('Specify file name to run')
    filename = sys.argv[1]
    # similar to pdb._runscript and runpy._run_module_as_main
    main_globals = sys.modules["__main__"].__dict__
    del sys.argv[0]

    with open(filename, "rb") as fp:
        tree = transform_module(fp.read(), filename)
        code = compile(tree, filename, 'exec')
        main_globals['__ast__'] = tree
        exec(code, main_globals)
