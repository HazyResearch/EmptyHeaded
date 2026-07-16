#!/usr/bin/env python
"""Turn an EmptyHeaded generated query header into a relocatable runner API."""

import json
import os
import re
import shutil
import sys


STRING_GROUP = re.compile(r'(?:(?:"(?:\\.|[^"\\])*")\s*)+')
STRING_LITERAL = re.compile(r'"(?:\\.|[^"\\])*"')


def make_portable(source, destination, database_root):
    if os.path.abspath(source) != os.path.abspath(destination):
        shutil.copy2(source, destination)

    with open(destination) as stream:
        text = stream.read()

    prefix = database_root.rstrip("/")

    def replace_path(match):
        literals = STRING_LITERAL.findall(match.group(0))
        value = "".join(json.loads(literal) for literal in literals)
        if value == prefix or value.startswith(prefix + "/"):
            suffix = value[len(prefix):]
            return "EMPTYHEADED_DATABASE_ROOT " + json.dumps(suffix)
        return match.group(0)

    text = STRING_GROUP.sub(replace_path, text)
    text = text.replace("void run_0(mymap *input_tries)",
                        "long run_0(mymap *input_tries)", 1)
    old_tail = "  thread_pool::deleteThreadPool();\n}"
    new_tail = "  thread_pool::deleteThreadPool();\n  return TriangleCount;\n}"
    if old_tail not in text:
        raise RuntimeError("could not locate generated run_0 tail")
    text = text.replace(old_tail, new_tail, 1)

    guard = """#ifndef EMPTYHEADED_DATABASE_ROOT
#error "Define EMPTYHEADED_DATABASE_ROOT as a C++ string literal"
#endif

"""
    text = guard + text

    if prefix in text:
        raise RuntimeError("absolute database path remains in portable header")
    with open(destination, "w") as stream:
        stream.write(text)


def main(argv):
    if len(argv) != 4:
        raise SystemExit("usage: make_portable_emptyheaded_header.py SOURCE DEST DATABASE_ROOT")
    make_portable(argv[1], argv[2], argv[3])


if __name__ == "__main__":
    main(sys.argv)
