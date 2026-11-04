# Execute un fichier Python decoupe en cellules "# %%" comme un notebook : affiche le code puis le resultat
import ast, sys, traceback
src = open(sys.argv[1]).read()
g = {"__name__": "__main__"}
for i, cell in enumerate(src.split("\n# %%")):
    code = cell.strip("\n")
    if not code.strip():
        continue
    print("\n>>> [cellule %d]" % i, flush=True)
    for line in code.splitlines():
        print("... " + line)
    sys.stdout.flush()
    try:
        tree = ast.parse(code)
        last = None
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            last = ast.Expression(tree.body.pop().value)
        exec(compile(tree, "<cell>", "exec"), g)
        if last is not None:
            v = eval(compile(last, "<cell>", "eval"), g)
            if v is not None:
                print(repr(v))
    except Exception:
        print("!!! EXCEPTION")
        traceback.print_exc(limit=3, file=sys.stdout)
    sys.stdout.flush()
