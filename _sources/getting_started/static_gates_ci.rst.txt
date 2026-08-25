Static analysis and sanitizer gates
===================================

Pull requests always run Ruff 0.12.8 over ``python/mspasspy`` and
``python/tests`` with ``E9,F63,F7,F82`` selected.  Pull requests that change
``cxx/**`` additionally run the complete native CTest suite in separate ASan
and UBSan builds.  The stable ``static-gates`` job accepts only successful
applicable jobs or two skipped sanitizer jobs when no native path changed.

Branch protection must not require ``static-gates`` until this workflow has
been merged into ``master`` and produced the check there.  Configuring it
earlier could leave pull requests waiting for a context that the base branch
cannot create.  After a successful run on ``master``, a repository
administrator should add exactly ``static-gates`` to the required status
checks and confirm it through the branch-protection API.
