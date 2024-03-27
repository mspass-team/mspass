from mspasspy.preprocessing.css30.datascope import DatascopeDatabase


def compare_selected(A, B, key):
    """
    Small test function compare column with index defined by the key
    argument in two DataFrame inputs A and B.   Returns True if the
    to have the same data and False if they differ.
    """
    Ak = A[key]
    Bk = B[key]
    diff = Ak.compare(Bk)
    if len(diff) == 0:
        return True
    else:
        return False


# testing section - pytest prototype
def test_DatascopeDatabase():
    """
    pytest funcion for :py:class:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase`.
    Uses a small, example set of datascope text files assumed located
    in the directory (relative to pytest directory)
    "./python/tests/data/datascope".   The class tested is driven
    by a pf file that is assumd to prexist in the
    directory "./data/pf" and called DatascopeDatabase.pf.
    That file is best created with an antelope dependent
    command line tool found in the scripts directory with the code for
    py:class:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase`
    """
    # these paths assume the standard mspass pytest run directory
    dbname = "./python/tests/data/datascope/testdb"
    pffile = "./data/pf/DatascopeDatabase.pf"
    db = DatascopeDatabase(dbname, pffile=pffile)
    # test loading without a select
    df = db.get_table("arrival")
    # test save to a different db
    df = db.df2table(df, "out", "arrival", append=False)
    # magic number matcbhing test database file
    assert len(df) == 651
    # test that in saved copy matches the original
    db2 = DatascopeDatabase("out", pffile=pffile)
    df2 = db2.get_table("arrival")
    dftest = df2.compare(df)
    assert len(dftest) == 0
    # repeat with a select and reordering of columns
    attributes_to_use = ["sta", "chan", "iphase", "time"]
    df = db.get_table("arrival", attributes_to_use=attributes_to_use)
    assert len(df) == 651
    df = db.df2table(df, "out", "arrival", append=False)
    df2 = db2.get_table("arrival")
    # here we can't compare all columns because the select
    # dropped some so we only test those that should be in the output.
    for k in attributes_to_use:
        assert compare_selected(df, df2, k)
    n0 = len(df)
    assert n0 == 651
    # test append mode on arrival
    dftest = db.df2table(df, "out", "arrival", append=True)
    df2 = db2.get_table("arrival")
    assert len(df2) == 2 * n0
    # test get_primary_keys method
    keylist = db.get_primary_keys("origin")
    assert len(keylist) == 6
    keylist = db.get_primary_keys("event")
    assert len(keylist) == 1
    assert keylist[0] == "evid"
    # basic test of join method
    # this test is not right for general use of this join in
    # css3.0 but it works for this small database
    dfj = db.join(df, "site", join_keys=["sta"])
    # this magic number derived from interactive testing
    # bote dataframe join does not duplicate sta as sta_site
    # because it is a key.  Probably needs a test for suffix option
    # of join
    assert len(dfj.columns) == 37
    # test get_nulls method.   expected to return a dict of
    # the size in the assert after the call
    nulls = db.get_nulls("site")
    assert isinstance(nulls, dict)
    assert len(nulls) == 12
    # Do a basic test on all the other tables in the test db
    for tbl in ["assoc", "event", "origin", "site", "wfdisc"]:
        df = db.get_table(tbl)
        # append set False to allow the test to be stateless
        df = db.df2table(df, "out", tbl, append=False)
        dfo = db2.get_table(tbl)
        dftest = dfo.compare(df)
        assert len(dftest) == 0
