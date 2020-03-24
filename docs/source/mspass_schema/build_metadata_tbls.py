import sys
sys.path.append('/Users/pavlis/src/mspass/python/mspasspy')
from ccore import MetadataDefinitions
from ccore import MDtype
from ccore import AntelopePf
def write_group(pf, tag,mdef):
    """
    This function is implements the repetitious task of creating a
    csv file of attributes that have some kind of logical or required
    grouping in MsPASS.   The tag argument is used to create a csv
    file of the same name for output.  e.g. if tag is 'source' the
    function will write results to a file called 'source.csv'.  The
    tag is also used to fetch an &Tbl{} block from the AntelopePf object
    pf that is assumed to contain the set of unique keys that define
    that grouping.   We intentionally do not handle exceptions for
    get errors in that context because this program is expected to
    be run under the hood by Travis (or equivalent) and we want the
    program to exit with an error condition if there is such an error
    to avoid corrupting automatically generated documentation.

    :param pf: is an AntelopePF that is assumed to contain a branch
      (&Arr) with the tag defined by the branch argument.
    :param branch: is the tag of the requested branch.  Note again
       the output csv file will be the value of branch with file
       extension ".csv"
    :param mdef: is the MetadataDefinitions object created by the main
       program here.  We pass it for efficiency.
    """
    tbl=pf.get_tbl(tag)
    filename=tag+".csv"
    fh=open(filename,"w+")
    fh.write('\"%s\",\"%s\",\"%s\",\"%s\"\n' % ("key","type","Mutable","Concept"))
    for k in tbl:
        t=mdef.type(k)
        tstr="undefined"
        if(t==MDtype.Int64):
            tstr="integer"
        elif(t==MDtype.Double):
            tstr="real"
        elif(t==MDtype.String):
            tstr="string"
        writeable=mdef.writeable(k)
        wstr="undefined"
        if(writeable):
            wstr="Yes"
        else:
            wstr="No"
        fh.write('\"%s\",\"%s\",\"%s\",\"%s\"\n' % (k,tstr,wstr,mdef.concept(k)))
    fh.close()

# These are file names for outputs
# this is all entris in natrual sort order (keys alphabetical)
allfile="all.csv"
allrst="all.rst"
# This contains all aliases
aliasfile="aliases.csv"

mdef=MetadataDefinitions()
fh=open(allfile,"w+")
fh.write('\"%s\",\"%s\",\"%s\",\"%s\"\n' % ("key","type","Mutable","Concept"))
mdk=mdef.keys()
for k in mdk:
    t=mdef.type(k)
    tstr="undefined"
    if(t==MDtype.Int64):
        tstr="integer"
    elif(t==MDtype.Double):
        tstr="real"
    elif(t==MDtype.String):
        tstr="string"
    writeable=mdef.writeable(k)
    wstr="undefined"
    if(writeable):
        wstr="Yes"
    else:
        wstr="No"
    fh.write('\"%s\",\"%s\",\"%s\",\"%s\"\n' % (k,tstr,wstr,mdef.concept(k)))
fh.close()
fh=open(aliasfile,"w+")
fh.write('\"%s\",\"%s\"\n' % ("Unique key","Valid aliases"))
for k in mdk:
    aliaslist=mdef.aliases(k)
    if(len(aliaslist)>0):
        if(len(aliaslist)==1):
            fh.write('\"%s\",\"%s\"' % (k,aliaslist[0]))
        else:
            fh.write('\"%s\",\"' % k )
            for i in range(len(aliaslist)-1):
                fh.write('%s : ' % aliaslist[i])
            val=aliaslist[len(aliaslist)-1]
            fh.write('%s\"' % val)
        fh.write('\n')

# Now build the group tables using the function above.  Need the pf first
pf=AntelopePf('build_metadata_tbls.pf')
write_group(pf,'site',mdef)
write_group(pf,'source',mdef)
write_group(pf,'obspy_trace',mdef)
write_group(pf,'sitechan',mdef)
write_group(pf,'3Cdata',mdef)
write_group(pf,'sitechan',mdef)
write_group(pf,'phase',mdef)
write_group(pf,'MongoDB',mdef)
write_group(pf,'files',mdef)
