from pyspark.sql.functions import *
from pyspark.sql.types import StringType

from main.python.model.FileReader import textReader, csvReader
from main.python.model.FileWriter import csvWriter


class SrcFiles:
    def __init__(self, tocorganization, tocpartycontractrole, toccontractstub, todomaininstance, tocpointofcontact,
                 lookupgroupind, mstr_org_tbl):
        self.tocorganization = tocorganization
        self.itocpartycontractrole = tocpartycontractrole
        self.toccontractstub = toccontractstub
        self.todomaininstance = todomaininstance
        self.tocpointofcontact = tocpointofcontact
        self.lookupgroupind = lookupgroupind
        self.mstr_org_tbl = mstr_org_tbl


class Process(object):
    def run(self, appconfig, spark):
        readInputFilesList = textReader(spark, appconfig.inputFilesPath).collect()
        inputFiles = readInputFilesList.map(
            lambda inputPath: csvReader(spark, appconfig.inputBasePath + "/" + inputPath))
        buildInput = buildInputDataSets(inputFiles)
        resjoin6befFilter = join6befFilter(
            joinTocOrgWithToc(buildInput.tocorganization, buildInput.tocpartycontractrole,
                              buildInput.toccontractstub, buildInput.todomaininstance),
            selGrpCntc(buildInput.tocpointofcontact))
        resselCol = selexpr(resjoin6befFilter)
        reslkpGrpInd = lkpGrpInd(buildInput.lookupgroupind, buildInput.mstr_org_tbl)
        resjoinLkpGrpInd = joinLkpGrpInd(resselCol, reslkpGrpInd)
        resfilterLkpGrpIndnulls = filterLkpGrpIndnulls(resjoinLkpGrpInd)
        resrejLkpGrpInd = rejLkpGrpInd(resjoinLkpGrpInd)
        rescapRejLkpGrpInd = capRejLkpGrpInd(resrejLkpGrpInd, reslkpGrpInd)
        resfunnelMstrPrty = funnelMstrPrty(resfilterLkpGrpIndnulls, rescapRejLkpGrpInd)
        reswriteContact = writeContact(spark, resfunnelMstrPrty)
        csvWriter(reswriteContact, appconfig.outputFilesPath + "/" + appconfig.ouputFileName)


def buildInputDataSets(readSrcFiles): return SrcFiles(
    readSrcFiles[0]
        .withColumn('toc_org_CABC', expr("CABC"))
        .withColumn('toc_org_IABC', expr("IABC"))
        .withColumn('toc_org_NameABC', expr("NAMEABC"))
        .withColumn("toc_org_CORPORA", expr("CORPORATETAXNABC"))
        .withColumn("toc_org_LASTUPDATEDATEABC", expr("LASTUPDATEDATEABC"))
        .drop('CABC', 'IABC', 'NAMEABC', 'CORPORATETAXNABC', 'STARTDATEABC', 'LASTUPDATEDATEABC')
        .select('toc_org_CABC', 'toc_org_IABC', 'toc_org_NameABC', 'toc_org_CORPORATETAXNABC',
                'toc_org_LASTUPDATEDATEABC', 'PARTYTYPEABC'),
    readSrcFiles[1] \
        .select(expr("C_OCPRTY_CONTRACTROLESABC"), expr("I_OCPRTY_ContractRolesABC"), expr("ROLEABC"),
                expr("C_OCCTRSTB_CONTRACTROLESABC"), expr("I_OCCTRSTB_CONTRACTROLESABC")), \
    readSrcFiles[2] \
        .withColumn('toc_con_stb_ENDDATEABC', expr("ENDDATEABC")) \
        .withColumn("toc_con_stb_STARTDATEABC", expr("STARTDATEABC")) \
        .withColumn("toc_con_stb_CABC", expr("CABC")) \
        .withColumn("toc_con_stb_IABC", expr("IABC")) \
        .drop('ENDDATEABC', 'STARTDATEABC', 'CABC', 'IABC') \
        .select(expr("toc_con_stb_ENDDATEABC"), expr("toc_con_stb_STARTDATEABC"), expr("toc_con_stb_CABC"),
                expr("toc_con_stb_IABC"), expr("CONTRACTSTATUABC"), expr("ReferenceNumbABC")), \
    readSrcFiles[3].select(expr("IABC"), expr("NAMEABC")), \
    readSrcFiles[4], readSrcFiles[5], readSrcFiles[6])


def joinTocOrgWithToc(df_tocorganization, df_tocpartycontractrole, df_toccontractstub,
                      df_todomaininstance): return df_tocorganization.join(df_tocpartycontractrole,
                                                                           df_tocorganization["toc_org_CABC"] ==
                                                                           df_tocpartycontractrole[
                                                                               "C_OCPRTY_CONTRACTROLESABC"] &
                                                                           df_tocorganization["toc_org_IABC"] ==
                                                                           df_tocpartycontractrole[
                                                                               "I_OCPRTY_CONTRACTROLESABC"], "left") \
    .join(df_toccontractstub,
          df_tocpartycontractrole["C_OCCTRSTB_CONTRACTROLESABC"] == df_toccontractstub["toc_con_stb_CABC"] &
          df_tocpartycontractrole["I_OCCTRSTB_CONTRACTROLESABC"] == df_toccontractstub["toc_con_stb_IABC"], "left") \
    .join(df_todomaininstance, expr("CONTRACTSTATUABC") == df_todomaininstance("IABC"), "left").withColumnRenamed(
    "NAMEABC", "GRP_CNTRCT_STATUSABC").drop("IABC") \
    .join(df_todomaininstance, expr("PARTYTYPEABC") == df_todomaininstance("IABC"), "left").withColumnRenamed("NAMEABC",
                                                                                                              "PRTY_TYPABC").drop(
    "IABC") \
    .join(df_todomaininstance, expr("ROLEABC") == expr("IABC"), "left").withColumnRenamed("NAMEABC", "PRTY_ROLEABC")


def selGrpCntc(df_tocpointofcontact): return df_tocpointofcontact \
    .withColumn("C_OCPRTY_POINTSOFCONTAABC",
                when(expr("C_OCPRTY_POINTSOFCONTAABC") == "NULL", 0).otherwise(expr("C_OCPRTY_POINTSOFCONTAABC"))) \
    .withColumn("EFFECTIVEFROMABC",
                when(expr("EFFECTIVEFROMABC") == "NULL", "1800-01-01 00:00:00.000").otherwise("EFFECTIVEFROMABC")) \
    .withColumn("EFFECTIVETOABC",
                when(expr("EFFECTIVETOABC") == "NULL", "2100-01-01 00:00:00.000").otherwise("EFFECTIVETOABC")) \
    .select(expr("C_OCPRTY_POINTSOFCONTAABC"), expr("I_OCPRTY_POINTSOFCONTAABC"), expr("CONTACTNAMEABC")) \
    .groupBy("C_OCPRTY_POINTSOFCONTAABC", "I_OCPRTY_POINTSOFCONTAABC").agg(
    max("CONTACTNAMEABC").alias("GRP_CNTCABC")).cache()


def join6befFilter(finalDf, selGrpCntc):
    join6befFilter = finalDf.join(selGrpCntc,
                                  finalDf("toc_org_CABC") == selGrpCntc("C_OCPRTY_POINTSOFCONTAABC") & finalDf(
                                      "toc_org_IABC") == selGrpCntc("I_OCPRTY_POINTSOFCONTAABC"), "left") \
        .select(finalDf("toc_org_CABC"), finalDf("toc_org_IABC"), finalDf("REFERENCENUMBABC"),
                finalDf("toc_org_NameABC").alias("ACCT_NMABC"),
                finalDf("toc_org_CORPORATETAXNABC").alias("GRP_TAX_IDABC"),
                finalDf("toc_con_stb_ENDDATEABC").alias("GRP_TERM_DTABC"),
                finalDf("toc_con_stb_STARTDATEABC").alias("GRP_START_DTABC"), finalDf("GRP_CNTRCT_STATUSABC"),
                finalDf("PRTY_TYPABC"), finalDf("PRTY_ROLEABC"), selGrpCntc("GRP_CNTCABC"),
                finalDf("toc_org_LASTUPDATEDATEABC"))

    return join6befFilter.filter(expr("REFERENCENUMBABC").isNotNull)


@udf(StringType())
def convert(x):
    if x.indexOfSlice(":") == -1:
        return x.substring(0, x.length)
    else:
        return x.substring(0, x.indexOfSlice(":") - 0)


@udf(StringType())
def cnvCharIndex(x):
    if x.indexOfSlice(":") == -1:
        return ' '
    else:
        return x.substring(x.indexOfSlice(":") + 1, len(x))


##convert_udf = udf(convert, StringType())
##cnvCharIndex_udf = udf(cnvCharIndex, StringType())


def selexpr(join6):
    selCol = join6.select(expr("toc_org_CABC").alias("ORG_CABC"), expr("toc_org_IABC").alias("ORG_IABC"),
                          when(expr("toc_org_IABC").isNull, "")
                          .when(expr("toc_org_IABC") > 0,
                                concat_ws("~", expr("toc_org_CABC"), expr("toc_org_IABC"))).alias("PRTY_KEYABC")
                          , convert(expr("REFERENCENUMBABC")).alias("PRTY_GRP_NBRABC"),
                          cnvCharIndex(expr("REFERENCENUMBABC")).alias("PRTY_ACCT_NBRABC"),
                          expr("ACCT_NMABC").alias("PRTY_ACCT_NMABC"), expr("GRP_TAX_IDABC").alias("PRTY_TAX_IDABC")
                          , expr("GRP_START_DTABC").alias("PRTY_EFF_DTTMABC")
                          , expr("GRP_TERM_DTABC").alias("PRTY_END_DTTMABC"),
                          expr("GRP_CNTRCT_STATUSABC").alias("PRTY_STUTSABC")
                          , expr("PRTY_TYPABC"), expr("PRTY_ROLEABC"),
                          expr("toc_org_LASTUPDATEDATEABC")
                          , expr("GRP_CNTCABC").alias(("PRTY_CONTC_NMABC"))
                          , lit(None).alias("PRTY_CONT_NMABC")
                          , lit(None).alias("PRTY_ERISA_INRABC"))

    return selCol


def lkpGrpInd(df_lookupgroupind, df_mstr_org_tbl):
    return df_lookupgroupind.join(df_mstr_org_tbl, trim(df_lookupgroupind("CURR_CDABC")) == trim(
        df_mstr_org_tbl("ORG_NMABC")) & df_mstr_org_tbl("ORG_STUSABC") == "ACTIVE", "left") \
        .select(trim(expr("GROUP_NOABC")).cast("string").alias("GROUP_NOABC"),
                trim(expr("ACCT_NOABC")).cast("string").alias("ACCT_NOABC"), expr("GROUP_NMABC"), expr("CURR_CDABC"),
                expr("MKT_SEGMNTABC"), expr("SITUS_STABC"), \
                expr("BOCABC"), expr("ACCT_MGRABC"), expr("ACCT_EXEC_NMABC"), expr("SALES_CDABC"), \
                expr("RATING_STABC"), expr("BILL_STABC"), expr("NO_OF_LVSABC"), expr("SRCABC"), \
                expr("ORG_SEQ_IDABC"), expr("PRTY_FED_IDABC"), expr("PRTY_SIC_CDABC"), expr("PRTY_SIC_CD_DESCABC"), \
                expr("PRTY_SLS_CDABC"), expr("PRTY_SLS_REP_NMABC"))


def joinLkpGrpInd(selCol, lkpGrpInd):
    return selCol.join(lkpGrpInd, selexpr("PRTY_GRP_NBRABC") == lkpGrpInd("GROUP_NOABC") & selexpr(
        "PRTY_ACCT_NBRABC") == lkpGrpInd("ACCT_NOABC"), "left") \
        .select(expr("PRTY_KEYABC")
                , expr("PRTY_GRP_NBRABC")
                , expr("GROUP_NMABC").alias("PRTY_GRP_NMABC")
                , expr("PRTY_ACCT_NBRABC"),
                expr("PRTY_ACCT_NMABC")
                , expr("PRTY_TYPABC"), expr("PRTY_ROLEABC")
                , expr("PRTY_STUTSABC")
                , expr("PRTY_TAX_IDABC")
                , expr("PRTY_EFF_DTTMABC")
                , expr("PRTY_END_DTTMABC")
                , expr("PRTY_CONTC_NMABC")
                , expr("MKT_SEGMNTABC").alias("PRTY_MRK_SEGT_TYPEABC")
                , expr("BOCABC").alias("PRTY_BENF_OFFICEABC")
                , expr("BILL_STABC").alias("PRTY_BILL_STABC")
                , expr("RATING_STABC").alias("PRTY_RATE_STABC")
                , lit(None).alias("PRTY_CONT_NMABC")
                , lit(None).alias("PRTY_ERISA_INRABC"))


def filterLkpGrpIndnulls(joinLkpGrpInd):
    return joinLkpGrpInd.filter(expr("PRTY_GRP_NMABC").isNotNull)


def rejLkpGrpInd(joinLkpGrpInd): return joinLkpGrpInd.select(expr("PRTY_KEYABC"), expr("PRTY_GRP_NBRABC"),
                                                             expr("PRTY_GRP_NMABC"), expr("PRTY_ACCT_NBRABC"),
                                                             expr("PRTY_ACCT_NMABC"), expr("PRTY_TYPABC"),
                                                             expr("PRTY_ROLEABC"), expr("PRTY_STUTSABC"),
                                                             expr("PRTY_TAX_IDABC")
                                                             , expr("PRTY_EFF_DTTMABC"), expr("PRTY_END_DTTMABC"),
                                                             expr("PRTY_CONTC_NMABC"), expr("PRTY_MRK_SEGT_TYPEABC")
                                                             , expr("PRTY_BENF_OFFICEABC"), expr("PRTY_BILL_STABC"),
                                                             expr("PRTY_RATE_STABC"), expr("PRTY_CONT_NMABC")
                                                             , expr("PRTY_ERISA_INRABC")).filter(
    expr("PRTY_GRP_NMABC").isNull)


def capRejLkpGrpInd(rejLkpGrpInd, lkpGrpInd): return rejLkpGrpInd.join(lkpGrpInd,
                                                                       rejLkpGrpInd("PRTY_GRP_NBRABC") == lkpGrpInd(
                                                                           "GROUP_NOABC"), "Inner") \
    .select(expr("PRTY_KEYABC")
            , expr("PRTY_GRP_NBRABC")
            , expr("PRTY_GRP_NMABC")
            , expr("PRTY_ACCT_NBRABC"),
            expr("PRTY_ACCT_NMABC")
            , expr("PRTY_TYPABC"), expr("PRTY_ROLEABC")
            , expr("PRTY_STUTSABC")
            , expr("PRTY_TAX_IDABC")
            , expr("PRTY_EFF_DTTMABC")
            , expr("PRTY_END_DTTMABC")
            , expr("PRTY_CONTC_NMABC")
            , expr("PRTY_MRK_SEGT_TYPEABC")
            , expr("PRTY_BENF_OFFICEABC")
            , expr("PRTY_BILL_STABC")
            , expr("PRTY_RATE_STABC")
            , expr("PRTY_CONT_NMABC")
            , expr("PRTY_ERISA_INRABC"))


def funnelMstrPrty(filterLkpGrpIndnulls, capRejLkpGrpInd): return filterLkpGrpIndnulls.union(capRejLkpGrpInd)


def writeContact(sparkSession, funnelMstrPrty):
    writeContact = funnelMstrPrty.select(
        concat_ws("|", "PRTY_KEYABC", "PRTY_GRP_NBRABC", "PRTY_GRP_NMABC", "PRTY_ACCT_NBRABC", "PRTY_ACCT_NMABC",
                  "PRTY_TYPABC", "PRTY_ROLEABC", "PRTY_STUTSABC", "PRTY_TAX_IDABC", "PRTY_EFF_DTTMABC",
                  "PRTY_END_DTTMABC", "PRTY_CONTC_NMABC", "PRTY_MRK_SEGT_TYPEABC", "PRTY_BENF_OFFICEABC",
                  "PRTY_BILL_STABC", "PRTY_RATE_STABC", "PRTY_CONT_NMABC", "PRTY_ERISA_INRABC"))
    # val writeContact = finalDf.select( "toc_org_CABC","toc_org_IABC","toc_org_NameABC","toc_org_CORPORATETAXNABC","toc_org_LASTUPDATEDATEABC","toc_con_stb_ENDDATEABC","toc_con_stb_STARTDATEABC","GRP_CNTRCT_STATUSABC","PRTY_TYPABC","PRTY_ROLEABC")
    writeContact.show(5, False)
    return writeContact
