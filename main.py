from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import sys
from datetime import datetime
from time import strftime
now = datetime.now()

class SparkDataProcessing:
    def __init__(self, app_name="ATLANS4HANA"):
        with open(r"D:\pyspark\Project\PythonProject\PythonProject\AtlanS4HANA\files\S4HANA_FILEPATH_DETAILS.json") as fileObj:
            self.pathdetails = json.load(fileObj)

        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def getSchema(self,filename):
        match filename:
            case "TABLE":
                tableFile_schema = StructType([
                    StructField("tableName", StringType(), True),
                    StructField("activationStatus", StringType(), True),
                    StructField("entryVersion", StringType(), True),
                    StructField("tableCategory", StringType(), True),
                    StructField("lastChangedbyUser", StringType(), True),
                    StructField("lastChangedDate", StringType(), True),
                    StructField("lastChangedTime", StringType(), True)
                ])
                return tableFile_schema

            case "TABLE_DESC":
                tableDescFile_schema = StructType([
                    StructField("tableName", StringType(), True),
                    StructField("activationStatus", StringType(), True),
                    StructField("entryVersion", StringType(), True),
                    StructField("repositoryObjDesc", StringType(), True)
                ])
                return tableDescFile_schema

            case "PACKAGE":
                packageFile_schema = StructType([
                    StructField("programId", StringType(), True),
                    StructField("objectType", StringType(), True),
                    StructField("objectName", StringType(), True),
                    StructField("packageName", StringType(), True)
                ])
                return packageFile_schema

            case "TABLE_FIELDS":
                tableFieldFile_schema = StructType([
                    StructField("tableName", StringType(), True),
                    StructField("fieldName", StringType(), True),
                    StructField("POSITION", StringType(), True),
                    StructField("keyFlag", StringType(), True),
                    StructField("rollName", StringType(), True),
                    StructField("checkTable", StringType(), True),
                    StructField("abapDataType", StringType(), True),
                    StructField("indicator", StringType(), True),
                    StructField("dataType", StringType(), True),
                    StructField("length", StringType(), True),
                    StructField("decimal", StringType(), True)
                ])
                return tableFieldFile_schema

            case "VIEW_NAME":
                viewNameFile_schema = StructType([
                    StructField("VIEWNAME", StringType(), True),
                    StructField("AS4LOCAL", StringType(), True),
                    StructField("AS4VERS", StringType(), True),
                    StructField("AS4USER", StringType(), True),
                    StructField("AS4DATE", StringType(), True),
                    StructField("VIEWCLASS", StringType(), True)
                ])
                return viewNameFile_schema

            case "VIEW_TEXT":
                viewTextFile_schema = StructType([
                    StructField("VIEWNAME", StringType(), True),
                    StructField("AS4LOCAL", StringType(), True),
                    StructField("AS4VERS", StringType(), True),
                    StructField("DDTEXT", StringType(), True)
                ])
                return viewTextFile_schema

            case "VIEW_FIELD_MAPPING":
                viewFieldMappingFile_schema = StructType([
                    StructField("VIEWNAME", StringType(), True),
                    StructField("VIEWFIELD", StringType(), True),
                    StructField("TABNAME", StringType(), True),
                    StructField("FIELDNAME", StringType(), True)
                ])
                return viewFieldMappingFile_schema

            case "VIEW_JOIN_CONDITION":
                viewJoinConditionFile_schema = StructType([
                    StructField("CONDNAME", StringType(), True),
                    StructField("TABNAME", StringType(), True),
                    StructField("FIELDNAME", StringType(), True),
                    StructField("NEGATION", StringType(), True),
                    StructField("OPERATOR", StringType(), True),
                    StructField("CONSTANTS", StringType(), True),
                    StructField("CONTLINE", StringType(), True),
                    StructField("AND_OR", StringType(), True),
                    StructField("OFFSET", StringType(), True),
                    StructField("FLENGTH", StringType(), True),
                    StructField("MCOFIELD", StringType(), True)
                ])
                return viewJoinConditionFile_schema

            case "ABAP_PROGRAM":
                abapProgramFile_schema = StructType([
                    StructField("NAME", StringType(), True),
                    StructField("SUBC", StringType(), True),
                    StructField("CNAM", StringType(), True),
                    StructField("CDAT", StringType(), True),
                    StructField("UNAM", StringType(), True),
                    StructField("UDAT", StringType(), True)
                ])
                return abapProgramFile_schema

            case "ABAP_DESCRIPTION":
                abapDescriptionFile_schema = StructType([
                    StructField("NAME", StringType(), True),
                    StructField("TEXT", StringType(), True)
                ])
                return abapDescriptionFile_schema

            case "TCODE":
                TCodeFile_schema = StructType([
                    StructField("TCODE", StringType(), True),
                    StructField("PGMNA", StringType(), True)
                ])
                return TCodeFile_schema

            case "TCODE_TEXT":
                TCode_textFile_schema = StructType([
                    StructField("TCODE", StringType(), True),
                    StructField("TTEXT", StringType(), True)
                ])
                return TCode_textFile_schema

            case "FUNC_PARAMETER":
                funcParameter_schema = StructType([
                    StructField("FUNCNAME", StringType(), True),
                    StructField("PARAMETER", StringType(), True),
                    StructField("PARAMTYPE", StringType(), True)
                ])
                return funcParameter_schema

            case "FUNC_TEXT":
                funcText_schema = StructType([
                    StructField("FUNCNAME", StringType(), True),
                    StructField("STEXT", StringType(), True)
                ])
                return funcText_schema

            case "FUNC_MODULES":
                funcModules_schema = StructType([
                    StructField("FUNCNAME", StringType(), True),
                    StructField("NewPNAME", StringType(), True)
                ])
                return funcModules_schema

            case "FUNC_PARAMETER_STEXT":
                funcParameterSTEXT_schema = StructType([
                    StructField("FUNCNAME", StringType(), True),
                    StructField("PARAMETER", StringType(), True),
                    StructField("STEXT", StringType(), True)
                ])
                return funcParameterSTEXT_schema

            case "TDEVC":
                TDEVC_schema = StructType([
                    StructField("DEVCLASS", StringType(), True),
                    StructField("DLVUNIT", StringType(), True),
                    StructField("COMPONENT", StringType(), True)
                ])
                return TDEVC_schema

            case "CVERS_REF":
                CVERS_REF_schema = StructType([
                    StructField("COMPONENT", StringType(), True),
                    StructField("DESC_TEXT", StringType(), True)
                ])
                return CVERS_REF_schema

            case "DF14L":
                DF14L_schema = StructType([
                    StructField("FCTR_ID", StringType(), True),
                    StructField("PS_POSID", StringType(), True)
                ])
                return DF14L_schema

            case "DF14T":
                DF14T_schema = StructType([
                    StructField("FCTR_ID", StringType(), True),
                    StructField("NAME", StringType(), True)
                ])
                return DF14T_schema

            case "TF_DD03T":
                TF_DD03T_schema = StructType([
                    StructField("DD03T_TABNAME", StringType(), True),
                    StructField("DD03T_FIELDNAME", StringType(), True),
                    StructField("DD03T_DDTEXT", StringType(), True)
                ])
                return TF_DD03T_schema

            case "TF_DD04T":
                TF_DD04T_schema = StructType([
                    StructField("DD04T_ROLLNAME", StringType(), True),
                    StructField("DD04T_DDTEXT", StringType(), True)
                ])
                return TF_DD04T_schema

            case _:
                raise Exception(f"Enter Valid File Name : {filename}")

    def readData(self,filename, file_format="csv", header=False, delimiter=";"):

        filepath = self.pathdetails.get(filename,"NA")
        print("Read Data : ",filepath)
        match filename:
            case "TABLE":
                tableFile_schema = self.getSchema("TABLE")
                self.tableFile_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(tableFile_schema) \
                    .load(filepath)
                self.tableFile_df = self.tableFile_df.distinct()
                return  self.tableFile_df

            case "TABLE_DESC":
                tableDescFile_schema = self.getSchema("TABLE_DESC")
                self.tableDescFile_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(tableDescFile_schema) \
                    .load(filepath)
                self.tableDescFile_df = self.tableDescFile_df.distinct()
                return self.tableDescFile_df


            case "PACKAGE":
                packageFile_schema = self.getSchema("PACKAGE")
                self.packageFile_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(packageFile_schema) \
                    .load(filepath)
                self.packageFile_df = self.packageFile_df.distinct()
                return self.packageFile_df

            case "TABLE_FIELDS":
                tableFieldFile_schema = self.getSchema("TABLE_FIELDS")
                self.tableFieldFile_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(tableFieldFile_schema) \
                    .load(filepath)
                self.tableFieldFile_df = self.tableFieldFile_df.distinct()
                return self.tableFieldFile_df

            case "VIEW_NAME":
                viewNameFile_schema = self.getSchema("VIEW_NAME")
                self.View_Name_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(viewNameFile_schema) \
                    .load(filepath)
                self.View_Name_df = self.View_Name_df.distinct()
                return self.View_Name_df

            case "VIEW_TEXT":
                viewTextFile_schema = self.getSchema("VIEW_TEXT")
                self.View_text_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(viewTextFile_schema) \
                    .load(filepath)
                self.View_text_df = self.View_text_df.distinct()
                return self.View_text_df

            case "VIEW_FIELD_MAPPING":
                viewFieldMappingFile_schema = self.getSchema("VIEW_FIELD_MAPPING")
                self.View_FieldMapping_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(viewFieldMappingFile_schema) \
                    .load(filepath)
                self.View_FieldMapping_df = self.View_FieldMapping_df.distinct()
                return self.View_FieldMapping_df

            case "VIEW_JOIN_CONDITION":
                viewJoinConditionFile_schema = self.getSchema("VIEW_JOIN_CONDITION")
                self.View_JoinCondition_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(viewJoinConditionFile_schema) \
                    .load(filepath)
                self.View_JoinCondition_df = self.View_JoinCondition_df.distinct()
                return self.View_JoinCondition_df

            case "ABAP_PROGRAM":
                abapProgramFile_schema = self.getSchema("ABAP_PROGRAM")
                self.ABAP_Program_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(abapProgramFile_schema) \
                    .load(filepath)
                self.ABAP_Program_df = self.ABAP_Program_df.distinct()
                return self.ABAP_Program_df

            case "ABAP_DESCRIPTION":
                abapProgramFile_schema = self.getSchema("ABAP_DESCRIPTION")
                self.ABAP_Description_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(abapProgramFile_schema) \
                    .load(filepath)
                self.ABAP_Description_df = self.ABAP_Description_df.distinct()
                return self.ABAP_Description_df

            case "TCODE":
                TCodeFile_schema = self.getSchema("TCODE")
                self.TCode_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(TCodeFile_schema) \
                    .load(filepath)
                self.TCode_df = self.TCode_df.distinct()
                return self.TCode_df

            case "TCODE_TEXT":
                TCode_textFile_schema = self.getSchema("TCODE_TEXT")
                self.TCode_text_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(TCode_textFile_schema) \
                    .load(filepath)
                self.TCode_text_df = self.TCode_text_df.distinct()
                return self.TCode_text_df

            case "FUNC_PARAMETER":
                funcParameter_schema = self.getSchema("FUNC_PARAMETER")
                self.funcParameter_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(funcParameter_schema) \
                    .load(filepath)
                self.funcParameter_df = self.funcParameter_df.distinct()
                return self.funcParameter_df

            case "FUNC_TEXT":
                funcText_schema = self.getSchema("FUNC_TEXT")
                self.funcText_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(funcText_schema) \
                    .load(filepath)
                self.funcText_df = self.funcText_df.distinct()
                return self.funcText_df

            case "FUNC_MODULES":
                funcModules_schema = self.getSchema("FUNC_MODULES")
                self.funcModules_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(funcModules_schema) \
                    .load(filepath)
                self.funcModules_df = self.funcModules_df.distinct()
                return self.funcModules_df

            case "FUNC_PARAMETER_STEXT":
                funcParameterSTEXT_schema = self.getSchema("FUNC_PARAMETER_STEXT")
                self.funcParameterSTEXT_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(funcParameterSTEXT_schema) \
                    .load(filepath)
                self.funcParameterSTEXT_df = self.funcParameterSTEXT_df.distinct()
                return self.funcParameterSTEXT_df

            case "TDEVC":
                TDEVC_schema = self.getSchema("TDEVC")
                self.TDEVC_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(TDEVC_schema) \
                    .load(filepath)
                self.TDEVC_df = self.TDEVC_df.distinct()
                return self.TDEVC_df

            case "CVERS_REF":
                CVERS_REF_schema = self.getSchema("CVERS_REF")
                self.CVERS_REF_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(CVERS_REF_schema) \
                    .load(filepath)
                self.CVERS_REF_df = self.CVERS_REF_df.distinct()
                return self.CVERS_REF_df

            case "DF14L":
                DF14L_schema = self.getSchema("DF14L")
                self.DF14L_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(DF14L_schema) \
                    .load(filepath)
                self.tableFieldFile_df = self.DF14L_df.distinct()
                return self.DF14L_df

            case "DF14T":
                DF14T_schema = self.getSchema("DF14T")
                self.DF14T_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(DF14T_schema) \
                    .load(filepath)
                self.DF14T_df = self.DF14T_df.distinct()
                return self.DF14T_df

            case "TF_DD03T":
                TF_DD03T_schema = self.getSchema("TF_DD03T")
                self.TF_DD03T_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(TF_DD03T_schema) \
                    .load(filepath)
                self.TF_DD03T_df = self.TF_DD03T_df.distinct()
                return self.TF_DD03T_df

            case "TF_DD04T":
                TF_DD04T_schema = self.getSchema("TF_DD04T")
                self.TF_DD04T_df = self.spark.read.format(file_format) \
                    .option("inferSchema", True) \
                    .option("Header", header) \
                    .option("delimiter", delimiter) \
                    .schema(TF_DD04T_schema) \
                    .load(filepath)
                self.TF_DD04T_df = self.TF_DD04T_df.distinct()
                return self.TF_DD04T_df


            case _:
                raise Exception(f"Error occurred while reading the File : {filename}")

    def writeData(self, output_path, output_format="json"):

        if '/' in output_path:
            x = output_path.split('/')
            print("Output Linux Path: ",output_path)
        else:
            x = output_path.split('\\')
        filename = x[-1].split('.')[0]
        print("Output Windows Path : ",output_path)

        match filename:

            case "Table_Merged":
                self.final_df = self.table()
                self.final_df.coalesce(1).write.format(output_format).mode("overwrite").save(output_path)
                print(f"Data written to {output_path}")

            case "Column_Merged":
                self.table_column_field_df = self.column()
                self.table_column_field_df.coalesce(1).write.format(output_format).mode("overwrite").save(output_path)
                print(f"Data written to {output_path}")

            case "ViewNameText_Merged":
                self.FinalViewNameText_df = self.viewNameText()
                self.FinalViewNameText_df.coalesce(1).write.format(output_format).mode("overwrite").save(output_path)
                print(f"Data written to {output_path}")

            case "ViewFieldMapping_Merged":
                self.viewFieldMapping_df = self.viewField()
                self.viewFieldMapping_df.coalesce(1).write.format(output_format).mode("overwrite").save(output_path)
                print(f"Data written to {output_path}")

            case "ViewJoinCondition_Merged":
                self.ViewJoinCondition_df = self.viewJoin()
                self.ViewJoinCondition_df.coalesce(1).write.format(output_format).mode("overwrite").save(output_path)
                print(f"Data written to {output_path}")

            case "ABAPProgramDesc_Merged":
                self.ABAPProgramDesc_df = self.abapProgDesc()
                self.ABAPProgramDesc_df.coalesce(1).write.format(output_format).mode("overwrite").save(output_path)
                print(f"Data written to {output_path}")

            case "TCodeText_Merged":
                self.TCodeText_df = self.tcodeTcodeText()
                self.TCodeText_df.coalesce(1).write.format(output_format).mode("overwrite").save(output_path)
                print(f"Data written to {output_path}")


            case "FunctionModules_Merged":
                self.FinalFunctionModule = self.functionModule()
                self.FinalFunctionModule.coalesce(1).write.format(output_format).mode("overwrite").save(output_path)
                print(f"Data written to {output_path}")

            case _:
                raise Exception(f"Enter valid file path {output_path} and file name : {filename}")

    def auditLogging(self,*args):
        try:
            filepath = self.pathdetails.get("AUDITLOG", "NA")
            time = strftime(now.strftime("%d-%m-%Y %H:%M:%S"))
            with open(filepath, 'a') as fileObj:
                fileObj.write('Time  : ' + time + ' ')
                fileObj.write('Error Description : ' + ''.join(args) + '\n')
        except Exception as e:
            print(e)

    def tdevc_cversRef_df14l_df14t(self):
        TDEVC_df = self.readData("TDEVC")
        CVERS_REF_df = self.readData("CVERS_REF")
        DF14L_df = self.readData("DF14L")
        DF14T_df = self.readData("DF14T")

        TDEVC_CVERS_df = TDEVC_df.join(CVERS_REF_df, trim(TDEVC_df.DLVUNIT) == trim(CVERS_REF_df.COMPONENT), "left") \
            .select(trim(TDEVC_df.DEVCLASS).alias("DEVCLASS"), trim(TDEVC_df.DLVUNIT).alias("DLVUNIT"),
                    trim(TDEVC_df.COMPONENT).alias("APP_COMPONENT"), trim(CVERS_REF_df.COMPONENT).alias("SOFT_COMPONENT"),
                    trim(CVERS_REF_df.DESC_TEXT).alias("DESC_TEXT"))

        DF14L_DF14T_df = DF14L_df.join(DF14T_df, trim(DF14L_df.FCTR_ID) == trim(DF14T_df.FCTR_ID), "left") \
            .select(trim(DF14L_df.FCTR_ID).alias("FCTR_ID"), trim(DF14L_df.PS_POSID).alias("PS_POSID"), trim(DF14T_df.NAME).alias("NAME"))

        self.TDEVC_CVERS_DF14L_DF14T_df = TDEVC_CVERS_df.join(DF14L_DF14T_df, trim(TDEVC_CVERS_df.APP_COMPONENT) == trim(DF14L_DF14T_df.FCTR_ID), "left") \
            .select(trim(TDEVC_CVERS_df.DEVCLASS).alias("DEVCLASS"), trim(DF14L_DF14T_df.FCTR_ID).alias("FCTR_ID"),
                    trim(TDEVC_CVERS_df.DLVUNIT).alias("DLVUNIT"), trim(TDEVC_CVERS_df.APP_COMPONENT).alias("COMPONENT"),
                    trim(DF14L_DF14T_df.PS_POSID).alias("PS_POSID"), trim(DF14L_DF14T_df.NAME).alias("NAME"),
                    trim(TDEVC_CVERS_df.DESC_TEXT).alias("DESC_TEXT"))

        return self.TDEVC_CVERS_DF14L_DF14T_df

    def table(self):
        packageFile_df = self.readData("PACKAGE")
        tableFile_df = self.readData("TABLE")
        tableDescFile_df = self.readData("TABLE_DESC")
        TDEVC_CVERS_DF14L_DF14T_df = self.tdevc_cversRef_df14l_df14t()

        packageFileTable_df = packageFile_df.filter(trim(packageFile_df.objectType) == 'TABL')

        table_df = tableFile_df.join(tableDescFile_df, (tableDescFile_df.tableName == tableFile_df.tableName) & (
                    tableDescFile_df.activationStatus == tableFile_df.activationStatus) & (
                                                 tableDescFile_df.entryVersion == tableFile_df.entryVersion), "left") \
            .select(tableFile_df.tableName, tableFile_df.tableCategory, tableFile_df.lastChangedbyUser, tableFile_df.lastChangedDate,
                    tableFile_df.activationStatus, tableDescFile_df.repositoryObjDesc)

        # print(" Total Table File Records : " , table_df.count())

        final_df = table_df.join(packageFileTable_df, trim(table_df.tableName) == trim(packageFileTable_df.objectName), "left").filter(
            (trim(table_df.tableCategory) == "TRANSP") | (trim(table_df.tableCategory) == "POOL") | (trim(table_df.tableCategory) == "CLUSTER")) \
            .select(trim(table_df.tableName).alias("TABNAME"), trim(table_df.tableCategory).alias("TABCLASS"),
                    trim(table_df.lastChangedbyUser).alias("AS4USER"), trim(table_df.lastChangedDate).alias("AS4DATE"),
                    trim(table_df.activationStatus).alias("CONTFLAG"), trim(table_df.repositoryObjDesc).alias("DDTEXT"),
                    trim(packageFileTable_df.packageName).alias("DEVCLASS"))

        # changes of 26 JAN 2025
        self.final_df = final_df.join(TDEVC_CVERS_DF14L_DF14T_df, trim(final_df.DEVCLASS) == trim(TDEVC_CVERS_DF14L_DF14T_df.DEVCLASS), "left") \
            .select(trim(final_df.TABNAME).alias("TABNAME"), trim(final_df.TABCLASS).alias("TABCLASS"), trim(final_df.AS4USER).alias("AS4USER"),
                    trim(final_df.AS4DATE).alias("AS4DATE"), trim(final_df.CONTFLAG).alias("CONTFLAG"), trim(final_df.DDTEXT).alias("DDTEXT"),
                    trim(final_df.DEVCLASS).alias("DEVCLASS"), trim(TDEVC_CVERS_DF14L_DF14T_df.FCTR_ID).alias("FCTR_ID"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.DLVUNIT).alias("DLVUNIT"), trim(TDEVC_CVERS_DF14L_DF14T_df.COMPONENT).alias("COMPONENT"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.PS_POSID).alias("PS_POSID"), trim(TDEVC_CVERS_DF14L_DF14T_df.NAME).alias("NAME"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.DESC_TEXT).alias("DESC_TEXT"))

        return self.final_df

    def column(self):

        packageFile_df = self.readData("PACKAGE")
        tableFieldFile_df = self.readData("TABLE_FIELDS")
        TF_DD04T_df = self.readData("TF_DD04T")
        TF_DD03T_df = self.readData("TF_DD03T")
        TDEVC_CVERS_DF14L_DF14T_df = self.tdevc_cversRef_df14l_df14t()

        packageFileColumn_df = packageFile_df.filter(trim(packageFile_df.objectType) == 'TABL')

        table_column_field_df = tableFieldFile_df.join(packageFileColumn_df,
                                                       trim(tableFieldFile_df.tableName) == trim(packageFileColumn_df.objectName), "left") \
            .select(trim(tableFieldFile_df.tableName).alias("TABNAME"), trim(tableFieldFile_df.fieldName).alias("FIELDNAME"),
                    trim(tableFieldFile_df.POSITION).alias("POSITION"), trim(tableFieldFile_df.keyFlag).alias("KEYFLAG"),
                    trim(tableFieldFile_df.rollName).alias("ROLLNAME"), trim(tableFieldFile_df.checkTable).alias("CHECKTABLE"),
                    trim(tableFieldFile_df.abapDataType).alias("INTTYPE"), trim(tableFieldFile_df.indicator).alias("NOTNULL"),
                    trim(tableFieldFile_df.dataType).alias("DATATYPE"), trim(tableFieldFile_df.length).alias("LENG"),
                    trim(tableFieldFile_df.decimal).alias("DECIMALS"), trim(packageFileColumn_df.packageName).alias("DEVCLASS"))

        # changes of 26 JAN 2025
        table_column_field_temp_df = table_column_field_df.join(TDEVC_CVERS_DF14L_DF14T_df,
                                                                trim(table_column_field_df.DEVCLASS) == trim(TDEVC_CVERS_DF14L_DF14T_df.DEVCLASS),
                                                                "left") \
            .select(trim(table_column_field_df.TABNAME).alias("TABNAME"), trim(table_column_field_df.FIELDNAME).alias("FIELDNAME"),
                    trim(table_column_field_df.POSITION).alias("POSITION"), trim(table_column_field_df.KEYFLAG).alias("KEYFLAG"),
                    trim(table_column_field_df.ROLLNAME).alias("ROLLNAME"), trim(table_column_field_df.CHECKTABLE).alias("CHECKTABLE"),
                    trim(table_column_field_df.INTTYPE).alias("INTTYPE"), trim(table_column_field_df.NOTNULL).alias("NOTNULL"),
                    trim(table_column_field_df.DATATYPE).alias("DATATYPE"), trim(table_column_field_df.LENG).alias("LENG"),
                    trim(table_column_field_df.DECIMALS).alias("DECIMALS"), trim(table_column_field_df.DEVCLASS).alias("DEVCLASS"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.FCTR_ID).alias("FCTR_ID"), trim(TDEVC_CVERS_DF14L_DF14T_df.DLVUNIT).alias("DLVUNIT"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.COMPONENT).alias("COMPONENT"), trim(TDEVC_CVERS_DF14L_DF14T_df.PS_POSID).alias("PS_POSID"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.NAME).alias("NAME"), trim(TDEVC_CVERS_DF14L_DF14T_df.DESC_TEXT).alias("DESC_TEXT"))

        # changes of 11 Feb 2025

        table_column_field_df = table_column_field_temp_df.join(TF_DD04T_df,
                                                                trim(table_column_field_temp_df.ROLLNAME) == trim(TF_DD04T_df.DD04T_ROLLNAME), "left") \
            .join(TF_DD03T_df, (trim(table_column_field_temp_df.TABNAME) == trim(TF_DD03T_df.DD03T_TABNAME)) & (
                    trim(table_column_field_temp_df.FIELDNAME) == trim(TF_DD03T_df.DD03T_FIELDNAME)), "left") \
            .withColumn("DDTEXT", when(trim(table_column_field_temp_df.ROLLNAME) == "", trim(TF_DD03T_df.DD03T_DDTEXT)).otherwise(
            trim(TF_DD04T_df.DD04T_DDTEXT))) \
            .filter(~col('FIELDNAME').startswith('.'))

        self.table_column_field_df = table_column_field_df.fillna("").select(
            ["TABNAME", "FIELDNAME", "POSITION", "KEYFLAG", "ROLLNAME", "DDTEXT", "CHECKTABLE", "INTTYPE", "NOTNULL", "DATATYPE", "LENG", "DECIMALS",
             "DEVCLASS", "FCTR_ID", "DLVUNIT", "COMPONENT", "PS_POSID", "NAME", "DESC_TEXT"])

        return self.table_column_field_df

    def viewNameText(self):
        packageFile_df = self.readData("PACKAGE")
        View_Name_df = self.readData("VIEW_NAME")
        View_text_df = self.readData("VIEW_TEXT")
        TDEVC_CVERS_DF14L_DF14T_df = self.tdevc_cversRef_df14l_df14t()

        packageFileView_df = packageFile_df.filter(trim(packageFile_df.objectType) == 'VIEW')

        FinalViewNameText_df = View_Name_df.join(View_text_df, trim(View_Name_df.VIEWNAME) == trim(View_text_df.VIEWNAME), "left") \
            .join(packageFileView_df, trim(packageFileView_df.objectName) == trim(View_Name_df.VIEWNAME), "left") \
            .select(trim(View_Name_df.VIEWNAME).alias("VIEWNAME"), trim(View_text_df.DDTEXT).alias("DDTEXT"),
                    trim(View_Name_df.VIEWCLASS).alias("VIEWCLASS"), trim(View_Name_df.AS4USER).alias("AS4USER"),
                    trim(View_Name_df.AS4DATE).alias("AS4DATE"), trim(packageFileView_df.packageName).alias("DEVCLASS")).distinct()

        # changes of 26 JAN 2025
        self.FinalViewNameText_df = FinalViewNameText_df.join(TDEVC_CVERS_DF14L_DF14T_df,
                                                         trim(FinalViewNameText_df.DEVCLASS) == trim(TDEVC_CVERS_DF14L_DF14T_df.DEVCLASS), "left") \
            .select(trim(FinalViewNameText_df.VIEWNAME).alias("VIEWNAME"), trim(FinalViewNameText_df.DDTEXT).alias("DDTEXT"),
                    trim(FinalViewNameText_df.VIEWCLASS).alias("VIEWCLASS"), trim(FinalViewNameText_df.AS4USER).alias("AS4USER"),
                    trim(FinalViewNameText_df.AS4DATE).alias("AS4DATE"), trim(FinalViewNameText_df.DEVCLASS).alias("DEVCLASS"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.FCTR_ID).alias("FCTR_ID"), trim(TDEVC_CVERS_DF14L_DF14T_df.DLVUNIT).alias("DLVUNIT"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.COMPONENT).alias("COMPONENT"), trim(TDEVC_CVERS_DF14L_DF14T_df.PS_POSID).alias("PS_POSID"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.NAME).alias("NAME"), trim(TDEVC_CVERS_DF14L_DF14T_df.DESC_TEXT).alias("DESC_TEXT"))

        return self.FinalViewNameText_df

    def viewField(self):
        View_FieldMapping_df = self.readData("VIEW_FIELD_MAPPING")

        self.viewFieldMapping_df = View_FieldMapping_df.select(trim(View_FieldMapping_df.VIEWNAME).alias("VIEWNAME"),
                                                          trim(View_FieldMapping_df.VIEWFIELD).alias("VIEWFIELD"),
                                                          trim(View_FieldMapping_df.TABNAME).alias("TABNAME"),
                                                          trim(View_FieldMapping_df.FIELDNAME).alias("FIELDNAME"))

        return self.viewFieldMapping_df

    def viewJoin(self):
        View_JoinCondition_df = self.readData("VIEW_JOIN_CONDITION")
        self.ViewJoinCondition_df = View_JoinCondition_df.select(trim(View_JoinCondition_df.CONDNAME).alias("CONDNAME"),
                                                            trim(View_JoinCondition_df.TABNAME).alias("TABNAME"),
                                                            trim(View_JoinCondition_df.FIELDNAME).alias("FIELDNAME"),
                                                            trim(View_JoinCondition_df.NEGATION).alias("NEGATION"),
                                                            trim(View_JoinCondition_df.OPERATOR).alias("OPERATOR"),
                                                            trim(View_JoinCondition_df.CONSTANTS).alias("CONSTANTS"),
                                                            trim(View_JoinCondition_df.CONTLINE).alias("CONTLINE"),
                                                            trim(View_JoinCondition_df.AND_OR).alias("AND_OR"),
                                                            trim(View_JoinCondition_df.OFFSET).alias("OFFSET"),
                                                            trim(View_JoinCondition_df.FLENGTH).alias("FLENGTH"),
                                                            trim(View_JoinCondition_df.MCOFIELD).alias("MCOFIELD"))

        return self.ViewJoinCondition_df

    def abapProgDesc(self):
        packageFile_df = self.readData("PACKAGE")
        ABAP_Program_df = self.readData("ABAP_PROGRAM")
        ABAP_Description_df = self.readData("ABAP_DESCRIPTION")
        TDEVC_CVERS_DF14L_DF14T_df = self.tdevc_cversRef_df14l_df14t()
        packageFileABAP_df = packageFile_df.filter(trim(packageFile_df.objectType) == 'PROG')

        ABAPProgramDesc_df = ABAP_Program_df.join(packageFileABAP_df, trim(packageFileABAP_df.objectName) == trim(ABAP_Program_df.NAME), "left") \
            .join(ABAP_Description_df, trim(ABAP_Program_df.NAME) == trim(ABAP_Description_df.NAME), "left") \
            .select(trim(ABAP_Program_df.NAME).alias("NAME"), trim(ABAP_Program_df.SUBC).alias("SUBC"), trim(ABAP_Program_df.CNAM).alias("CNAM"),
                    trim(ABAP_Program_df.CDAT).alias("CDAT"), trim(ABAP_Program_df.UNAM).alias("UNAM"), trim(ABAP_Program_df.UDAT).alias("UDAT"),
                    trim(ABAP_Description_df.TEXT).alias("TEXT"), trim(packageFileABAP_df.packageName).alias("DEVCLASS"))

        # changes of 26 JAN 2025
        self.ABAPProgramDesc_df = ABAPProgramDesc_df.join(TDEVC_CVERS_DF14L_DF14T_df,
                                                     trim(ABAPProgramDesc_df.DEVCLASS) == trim(TDEVC_CVERS_DF14L_DF14T_df.DEVCLASS), "left") \
            .select(trim(ABAPProgramDesc_df.NAME).alias("NAME"), trim(ABAPProgramDesc_df.SUBC).alias("SUBC"),
                    trim(ABAPProgramDesc_df.CNAM).alias("CNAM"), trim(ABAPProgramDesc_df.CDAT).alias("CDAT"),
                    trim(ABAPProgramDesc_df.UNAM).alias("UNAM"), trim(ABAPProgramDesc_df.UDAT).alias("UDAT"),
                    trim(ABAPProgramDesc_df.TEXT).alias("TEXT"), trim(ABAPProgramDesc_df.DEVCLASS).alias("DEVCLASS"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.FCTR_ID).alias("FCTR_ID"), trim(TDEVC_CVERS_DF14L_DF14T_df.DLVUNIT).alias("DLVUNIT"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.COMPONENT).alias("COMPONENT"), trim(TDEVC_CVERS_DF14L_DF14T_df.PS_POSID).alias("PS_POSID"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.NAME).alias("NAME1"), trim(TDEVC_CVERS_DF14L_DF14T_df.DESC_TEXT).alias("DESC_TEXT"))

        return self.ABAPProgramDesc_df

    def tcodeTcodeText(self):
        packageFile_df = self.readData("PACKAGE")
        TCode_df = self.readData("TCODE")
        TCode_text_df = self.readData("TCODE_TEXT")
        TDEVC_CVERS_DF14L_DF14T_df = self.tdevc_cversRef_df14l_df14t()
        packageFileTcode_df = packageFile_df.filter(trim(packageFile_df.objectType) == 'TRAN')

        TCodeText_df = TCode_df.join(TCode_text_df, trim(TCode_df.TCODE) == trim(TCode_text_df.TCODE), "left") \
            .join(packageFileTcode_df, trim(packageFileTcode_df.objectName) == trim(TCode_df.TCODE), "left") \
            .select(trim(TCode_df.TCODE).alias("TCODE"), trim(TCode_df.PGMNA).alias("PGMNA"), trim(TCode_text_df.TTEXT).alias("TTEXT"),
                    trim(packageFileTcode_df.packageName).alias("DEVCLASS"))

        # changes of 26 JAN 2025
        self.TCodeText_df = TCodeText_df.join(TDEVC_CVERS_DF14L_DF14T_df, trim(TCodeText_df.DEVCLASS) == trim(TDEVC_CVERS_DF14L_DF14T_df.DEVCLASS), "left") \
            .select(trim(TCodeText_df.TCODE).alias("TCODE"), trim(TCodeText_df.PGMNA).alias("PGMNA"), trim(TCodeText_df.TTEXT).alias("TTEXT"),
                    trim(TCodeText_df.DEVCLASS).alias("DEVCLASS"), trim(TDEVC_CVERS_DF14L_DF14T_df.DLVUNIT).alias("DLVUNIT"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.FCTR_ID).alias("FCTR_ID"), trim(TDEVC_CVERS_DF14L_DF14T_df.COMPONENT).alias("COMPONENT"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.PS_POSID).alias("PS_POSID"), trim(TDEVC_CVERS_DF14L_DF14T_df.NAME).alias("NAME"),
                    trim(TDEVC_CVERS_DF14L_DF14T_df.DESC_TEXT).alias("DESC_TEXT"))

        return self.TCodeText_df

    def functionModule(self):
        packageFile_df = self.readData("PACKAGE")
        funcModules_df = self.readData("FUNC_MODULES")
        funcParameter_df = self.readData("FUNC_PARAMETER")
        funcText_df = self.readData("FUNC_TEXT")
        funcParameterSTEXT_df = self.readData("FUNC_PARAMETER_STEXT")

        packageFileFunctionModule_df = packageFile_df.filter(trim(packageFile_df.objectType) == 'FUGR')
        funcModules_df = funcModules_df.withColumn("PNAME", regexp_replace("NewPNAME", "SAPL", ""))

        # Function Parameter for Type - X - Grouped Logic
        funcParameterX_df = funcParameter_df.filter(funcParameter_df.PARAMTYPE == 'X').select(trim(funcParameter_df.FUNCNAME).alias("FUNCNAME_Temp"),
                                                                                              trim(funcParameter_df.PARAMETER).alias(
                                                                                                  "PARAMETER_Temp"))
        funcParameterX_df = funcParameterX_df.join(funcParameterSTEXT_df,
                                                   (trim(funcParameterSTEXT_df.FUNCNAME) == trim(funcParameterX_df.FUNCNAME_Temp)) & (
                                                               trim(funcParameterSTEXT_df.PARAMETER) == trim(funcParameterX_df.PARAMETER_Temp)),
                                                   "left").select(trim(funcParameterX_df.FUNCNAME_Temp).alias("FUNCNAME_Temp"),
                                                                  trim(funcParameterX_df.PARAMETER_Temp).alias("PARAMETER_Temp"),
                                                                  trim(funcParameterSTEXT_df.STEXT).alias("STEXT_Temp"))

        # Replacing Null Values with Blank
        funcParameterX_df = funcParameterX_df.fillna("")

        funcParameterX_df = funcParameterX_df.withColumn("PARAMETER_X",
                                                         create_map(lit('PARAMETER'), col('PARAMETER_Temp'), lit('STEXT'), col('STEXT_Temp'))).select(
            col('FUNCNAME_Temp').alias('FUNCNAME'), col('PARAMETER_X').alias('PARAMETER_X'))


        groupedfuncParameterX_df = funcParameterX_df.groupBy("FUNCNAME").agg(collect_list("PARAMETER_X").alias("PARAMETER_X"))


        #  Function Parameter for Type - I  - Grouped Logic
        funcParameterI_df = funcParameter_df.filter((funcParameter_df.PARAMTYPE == 'I')).select(
            trim(funcParameter_df.FUNCNAME).alias("FUNCNAME_Temp"), trim(funcParameter_df.PARAMETER).alias("PARAMETER_Temp"))
        funcParameterI_df = funcParameterI_df.join(funcParameterSTEXT_df,
                                                   (trim(funcParameterSTEXT_df.FUNCNAME) == trim(funcParameterI_df.FUNCNAME_Temp)) & (
                                                               trim(funcParameterSTEXT_df.PARAMETER) == trim(funcParameterI_df.PARAMETER_Temp)),
                                                   "left").select(trim(funcParameterI_df.FUNCNAME_Temp).alias("FUNCNAME_Temp"),
                                                                  trim(funcParameterI_df.PARAMETER_Temp).alias("PARAMETER_Temp"),
                                                                  trim(funcParameterSTEXT_df.STEXT).alias("STEXT_Temp"))

        # Replacing Null Values with Blank
        funcParameterI_df = funcParameterI_df.fillna("")

        funcParameterI_df = funcParameterI_df.withColumn("PARAMETER_I",
                                                         create_map(lit('PARAMETER'), col('PARAMETER_Temp'), lit('STEXT'), col('STEXT_Temp'))).select(
            col('FUNCNAME_Temp').alias('FUNCNAME'), col('PARAMETER_I').alias('PARAMETER_I'))
        # display(funcParameterI_df)

        groupedfuncParameterI_df = funcParameterI_df.groupBy("FUNCNAME").agg(collect_list("PARAMETER_I").alias("PARAMETER_I"))


        #  Function Parameter for Type - E  - Grouped Logic
        funcParameterE_df = funcParameter_df.filter(funcParameter_df.PARAMTYPE == 'E').select(trim(funcParameter_df.FUNCNAME).alias("FUNCNAME_Temp"),
                                                                                              trim(funcParameter_df.PARAMETER).alias(
                                                                                                  "PARAMETER_Temp"))
        funcParameterE_df = funcParameterE_df.join(funcParameterSTEXT_df,
                                                   (trim(funcParameterSTEXT_df.FUNCNAME) == trim(funcParameterE_df.FUNCNAME_Temp)) & (
                                                               trim(funcParameterSTEXT_df.PARAMETER) == trim(funcParameterE_df.PARAMETER_Temp)),
                                                   "left").select(trim(funcParameterE_df.FUNCNAME_Temp).alias("FUNCNAME_Temp"),
                                                                  trim(funcParameterE_df.PARAMETER_Temp).alias("PARAMETER_Temp"),
                                                                  trim(funcParameterSTEXT_df.STEXT).alias("STEXT_Temp"))

        # Replacing Null Values with Blank
        funcParameterE_df = funcParameterE_df.fillna("")

        funcParameterE_df = funcParameterE_df.withColumn("PARAMETER_E",
                                                         create_map(lit('PARAMETER'), col('PARAMETER_Temp'), lit('STEXT'), col('STEXT_Temp'))).select(
            col('FUNCNAME_Temp').alias('FUNCNAME'), col('PARAMETER_E').alias('PARAMETER_E'))
        # display(funcParameterE_df)

        groupedfuncParameterE_df = funcParameterE_df.groupBy("FUNCNAME").agg(collect_list("PARAMETER_E").alias("PARAMETER_E"))

        # Function Module
        funcModuleFinal_df = funcModules_df.join(packageFileFunctionModule_df,
                                                 trim(packageFileFunctionModule_df.objectName) == trim(funcModules_df.PNAME), "left") \
            .join(funcText_df, trim(funcModules_df.FUNCNAME) == trim(funcText_df.FUNCNAME), "left") \
            .select(trim(funcModules_df.FUNCNAME).alias("FUNCNAME"), trim(funcModules_df.PNAME).alias("FUNC_GROUP"),
                    trim(funcModules_df.NewPNAME).alias("PNAME"), trim(packageFileFunctionModule_df.packageName).alias("DEVCLASS"),
                    trim(funcText_df.STEXT).alias("STEXT"))

        # Final Function File Creation
        FinalFunctionModule = funcModuleFinal_df.join(groupedfuncParameterX_df,
                                                      trim(funcModuleFinal_df.FUNCNAME) == trim(groupedfuncParameterX_df.FUNCNAME), "left") \
            .join(groupedfuncParameterI_df, trim(funcModuleFinal_df.FUNCNAME) == trim(groupedfuncParameterI_df.FUNCNAME), "left") \
            .join(groupedfuncParameterE_df, trim(funcModuleFinal_df.FUNCNAME) == trim(groupedfuncParameterE_df.FUNCNAME), "left") \
            .select(trim(funcModuleFinal_df.FUNCNAME).alias("FUNCNAME"), trim(funcModuleFinal_df.FUNC_GROUP).alias("FUNC_GROUP"),
                    trim(funcModuleFinal_df.PNAME).alias("PNAME"), trim(funcModuleFinal_df.DEVCLASS).alias("DEVCLASS"),
                    trim(funcModuleFinal_df.STEXT).alias("STEXT"), trim(funcModuleFinal_df.DLVUNIT).alias("DLVUNIT"),
                    trim(funcModuleFinal_df.COMPONENT).alias("COMPONENT"), trim(funcModuleFinal_df.PS_POSID).alias("PS_POSID"),
                    trim(funcModuleFinal_df.NAME).alias("NAME"), trim(funcModuleFinal_df.DESC_TEXT).alias("DESC_TEXT"),
                    groupedfuncParameterX_df.PARAMETER_X.alias("PARAMETERS_X"), groupedfuncParameterI_df.PARAMETER_I.alias("PARAMETERS_I"),
                    groupedfuncParameterE_df.PARAMETER_E.alias("PARAMETERS_E"))

        self.FinalFunctionModule = FinalFunctionModule.fillna("")

        return self.FinalFunctionModule

if __name__ == '__main__':

    try:
        processor = SparkDataProcessing()
        # processor.writeData("D:\pyspark\Project\PythonProject\PythonProject\AtlanS4HANA\curatedS4HANA\Table_Merged.json")

        if len(sys.argv) > 1:
            processor.writeData(sys.argv[1])
        else:
            # print("Usage: python main.py <FileOutputPath>")
            raise Exception(f"Please Provide Valid Output Path : {sys.argv[1]}")


    except Exception as e:
        print("Exception Occurred from : ",e.__class__.__name__)
        print("Exception Occurred : ",e)
        processor.auditLogging(str(e))