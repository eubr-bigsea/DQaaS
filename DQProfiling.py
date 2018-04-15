# coding=utf-8
"""
IMPORT STEP
"""

import re
import datetime
import time
import subprocess
import traceback
import json

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.functions import lit
import sys
import os


def save_txt(document, path):
    """
    Save an Rdd into a .txt file if it does not already exists

    Args:
        document(rdd): The document to save.
        path(string): The path in which the document have to be saved
    """
    try:
        document.coalesce(1).saveAsTextFile(path)
    except Exception:
        # the file has already been saved for the current data source
        pass


def save_json(document, schema, path, saveMode=None):
    """
    Save an Rdd or a dataframe into a .json file with the saveMode specified

    Args:
        document(Rdd or Dataframe): The document to save.
        schema(list): The schema of the columns of the rdd, used to convert to Dataframe if necessary
        path(string): The path in which the document have to be saved
        saveMode(string): A string representing the save mode

    """
    if saveMode is None:
        try:
            print("Saving Json...")
            if schema is None:
                document.coalesce(1).write.json(path)
            else:
                document.toDF(schema).coalesce(1).write.json(path)
        except Exception:
            print("The file already exists")
            import traceback
            traceback.print_exc()
    else:
        print("Modifying Json...")
        if schema is None:
            document.coalesce(1).write.mode(saveMode).json(path)
        else:
            document.toDF(schema).coalesce(1).write.mode(saveMode).json(path)


"""
DEFINITION STEP - XML
"""


def extract_main_elements_xml(document):
    """
    This function search in the xml data the main elements: separators, elements and timestamps and return a new rdd

    Args:
        document(line): The line of the rdd to parse.
    """

    matchObj = re.findall(r'<separator>(.*)</separator>', document)
    if matchObj:
        return matchObj[0]

    matchObj = re.findall(r'<element>(.*)</element>', document)
    if matchObj:
        return "element"

    matchObj = re.findall(r'<timestamp>(.*)</timestamp>', document)
    if matchObj:
        return "element"


def extract_header(document):
    """
    This function allow to extract the header of the columns from the xml if available

    Args:
        document(line): The line of the rdd to parse.
    """
    matchObj = re.findall(r'<header>(.*)</header>', document)
    if matchObj:
        return matchObj[0]


def extract_timestamp_format(document):
    """
    This function allow to extract only the timestamps' columns from the xml

    Args:
        document(line): The line of the rdd to parse.
    """
    matchObj = re.findall(r'<timestamp>(.*)</timestamp>', document)
    if matchObj:
        return matchObj[0]


def regular_parsing_xml(document):
    """
    Main function to extract the regular expression from the xml to be used to derive the elements for the analysis 
    - It must be upgraded

    Args:
        document(line): The line of the rdd to parse.
    """
    prev = ""
    next = False

    prec = ""
    post = ""

    for s in document.toLocalIterator():
        if(next):
            post = post + str(s)
            next = False
        else:
            if(str(s) == "element"):
                prec = prec + str(prev)
                next = True
        prev = str(s)

    prec = ''.join(set(prec))
    post = ''.join(set(post))
    regString = "[" + prec + "]" + "(.*?)" + "[" + post + "]"
    regString = regString.replace('"', '')
    return regString


"""
DEFINITION STEP - DOCUMENT STRUCTURATION
"""


def regular_parsing(document, regex):
    """
    Main function to derive all the elements that respects the regex String

    Args:
        document(line): The line of the rdd to parse.
            regex(string): The string that contains the regular expression
    """
    return re.findall(regex, document)


def escape_removal(document):
    """
    Function to remove the escape from the elements of the file. 
    The function should be upgraded to eliminate all the undesired symbols

    Args:
        document(line): The line of the rdd to parse.
    """
    return re.sub(r'\\', "", document)


def quote_removal(document):
    """
    Function to remove the double quotes from the data

    Args:
        document(line): The line of the rdd to parse.
    """
    return re.sub(r'"', "", document)


def comma_to_dot_number_conversion(document):
    """
    Function to convert the numbers with ,-separation to .-separated

    Args:
        document(line): The line of the rdd to parse.
    """
    return re.sub(r'(\d+),(\d+)', r'\1.\2', document)


def encode_single_value(line):
    line[0][0] = line[0][0].encode("UTF8", "ignore")
    return line


"""
DEFINITION STEP - DIMENSION ANALYSIS
"""


def available_dimensions(document, dimensionAllowedTypes, timeFormat):
    """
    This function allows to derive the type of each column element, with the format of a timestamp if included, 
    and the column that can be analyzed for each dimension

    Args:
            document(rdd): The rdd containing the data
            dimensionAllowedTypes(dict): Dictionary containing the list of allowed type of attribute 
                for each available quality dimension
            timeFormat(list): List of strings containing the possible timestamp format of the attributes
    """

    print(" ")
    print("Available Dimensions")
    print(" ")

    dataTypes = []

    lineLength = len(document.take(1)[0])
    datetimeFormat = [i for i in range(lineLength)]

    for i in range(lineLength):
        print("-Attribute " + str(i))
        found = False
        try:
            # convert into float and use count as a trigger action for the mapping to
            # begin, if the field cannot be converted an Exception will be
            # triggered
            partialDocument = document.map(
                lambda x: float(x[i].replace(',', '.'))).count()
            dataTypes.append("float")
            print("--float")
        except Exception:
            # try to convert each attribute for each format of datetime found
            # in the xml or given as input the
            for s in timeFormat:
                try:
                    # convert into datetime and use count as a trigger action for the mapping
                    # to begin, if the field cannot be converted an Exception will be
                    # triggered
                    partialDocument = document.map(
                        lambda x: datetime.datetime.strptime(x[i], str(s))).count()
                    datetimeFormat[i] = str(s)
                    found = True
                    dataTypes.append("datetime")
                    print("--timestamp")
                except Exception:
                    print("--string")
                    pass

            if not found:
                # if the right type of field has not been found it will be
                # considered a string
                dataTypes.append("string")

    print("-Types of elements of 1 row")
    print(dataTypes)

    # build preliminary information; given as parameter the type of value of
    # columns that can be analyzed for each dimension, this piece of code will
    # derive the columns that can be analyzed for each dimension

    seen = {}
    for i, elem in enumerate(dataTypes):
        try:
            seen[elem].append(i)
        except KeyError:
            seen[elem] = [i]
    print(seen)

    copy = dimensionAllowedTypes.copy()
    for i in copy.iterkeys():
        try:
            a = []
            for elem in dimensionAllowedTypes[i]:
                a.extend(seen[str(elem)])
            a = set(a)
            a = list(a)
            dimensionAllowedTypes[i] = a
            print(i)
            print(dimensionAllowedTypes[i])
            if(i == "Timeliness"):
                for s in a:
                    dataTypes[s] = datetimeFormat[s]
                    print(dataTypes[s])
        except Exception:
            if(dimensionAllowedTypes[i][0] == "all"):
                dimensionAllowedTypes[i] = range(len(dataTypes))
            else:
                if i != "Volume":
                    del dimensionAllowedTypes[i]

    return dimensionAllowedTypes, dataTypes


"""
DEFINITION STEP - Dimension Analysis
"""


def precision(
        sc,
        sqlContext,
        document,
        columns,
        dataTypes,
        volatility,
        dictHeaderPosition,
        resultFolder,
        dimensionColumn):
    """
    This function calculate the dimension of precision for each numerical attribute 
    and return the Global values in addition to some Profiling information like minimum and maximum found values 
    per attribute

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            columns(list): Indexes of the attributes requested in the analysis
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: 
                Number of hours to consider the data still recent
            dictHeaderPosition(dict): Dictionary that associate the index of the attributes 
                to their corresponding name
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes 
                for each available quality dimension
    """

    print(" ")
    print("Precision")
    print(" ")

    finalPrecisionRdd = sc.emptyRDD()
    attrPrecision = []
    attrName = []

    for i in columns:
        print("-Numerical Attribute = " + str(dictHeaderPosition[i]))

        reShift = False
        precisionRdd = document.filter(lambda x: re.match(
            "^\d+?(\.|\,)?\d+?$", x[i])).map(lambda x: (float(x[i].replace(',', '.'))))

        statsDoc = precisionRdd.stats()
        meanAttribute = float(statsDoc.mean())
        devAttribute = float(statsDoc.stdev())
        minValue = float(statsDoc.min())
        maxValue = float(statsDoc.max())

        if (minValue < 0) & (maxValue > 0):
            reShift = True
            realMean = meanAttribute + abs(minValue)
        else:
            realMean = meanAttribute

        attributePrecision = max(
            0.0, 1.0 - abs(devAttribute / float(realMean)))

        # calculate final aggregated value for precision
        attrPrecision.append(attributePrecision)
        attrName.append("Precision_" + str(dictHeaderPosition[i]))
        attrPrecision.append(devAttribute)
        attrName.append("Precision(Deviation)_" + str(dictHeaderPosition[i]))

        attrPrecision.append(minValue)
        attrName.append("Min_Value_" + str(dictHeaderPosition[i]))
        attrPrecision.append(maxValue)
        attrName.append("Max_Value_" + str(dictHeaderPosition[i]))

    return document, attrName, attrPrecision


def count_null(l):
    count = 0
    for i in range(len(l)):
        if(l[i] != "null" and (l[i] is not None) and l[i] != "" and l[i] != "nan"):
            count += 1
    return count


def completeness_missing(
        sc,
        sqlContext,
        document,
        columns,
        dataTypes,
        volatility,
        dictHeaderPosition,
        resultFolder,
        dimensionColumn):
    """
    This function calculate the value of the Part of Completeness regarding the missing elements per line 
    and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            columns(list): Indexes of the attributes requested in the analysis
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: 
                Number of hours to consider the data still recent
            dictHeaderPosition(dict): Dictionary that associate the index of the attributes 
                to their corresponding name
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes 
                for each available quality dimension
    """

    print(" ")
    print("Completeness_Missing")
    print(" ")

    # Update File Analysis

    completenessDocument = (
        document.map(lambda x: x[0:] + [float(len(x))])
        .map(lambda x: x[0:-1] + [str(count_null(x[0:-1]) / (x[-1]))]))

    # print("########## COMPLETENESS_TEST #########")
    # print(completenessDocument.take(5))

    globalDocument = document.flatMap(lambda x: x)
    globalCount = globalDocument.count()
    filteredCount = (
        globalDocument.filter(lambda x: x != "null")
        .filter(lambda x: x is not None)
        .filter(lambda x: x != "")
        .filter(lambda x: x != "nan")
        .count())

    qualityCompletenessMissing = filteredCount / float(globalCount)

    print("--Final Global Completeness Missing: " +
          str(qualityCompletenessMissing))

    return completenessDocument, ["Completeness_Missing"], [qualityCompletenessMissing]


def time_or_zero(x, stringFormat):
    try:
        return datetime.datetime.strptime(x, stringFormat)
    except:
        return datetime.datetime.min


def timeliness(
        sc,
        sqlContext,
        document,
        columns,
        dataTypes,
        volatility,
        dictHeaderPosition,
        resultFolder,
        dimensionAllowedTypes):
    """
    This function calculate the dimension of Timeliness and prepare the Rdd to evaluate the dimension of Completeness_Frequency,, then save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            columns(list): Indexes of the attributes requested in the analysis
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: 
                Number of hours to consider the data still recent
            dictHeaderPosition(dict): Dictionary that associate the index of the attributes 
                to their corresponding name
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes 
                for each available quality dimension
    """

    print(" ")
    print("Timeliness and Completeness Frequency")
    print(" ")

    # the analysis time is set here
    datetimeNow = datetime.datetime.now()
    #print("-Save the timestamp of the analysis")

    # the volatility is converted in seconds
    volatilityTime = float(volatility) * 3600

    finalCompletenessRdd = sc.emptyRDD()
    finalTimeRdd = sc.emptyRDD()

    attrTimeFreq = []
    attrName = []

    timelinessInfoDict = {"timelinessNames": [],
                          "volatility": volatility, "timeFormats": []}

    for i in columns:

        print(" ")
        print("-Timestamp Attribute = " + str(dictHeaderPosition[i]))

        # select the first format of timestamp found
        stringFormat = dataTypes[i]

        datetimeDocument = document.map(
            lambda x: x + [time_or_zero(x[i], stringFormat)])

        min_max_rdd = datetimeDocument.filter(
            lambda x: x[-1] > datetime.datetime.min).map(lambda x: x[-1])
        if(min_max_rdd.count() == 0):
            attrName.append("Timeliness_Mean_" + str(dictHeaderPosition[i]))
            attrName.append("Timeliness_Max_" + str(dictHeaderPosition[i]))
            attrName.append("Timeliness_Min_" + str(dictHeaderPosition[i]))
            return datetimeDocument, attrName, None

        attrTimeFreq.append(min_max_rdd.min().strftime(stringFormat))
        attrTimeFreq.append(min_max_rdd.max().strftime(stringFormat))

        attrName.append("Minimum_Timestamp_" + str(dictHeaderPosition[i]))

        attrName.append("Maximum_Timestamp_" + str(dictHeaderPosition[i]))

        # search global frequency
        updateGlobal = False
        try:
            updateRateGlobalDF = sqlContext.read.json(
                resultFolder + "/update_rate_global_" + str(dictHeaderPosition[i]))
            print("-Update rate for the source available")
            updateGlobal = True
        except Exception:
            print("-Update rate for the source not available")

        # Global Frequency
        if updateGlobal:
            print(" ")
            print("--Calculate Global Frequency")

            completenessHour = (datetimeDocument.map(lambda x: (x[-1].hour, 1))
                                .reduceByKey(lambda x, y: x + y)
                                .map(lambda x: (x[0], float(x[1])))
                                )

            print("---Current  Elements per Hour")
            print(completenessHour.take(5))

            updateRateGlobal = updateRateGlobalDF.rdd.map(
                lambda x: (x.Hour, x.Frequency))

            completenessHour = (
                completenessHour.join(updateRateGlobal)
                .map(lambda x: (x[0], x[1][0], max(0.0, min(1.0, x[1][0] / (float(x[1][1]) * 3600)))))
                .map(lambda x: (x[0], x[1], x[2])))

            print("---Completeness Frequency per Hour")

            qualityCompletenessFrequency = completenessHour.map(lambda x: x[
                                                                2]).mean()

            print("---Update Completeness Frequency Value = " +
                  str(qualityCompletenessFrequency))

            attrTimeFreq.append(qualityCompletenessFrequency)
            attrName.append("Completeness_Frequency_" +
                            str(dictHeaderPosition[i]))

        print("--Calculate Global Timeliness")

        timelinessDocument = (
            datetimeDocument.map(lambda x: x[0:-1] + [(datetimeNow - x[-1])])
            .map(lambda x:
                 x[0:-1] + [(float(x[-1].microseconds + (x[-1].seconds + x[-1].days * 24 * 3600) * 10**6) / 10**6)])
            .map(lambda x: x[0:-1] + [(max(0.0, 1 - (x[-1] / volatilityTime)))]))

        # Final aggregated value for timeliness
        statsDoc = timelinessDocument.map(lambda x: x[-1]).stats()
        timelinessMeanValue = float(statsDoc.mean())
        timelinessMaxValue = float(statsDoc.max())
        timelinessMinValue = float(statsDoc.min())

        timelinessDocument = timelinessDocument.map(
            lambda x: x[0:-1] + [str(x[-1])])

        found = False
        for k, v in dictHeaderPosition.iteritems():
            if v.startswith("TIMELINESS_"):
                found = True
                break
        if (not found):
            dictHeaderPosition[len(dictHeaderPosition)
                               ] = "TIMELINESS_" + str(dictHeaderPosition[i])
        timelinessInfoDict["timelinessNames"].append(
            "TIMELINESS_" + str(dictHeaderPosition[i]))
        timelinessInfoDict["timeFormats"].append(dataTypes[i])

        attrTimeFreq.append(timelinessMeanValue)
        attrTimeFreq.append(timelinessMaxValue)
        attrTimeFreq.append(timelinessMinValue)
        attrName.append("Timeliness_Mean_" + str(dictHeaderPosition[i]))
        attrName.append("Timeliness_Max_" + str(dictHeaderPosition[i]))
        attrName.append("Timeliness_Min_" + str(dictHeaderPosition[i]))

        attrTimeFreq.append(str(datetimeNow.strftime(stringFormat)))
        attrName.append("Last_Analysis_Timestamp_" +
                        str(dictHeaderPosition[i]))

        print(" ")

    timelinessInfoRdd = sc.parallelize(
        [(timelinessInfoDict["timelinessNames"],
            timelinessInfoDict["timeFormats"])])

    save_json(timelinessInfoRdd,
              ["timelinessNames",
               "timeFormats"],
              resultFolder + "/timelinessMetadata", "append")

    return timelinessDocument, attrName, attrTimeFreq


def distinctness(
        sc,
        sqlContext,
        document,
        columns,
        dataTypes,
        volatility,
        dictHeaderPosition,
        resultFolder,
        dimensionAllowedTypes):
    """
    This function calculates the dimension of Distinctness for each attribute, save the distinct values found 
    for each attribute and update the global distinctness quality of the source.

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            columns(list): Indexes of the attributes requested in the analysis
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: 
                Number of hours to consider the data still recent
            dictHeaderPosition(dict): Dictionary that associate the index of the attributes 
                to their corresponding name
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes 
                for each available quality dimension
    """
    print(" ")
    print("Distinctness")
    print(" ")

    attrDistinctness = []
    attrName = []

    finalPopulationRdd = sc.emptyRDD()
    finalDistinctnessRdd = sc.emptyRDD()

    totVolume = document.count()

    for i in columns:

        qualityDistinctness = 0
        print("-Attribute = " + str(dictHeaderPosition[i]))

        # read previous values of distinctness, and all the distinct values
        # available for each attribute
        try:
            previousCompletePopulation = sqlContext.read.json(
                resultFolder + "/source_distinct_values/" + str(dictHeaderPosition[i]))

            previousCount = previousCompletePopulation.count()
            previousVolume = sqlContext.read.json(
                resultFolder + "/source_distinctness").head().TotalRows

        except Exception:
            print("--There aren't all the distinct values files")
            qualityDistinctness = 1.0

        # find distinct values
        row = Row("Values")
        currentDF = document.map(lambda x: x[i]).distinct().map(row).toDF()
        currentCount = currentDF.count()
        print("--Number of distinct elements of the Attribute in the document = " + str(currentCount))

        if not qualityDistinctness:

            # get the new elements not contained in the previous file
            newElementsDF = currentDF.subtract(previousCompletePopulation)
            newCount = newElementsDF.count()
            print(
                "--Subtract from the global elements the elements found in the document")
            print("--NewCount = " + str(newCount))

            dCount = newCount + previousCount
            allCount = totVolume + previousVolume

            qualityDistinctness = dCount / float(allCount)

            finalDistinctnessRdd = (
                finalDistinctnessRdd.union(
                    sc.parallelize([(dictHeaderPosition[i], dCount, allCount, qualityDistinctness)])))
            distinctNumber = dCount

        else:
            qualityDistinctness = currentCount / float(totVolume)
            finalDistinctnessRdd = (
                finalDistinctnessRdd.union(
                    sc.parallelize([(dictHeaderPosition[i], currentCount, totVolume, qualityDistinctness)])))
            distinctNumber = currentCount

        attrDistinctness.append(qualityDistinctness)
        attrName.append("Distinctness_" + str(dictHeaderPosition[i]))

        attrDistinctness.append(distinctNumber)
        attrName.append("Distinct_Number_" + str(dictHeaderPosition[i]))

        # update the distinct_values with new elements
        try:
            if newCount > 0:
                save_json(newElementsDF, None, resultFolder + "/source_distinct_values/" +
                          str(dictHeaderPosition[i]), "append")
        except Exception:
            if currentCount > 0:
                save_json(currentDF, None, resultFolder +
                          "/source_distinct_values/" + str(dictHeaderPosition[i]))

    print("-Calculate value of Distinctness per Attribute's:")
    print("    --> Attribute, Distinct Count, Total Rows, DistinctnessValue")

    try:
        save_json(
            finalDistinctnessRdd,
            ["Attribute", "Count", "TotalRows", "DistinctnessValue"],
            resultFolder + "/source_distinctness",
            "overwrite")

    except Exception:
        save_json(
            finalDistinctnessRdd,
            ["Attribute", "Count", "TotalRows", "DistinctnessValue"],
            resultFolder + "/source_distinctness")

    return document, attrName, attrDistinctness


def consistency_zero_division(line):
    """
    This function returns a new line for each Row of the rdd with the evaluation of the Consistency dimension

    Args:
            line(Row): row of the rdd
    """
    try:
        return (line, float(line[1][1]) / line[1][0])
    except Exception:
        return (line, 0.0)


def multiple_row_filter(line, antecedent, consequent):
    """
    This function remap the rdd in order to match the requested rule

    Args:
            line(Row): row of the rdd
            antecedent(list): list of the indexes of the attributes that are part of the antecedent elements of the rule
            consequent(list): list of the indexes of the attributes that are part of the consequent elements of the rule
    """
    return (((','.join([str(line[i]) for i in antecedent])), (','.join([str(line[j]) for j in consequent]))), 1)


def consistency(
        sc,
        sqlContext,
        document,
        columns,
        dataTypes,
        volatility,
        dictHeaderPosition,
        resultFolder,
        dimensionAllowedTypes):
    """
    This function calculate the dimension of consistency for each existent rule, save the results 
    and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            columns(list): Indexes of the attributes requested in the analysis
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: 
                Number of hours to consider the data still recent
            dictHeaderPosition(dict): Dictionary that associate the index of the attributes 
                to their corresponding name
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes 
                for each available quality dimension
    """

    print(" ")
    print("Consistency")
    print(" ")

    attrConsistency = []
    attrName = []

    try:
        rules = sqlContext.read.json(resultFolder + "/association_rules")
        print("-List of Rules:")
        rules.show()

        # invert dictionary
        dictPositionHeader = dict([(v, k)
                                   for k, v in dictHeaderPosition.iteritems()])

        for row in rules.rdd.collect():

            print("-Checking rule: " + str(row[0]) + " -> " + str(row[1]))
            antecedents = [x.encode("UTF8", "ignore")
                           for x in row[0].split(",")]
            consequents = [x.encode("UTF8", "ignore")
                           for x in row[1].split(",")]
            antecedent = [dictPositionHeader[i] for i in antecedents]
            consequent = [dictPositionHeader[j] for j in consequents]

            consistentRdd = document.map(
                lambda x: multiple_row_filter(x, antecedent, consequent))
            consistentRdd = (
                consistentRdd.reduceByKey(lambda x, y: x + y)
                .map(lambda x: (x[0][0], x[1]))
                .combineByKey(lambda x: (x, x),
                              lambda x, value: (
                                  x[0] + value, max(x[1], value)),
                              lambda x, y: (x[0] + y[0], max(x[1], y[1])))
                .map(consistency_zero_division))

            # print("###### CONSISTENCY_TEST #########")
            # print(consistentRdd.take(32))

            consistentValue = consistentRdd.map(lambda x: x[1]).mean()

            attrConsistency.append(consistentValue)
            attrName.append("Consistency_" + str(row[0]) + "_" + str(row[1]))

    except Exception:
        print("-no available association rules")

    return document, attrName, attrConsistency


def check_association_rules(row, rules, headerPositionList):

    result = 0

    for i in range(len(rules)):
        ruleToCheck = rules[i]
        headerToCheck = headerPositionList[i]
        listToCheck = []
        for index in headerToCheck:
            listToCheck.append(row[index])
        if listToCheck in ruleToCheck:
            result += 1

    return result


def association_consistency(document, dictHeaderPosition, consistencyArgs):
    """
    This function calculate the dimension of consistency for each existent rule per tuple.

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            document(rdd): The rdd of the source.
            dictHeaderPosition(dict): Dictionary that associate the index of the attributes to their corresponding name
            consistencyArgs(list): list of strings with the path to the consistency rules files
    """
    print(" ")
    print("Association consistency")
    print(" ")

    consistencyRulesCount = 0.0
    invertedDictHeaderPosition = {v: k for k,
                                  v in dictHeaderPosition.iteritems()}
    consistencyRulesList = []
    headerPositionList = []

    for f in consistencyArgs:
        try:
            rulesDF = sqlContext.read.json(f)
            header = rulesDF.columns
            rulesRDD = rulesDF.rdd
            rulesRDD = rulesRDD.map(
                lambda x: [i.encode("UTF8", "ignore") for i in x])
            consistencyRulesList.append(rulesRDD.collect())
            headerPositionList.append(
                [int(invertedDictHeaderPosition[i]) for i in header])
            consistencyRulesCount += 1
        except Exception as e:
            print("Cannot open " + f + " file")

    #print("-Consistency rules found")
    # print(consistencyRulesList)

    if (consistencyRulesCount == 0):
        return document

    document = document.map(
        lambda x:
        x[0:] + [str(check_association_rules(x, consistencyRulesList, headerPositionList) / consistencyRulesCount)])

    # print("######## ASSOC_TEST ###########")
    # print(document.take(30))
    found = False
    for k, v in dictHeaderPosition.iteritems():
        if v.startswith("ASSOCIATION_CONSISTENCY"):
            found = True
            break
    if (not found):
        dictHeaderPosition[len(dictHeaderPosition)] = "ASSOCIATION_CONSISTENCY"

    return document


def main(sc, sqlContext, input_file, resultFolder, volatility=None, timeFormat=None, xmlPath=None):
    """
    $$$$$ Main Program $$$$$

    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            input_file(string): Absolute path of the location of the new portion of data to analyse
            resultFolder(string): Absolute path of the destination of all the saved files
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: 
                Number of hours to consider the data still recent
            timeFormat(list): List of strings containing the possible timestamp format of the attributes
            xmlPath(string): Absolute path of the location of the xml with the structure of the data
    """

    totStart = time.time()
    start = time.time()

    # initialize the final quality dictionary
    quality = {}
    dimensionAllowedTypes = {
        "Completeness_Missing": ["all"],
        "Completeness_Frequency": ["datetime"],
        "Accuracy": ["float"],
        "Completeness_Population": ["float", "string"],
        "Volume": ["float", "string"],
        "Precision": ["float"],
        "Consistency": ["all"],
        "Timeliness": ["datetime"],
        "Distinctness": ["float", "string"]}

    # try to get the preliminary information
    ready = False
    jparsing = False
    except_found = False

    try:
        print("input_file")
        print(input_file)
        print("resultFolder")
        print(resultFolder)

        preliminaryInformation = sqlContext.read.json(
            resultFolder + "/preliminary_information").head()

        print("Basic Information Retrieval...")

        header = preliminaryInformation.header
        header = [x.encode("UTF8", "ignore") for x in header]
        print("-Header Getted..")
        print(header)

        xmlRegex = preliminaryInformation.regex.encode("UTF8", "ignore")
        print(xmlRegex)
        if xmlRegex == "":
            jparsing = True

        try:
            volatility = float(preliminaryInformation.volatility)
            print(volatility)
        except:
            volatility = 17520.0

        dataTypes = preliminaryInformation.datatypes
        dataTypes = [x.encode("UTF8", "ignore") for x in dataTypes]
        print("-Data Types Getted..")
        print(dataTypes)

        dimensionColumn = preliminaryInformation.dimensionAttributePosition.asDict()
        print(dimensionColumn)

        dictHeaderNames = preliminaryInformation.dimensionAttributeName.asDict()
        dictHeaderNames = dict([(k, [j.encode("UTF8", "ignore") for j in v])
                                for k, v in dictHeaderNames.iteritems()])
        print("-Dimension applyable with column's names Getted..")
        print(dictHeaderNames)

        dictHeaderPosition = preliminaryInformation.attributePosition.asDict()
        dictHeaderPosition = dict(
            [(int(k), v.encode("UTF8", "ignore")) for k, v in dictHeaderPosition.iteritems()])

        ready = True

        if jparsing:
            documentDF = sqlContext.read.json(input_file)
            notNullDocumentDF = documentDF.dropna()
            # notNullDocumentDF.show()
            document = documentDF.fillna("null").rdd.map(
                lambda x: [i.encode("UTF8", "ignore") for i in x])

    except Exception as e:
        print(e)

        # Build preliminary information
        # XML parsing

        try:
            print(input_file)
            documentDF = sqlContext.read.json(input_file)
            print("Document Read as list of Json objects..")

            header = documentDF.columns
            print("-Header..")
            print(header)

            dictHeaderPosition = dict(enumerate(header))

            xmlRegex = ""

            # convert to Rdd
            notNullDocumentDF = documentDF.dropna()
            document = documentDF.fillna("null").rdd.map(
                lambda x: [i.encode("UTF8", "ignore") for i in x])
            jparsing = True
            except_found = True

        except Exception:

            print(
                "Impossible to read the document as list of Json objects, need an xml with the schema..")

            document = sc.textFile(input_file)

            # load the xmlSchema
            xmlSchema = sc.textFile(xmlPath)

            print("-XML Found")
            print(xmlSchema.take(6))

            # conversion
            xmlSchema = xmlSchema.map(lambda x: x.encode("UTF8", "ignore"))

            # quote removal
            xmlSchema = xmlSchema.map(quote_removal)

            # extract the header of the columns
            header = xmlSchema.map(extract_header).filter(
                lambda x: x != None).collect()
            print("-Header")
            print(header)

            # if there is no header the first line is used as header and then
            # it is removed from rdd
            if not header:
                print("-Considering the first line as header")
                firstLine = document.first()
                header = firstLine.split(",")
                document = document.filter(lambda line: line != firstLine)

            # extract the format of timestamps if available
            timeFormat = xmlSchema.map(extract_timestamp_format).filter(
                lambda x: x != None).collect()
            print("-Timeformats")
            print(timeFormat)

            dictHeaderPosition = dict(enumerate(header))
            print("-Header with index of rdd's elements")
            print(dictHeaderPosition)

            # apply function to extract main elements
            xmlSchema = xmlSchema.map(
                extract_main_elements_xml).filter(lambda x: x != None)

            # apply function to derive regular expression
            xmlRegex = regular_parsing_xml(xmlSchema)
            print(
                "-Regular Parsing Expression [element_separator].*[element_separator]")
            print(xmlRegex)

    # DATA structuration

    if not jparsing:

        print(" ")
        print("Conversion..")
        # conversion
        document = document.map(lambda x: x.encode("UTF8", "ignore"))

        # quote removal
        document = document.map(quote_removal)

        # remove null line
        document = document.filter(lambda line: len(line) > 0)

        # remove escapes from timestamps
        document = document.map(escape_removal)

        document = document.map(comma_to_dot_number_conversion)

        # parse the document to extract the list of elements
        document = document.map(lambda x: regular_parsing(x, xmlRegex))
        print("Extract All the elements following the regular expression..")
        # print(document.take(5))
        notNullDocumentDF = document.toDF(header).dropna()

    if not ready:

        # continue preliminary information retrieval
        # DIMENSION analysis

        print("-Derive dictionary of dimensions and allowed applyable columns")
        dimensionColumn, dataTypes = available_dimensions(
            document, dimensionAllowedTypes.copy(), timeFormat)

        print("-Dimensions with column's indexes to consider")
        print(dimensionColumn)
        print("-Datatypes, if there are timestamp, their format is returned in the correspondent column position")
        print(dataTypes)

        print("-Associate dictionary of dimensions with the allowed names of applyable columns")

        dictHeaderNames = dimensionAllowedTypes.copy()
        for k, v in dimensionColumn.iteritems():
            dictHeaderNames[k] = [dictHeaderPosition[i] for i in v]

        print("-Dimensions with column's names to consider")
        print(dictHeaderNames)

        # Save Source Preliminary Information

        print("-Save all derived information in the file output_information")
        print("   that is common for each data file in the data source folder")
        header.append("COMPLETENESS_MISSING")
        dictHeaderPosition[len(dictHeaderPosition)] = "COMPLETENESS_MISSING"

    end = time.time()
    print("Preliminary Information End, Elapsed Time: " +
          str(end - start) + " seconds")
    print(" ")

    # DATA ANALYSIS

    timeRdd = sc.parallelize([("Preliminary Information", (end - start))])

    dimensionName = {'Precision': precision, 'Completeness_Missing': completeness_missing,
                     'Distinctness': distinctness, 'Consistency': consistency, 'Timeliness': timeliness}

    finalQuality = sqlContext.createDataFrame([{'_test_': 'test'}])

    print("Starting Analysis")
    try:
        start = time.time()
        document, qualityName, partialQuality = (
            completeness_missing(
                sc,
                sqlContext,
                document,
                dimensionColumn["Completeness_Missing"],
                dataTypes,
                volatility,
                dictHeaderPosition,
                resultFolder,
                dimensionColumn))

        end = time.time()

        print("-Completeness_Missing Elapsed Time: " +
              str(end - start) + " seconds")
        timeRdd = timeRdd.union(sc.parallelize(
            [("Completeness_Missing", (end - start))]))

        if partialQuality is not None:
            for j in range(len(qualityName)):
                quality[qualityName[j]] = partialQuality[j]
                finalQuality = finalQuality.withColumn(
                    qualityName[j], lit(partialQuality[j]))

    except Exception:
        import traceback
        print("-Error in dimension Completeness_Missing")
        print("   solve the problem, delete the partial results and run the analysis again")
        traceback.print_exc()
        return 0

    print(document.take(5))

    for i in dimensionColumn.iterkeys():
        if i in ["Accuracy", "Volume", "Completeness_Population", "Completeness_Frequency", "Completeness_Missing"]:
            continue

        try:

            func = dimensionName[i]
            start = time.time()

            document, qualityName, partialQuality = func(sc, sqlContext, document, dimensionColumn[
                                                         i], dataTypes, volatility, dictHeaderPosition, resultFolder, dimensionColumn)

            if partialQuality is None:
                continue

            end = time.time()
            print("-" + str(i) + " Elapsed Time: " +
                  str(end - start) + " seconds")
            timeRdd = timeRdd.union(sc.parallelize([(str(i), (end - start))]))

            for j in range(len(qualityName)):
                quality[qualityName[j]] = partialQuality[j]
                finalQuality = finalQuality.withColumn(
                    qualityName[j], lit(partialQuality[j]))

        except Exception:
            import traceback
            print("-Error in dimension " + str(i))
            print(
                "   solve the problem, delete the partial results and run the analysis again")
            traceback.print_exc()
            return 0

    document = association_consistency(
        document, dictHeaderPosition, consistencyArgs)
    finalQuality = finalQuality.drop("_test_")
    print(" ")
    print("Final Quality")
    finalQuality.show(truncate=False)

    # Data Quality Update: Source

    print(" ")
    print("Update Source")
    start = time.time()

    newSourceQuality = sqlContext.createDataFrame([{'_test_': "test"}])

    # Current Count of Rows
    volumeCount = document.count()

    # invert dictionary
    dictPositionHeader = dict([(v, k)
                               for k, v in dictHeaderPosition.iteritems()])

    try:
        previousDistinctness = sqlContext.read.json(
            resultFolder + "/source_distinctness")
        sourceQuality = sqlContext.read.json(resultFolder + "/source_quality")
        print("-Previous Source Quality")
        # sourceQuality.show(5)
        sourceQuality = sourceQuality.head()

        try:
            # New Analysis Time
            datetimeNew = datetime.datetime.now()

            # Number of document in the source
            count = sourceQuality["UpdatesCount"]
            count = count + 1
            newSourceQuality = newSourceQuality.withColumn(
                "UpdatesCount", lit(count))

            # Volume of document in the source
            oldCount = sourceQuality["Volume(TotalRows)"]
            totalCount = volumeCount + oldCount
            newSourceQuality = newSourceQuality.withColumn(
                "Volume(TotalRows)", lit(totalCount))

            # Other Dimensions
            allTimeDifferences = {}

            for dim in quality.iterkeys():
                try:
                    qualityValue = float(quality[dim])
                except Exception:
                    print(str(dim) + " is not float")

                if "Timeliness" in str(dim):
                    attributeString = re.sub(
                        r"Timeliness_(.*?)_", "", str(dim))
                    try:
                        timeDifference = allTimeDifferences[attributeString]
                    except Exception:
                        currentTimeFormat = dataTypes[
                            dictPositionHeader[attributeString]]
                        timestampOld = sourceQuality[
                            "Last_Analysis_Timestamp_" + str(attributeString)]
                        dateDifference = datetimeNew - \
                            datetime.datetime.strptime(
                                timestampOld, currentTimeFormat)
                        timeDifference = (
                            (float(dateDifference.microseconds +
                                   (dateDifference.seconds + dateDifference.days * 24 * 3600) * 10**6) / 10**6) /
                            float(volatility * 3600))
                        allTimeDifferences[attributeString] = timeDifference

                    oldTimeliness = float(sourceQuality[dim])
                    sourceTimeliness = oldTimeliness - timeDifference

                    if "Min" in str(dim):
                        dimensionValue = min(qualityValue, sourceTimeliness)
                    else:
                        if "Max" in str(dim):
                            dimensionValue = max(
                                qualityValue, sourceTimeliness)
                        else:
                            dimensionValue = (sourceTimeliness * oldCount + qualityValue *
                                              volumeCount) / float(totalCount)

                else:
                    if "Minimum_Timestamp" in str(dim):
                        attributeString = re.sub(
                            r"Minimum_(.*?)_", "", str(dim))
                        currentTimeFormat = dataTypes[
                            dictPositionHeader[attributeString]]

                        minimumTimestamp = sourceQuality[
                            "Minimum_Timestamp_" + str(attributeString)]

                        sourceMinimumTimestamp = datetime.datetime.strptime(
                            minimumTimestamp, currentTimeFormat)
                        currentMinimumTimestamp = datetime.datetime.strptime(
                            quality[dim], currentTimeFormat)

                        dimensionValue = min(sourceMinimumTimestamp,
                                             currentMinimumTimestamp).strftime(currentTimeFormat)

                    else:
                        if "Maximum_Timestamp" in str(dim):
                            attributeString = re.sub(
                                r"Maximum_(.*?)_", "", str(dim))
                            currentTimeFormat = dataTypes[
                                dictPositionHeader[attributeString]]

                            maximumTimestamp = sourceQuality[
                                "Maximum_Timestamp_" + str(attributeString)]
                            sourceMaximumTimestamp = datetime.datetime.strptime(
                                maximumTimestamp, currentTimeFormat)
                            currentMaximumTimestamp = datetime.datetime.strptime(
                                quality[dim], currentTimeFormat)

                            dimensionValue = max(sourceMaximumTimestamp,
                                                 currentMaximumTimestamp).strftime(currentTimeFormat)

                        else:
                            if "Distinctness" in str(dim):
                                attributeString = re.sub(
                                    r"Distinctness_", "", str(dim))
                                dimensionValue = previousDistinctness.filter(
                                    previousDistinctness.Attribute == str(attributeString)).head()["DistinctnessValue"]
                            else:
                                if "Distinct_Number" in str(dim):
                                    dimensionValue = qualityValue
                                else:
                                    if "Last_Analysis_Timestamp" in str(dim):
                                        attributeString = str(dim).replace(
                                            "Last_Analysis_Timestamp_", "")
                                        currentTimeFormat = dataTypes[
                                            dictPositionHeader[attributeString]]
                                        dimensionValue = datetimeNew.strftime(
                                            currentTimeFormat)
                                    else:
                                        if "Min" in str(dim):
                                            dimensionValue = min(
                                                qualityValue, sourceQuality[str(dim)])
                                        else:
                                            if "Max" in str(dim):
                                                dimensionValue = max(
                                                    qualityValue, sourceQuality[str(dim)])
                                            else:
                                                dimensionValue = (
                                                    (sourceQuality[str(dim)] * oldCount + qualityValue * volumeCount) /
                                                    float(totalCount))

                newSourceQuality = newSourceQuality.withColumn(
                    str(dim), lit(dimensionValue))

            print("-New Source Quality")
            newSourceQuality = newSourceQuality.drop("_test_")
            newSourceQuality.show(5)
            save_json(newSourceQuality, None, resultFolder +
                      "/source_quality", "overwrite")
        except Exception:
            import traceback
            print("Error in Source Update")
            traceback.print_exc()
            return 0

    except Exception:

        # Number of document in the source
        newSourceQuality = newSourceQuality.withColumn("UpdatesCount", lit(1))

        # Volume of document in the source
        newSourceQuality = newSourceQuality.withColumn(
            "Volume(TotalRows)", lit(volumeCount))

        # Other Dimensions
        for dim in quality.iterkeys():
            newSourceQuality = newSourceQuality.withColumn(
                str(dim), lit(quality[dim]))

        print("New Source Quality")
        newSourceQuality = newSourceQuality.drop("_test_")
        newSourceQuality.show(5)

        save_json(newSourceQuality, None, resultFolder + "/source_quality")

    # End Update Source
    end = time.time()
    print("Update Source Elapsed Time: " + str(end - start) + " seconds")
    timeRdd = timeRdd.union(sc.parallelize([("Update_Source", (end - start))]))

    def toCSVLine(data):
        return ','.join(str(d) for d in data)

    r = re.compile("(.*?).txt")
    name = [filter(r.match, x.split("/"))[0].replace(".txt", "")
            for x in input_file]
    if(len(name) > 1):
        name = [name[0]] + [name[-1]]
        name = '_'.join(name)
    else:
        name = name[0]

    lines = timeRdd.map(toCSVLine)
    save_txt(lines, resultFolder + "/Times/" + name)

    totEnd = time.time()
    print("Total Elapsed Time: " + str(totEnd - totStart) + " seconds")

    # adding to headers computed dimensions
    headersCount = len(header)
    for k, v in dictHeaderPosition.iteritems():
        if int(k) == headersCount:
            header.append(v)
            headersCount += 1

    infoRdd = sc.parallelize([(header, volatility, dimensionAllowedTypes, dataTypes,
                               dimensionColumn, dictHeaderNames, dictHeaderPosition, xmlRegex, consistencyArgs)])

    save_json(
        infoRdd,
        ["header",
         "volatility",
         "dimensionAllowedTypes",
         "datatypes",
         "dimensionAttributePosition",
         "dimensionAttributeName",
         "attributePosition",
         "regex",
         "consistencyRuleFiles"],
        resultFolder + "/preliminary_information")

    save_json(document, list(dictHeaderPosition.values()),
              resultFolder + "/extended_dataset/" + name)

    return 1


def build_input_list(pattern, start, end, step):
    """
    This function builds a list of input file generating all the timestamps within an interval according to a pattern and a step.

    pattern(string): a timestamp format (e.g. 'doc%Y%m%d.txt')
    start(string): first timestamp
    end(string): last timestamp of the interval
    step(int): interval step in days
    """
    try:
        start = datetime.datetime.strptime(start, pattern)
        end = datetime.datetime.strptime(end, pattern)
        step = datetime.timedelta(days=step)
    except Exception:
        print("Wrong time pattern")
        return []

    if(end < start):
        return []
    input_list = []
    while(start <= end):
        input_list.append(start.strftime(pattern))
        start += step
    return input_list


"""
Entry Point
"""
if __name__ == "__main__":

    args = sys.argv[1:]

    input_file = args[0]
    multiple_file = input_file.startswith("--pattern=")
    if(multiple_file):
        start_end_interval = args[-1].split(";")
        input_file = build_input_list(
            input_file[10:],
            start_end_interval[0],
            start_end_interval[1],
            int(start_end_interval[2]))
    else:
        input_file = [input_file]
    if(len(input_file) == 0):
        print("No input file provided, or wrong pattern")
        sys.exit(0)
    resultFolder = args[1]

    # save the name of the file containing the portion of data analysed
    r = re.compile("(.*?).txt")
    name = [filter(r.match, x.split("/"))[0].replace(".txt", "")
            for x in input_file]
    if(len(name) > 1):
        name = [name[0]] + [name[-1]]
        name = '_'.join(name)
    else:
        name = name[0]

    print(name)
    print(input_file)
    conf = SparkConf().setAppName("DQProfiling_v1.3_" + name)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext.getOrCreate(sc)

    try:
        volatility = args[2]
    except Exception:
        volatility = None

    try:
        timeFormat = args[3].split(";")
    except Exception:
        timeFormat = None

    try:
        consistencyArgs = args[4].split(";")
        print("consistency rules args:")
        print(consistencyArgs)
        skipConsistency = False
    except Exception:
        consistencyArgs = None
        skipConsistency = True

    try:
        xmlPath = args[5]
    except Exception:
        xmlPath = None

    alreadyCorrectAnalysed = False
    # if the data have already been analysed the algorithm is ended
    try:
        analysedNames = sqlContext.read.json(
            resultFolder + "/source_analysed_updates")
        analysedName = analysedNames.filter(analysedNames.fileName == name).rdd.map(
            lambda x: (x.correctAnalysis)).collect()
        if (1 in analysedName):
            alreadyCorrectAnalysed = True
    except Exception:
        print("First file of the source")

    if not alreadyCorrectAnalysed:
        try:
            noError = main(sc, sqlContext, input_file,
                           resultFolder, volatility, timeFormat, xmlPath)
        except Exception:
            import traceback
            print("Data empty, input files missing or HDFS not available")
            traceback.print_exc()
            noError = 0

        try:
            save_json(
                sqlContext.createDataFrame(
                    [{"fileName": name,
                      "correctAnalysis": noError,
                      "analysisTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]),
                None,
                resultFolder + "/source_analysed_updates",
                "append")
        except Exception:
            save_json(
                sqlContext.createDataFrame(
                    [{"fileName": name,
                      "correctAnalysis": noError,
                      "analysisTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]),
                None,
                resultFolder + "/source_analysed_updates")

    else:
        print("Already Correctly Analysed")
        noError = 1

    sc.stop()
    sys.exit(not noError)
