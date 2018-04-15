"""
IMPORT STEP
"""

import re
import datetime
import time
import traceback
import sys
import json
from math import ceil
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import lit, col, udf


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


def save_json(document, schema, path, saveMode='overwrite'):
    """
    Save an Rdd or a dataframe into a .json file with the saveMode specified

    Args:
        document(rdd): The document to save.
        schema(list): The schema of the columns of the rdd, used to convert to Dataframe
        path(string): The path in which the document have to be saved
        saveMode(string): A string representing the save mode, like 'overwrite' and 'append'

    """
    if saveMode is None:
        try:
            print("Saving Json...")
            if schema is None:
                document.coalesce(1).write.json(path)
            else:
                document.toDF(schema).coalesce(1).write.json(path)
        except Exception as e:
            print("The file already exists")
            print(e)
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
    Main function to extract the regular expression from the xml to be used to derive the elements for the analysis - It must be upgraded

    Args:
        document(line): The line of the rdd to parse.
    """
    prev = ""
    next = False

    prec = ""
    post = ""
    # When each element is found, the antecedent and consequent separators are
    # saved in strings
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
    # Construct the final regular expression
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
    Function to remove the escape from the elements of the file, the function should be upgraded to eliminate all the undesired symbols

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


"""
DEFINITION STEP - DIMENSION ANALYSIS
"""


def restriction_filter(document, newIndexes, j=None):
    """
    The function performs the union of all the values of the different attributes into a single string, returning the final string

    Args:
        document(line): The line of the rdd to parse.
            newIndexes(list): List of index of the elements to union in a single string
            j(int): Index of the dependent element that should be returned as a value of the key-value pair
    """
    for h in range(1, len(newIndexes)):
        document[newIndexes[0]] = str(
            document[newIndexes[0]]) + "," + str(document[newIndexes[h]])

    if j is not None:
        return (document[newIndexes[0]], document[j])

    return (document[newIndexes[0]])


def accuracy(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the dimension of accuracy for each numerical attribute, save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """

    print(" ")
    print("Accuracy")
    print(" ")

    finalAccuracyRdd = sc.emptyRDD()
    attrName = []
    attrAccuracy = []
    confidence = []
    counter = 0

    # get the desired degrees of granularity
    try:
        granularity = granularityDimensions["Accuracy"].split(",")
        globalAcc = True if "global" in granularity else False
        attributeAcc = True if "attribute" in granularity else False
        valueAcc = True if "value" in granularity else False

        if granularity[0] == "":
            globalAcc = True
    except Exception:
        globalAcc = True
        attributeAcc = False
        valueAcc = False

    for j in columns:

        print("-Numerical Attribute = " + str(desiredColumns[j]))
        globalIncluded = 0

        meanAccuracy = meanAccuracyValues[counter]
        devAccuracy = devAccuracyValues[counter]
        counter = counter + 1

        if attributeAcc or valueAcc:

            for stringIndex in columnsKey:

                # key columns = keyColumns
                stringSplitIndex = stringIndex.split(",")
                newIndexes = [desiredColumns.index(
                    k) for k in stringSplitIndex]
                newDocument = document.map(
                    lambda x: restriction_filter(x, newIndexes, j))
                stringAttribute = '_'.join(stringSplitIndex)

                try:
                    # it is useless to group by the timestamps
                    if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                        continue

                except Exception:
                    pass

                if j in newIndexes:
                    continue

                print("--Key Attribute = " + stringAttribute)

                # calculate the distance between each value and the expected
                # mean, and calculate each accuracy value as this distance
                # divided by the maximum allowed interval
                staticAccuracyRdd = (newDocument.map(lambda x: (x[0], max(0.0, 1 - abs((float(x[1].replace(',', '.')) - float(meanAccuracy.replace(',', '.'))) / (float(devAccuracy.replace(',', '.')) / 2)))))
                                     .map(lambda x: (x[0], (1, x[1], ceil(x[1]))))
                                     .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
                                     )
                if globalAcc:
                    globalIncluded = 1
                    globalAccuracies = (staticAccuracyRdd.map(lambda x: (x[1][0], x[1][1], x[1][2]))
                                        .reduce(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
                                        )
                    attributeStaticAccuracy = globalAccuracies[
                        2] / float(globalAccuracies[0])
                    attributeAccuracy = globalAccuracies[
                        1] / float(globalAccuracies[0])

                accuracyRdd = staticAccuracyRdd.map(lambda x: (
                    x[0], x[1][1] / float(x[1][0]), x[1][2] / float(x[1][0])))

                print("Calculate for each record the accuracy value as 1 - (( Value - desired Mean )/(Maximum interval / 2)) and find the mean results per Attribute's Value: -> ( Attribute's Value, Mean Accuracy )")
                # print(accuracyRdd.take(5))

                if valueAcc:
                    save_json(accuracyRdd, ["Value", "AccuracyDynamic", "AccuracyStatic"], resultFolder +
                              "/accuracy_values/attribute_" + stringAttribute + "_REF_" + str(desiredColumns[j]))

                if attributeAcc:
                    finalAccuracyRdd = finalAccuracyRdd.union(sc.parallelize([(stringAttribute, accuracyRdd.map(
                        lambda x: x[1]).mean(), accuracyRdd.map(lambda x: x[2]).mean(), performanceSample)]))

            if attributeAcc:
                # save file into hdfs
                save_json(finalAccuracyRdd, ["Attribute", "AccuracyDynamic", "AccuracyStatic",
                                             "Confidence"], resultFolder + "/accuracy_attributes_" + str(desiredColumns[j]))

        # append the global value to the list
        if globalAcc:
            if not globalIncluded:
                # calculate final accuracies
                globalAccuracies = (document.map(lambda x: (max(0.0, 1 - abs((float(x[j].replace(',', '.')) - float(meanAccuracy.replace(',', '.'))) / (float(devAccuracy.replace(',', '.')) / 2)))))
                                    .map(lambda x: (1, x, ceil(x)))
                                    .reduce(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
                                    )

                attributeStaticAccuracy = globalAccuracies[
                    2] / float(globalAccuracies[0])
                attributeAccuracy = globalAccuracies[
                    1] / float(globalAccuracies[0])

            attrAccuracy.append(attributeStaticAccuracy)
            attrName.append("Accuracy_Static_" + str(desiredColumns[j]))
            confidence.append(performanceSample)

            attrAccuracy.append(attributeAccuracy)
            attrName.append("Accuracy_Dynamic_" + str(desiredColumns[j]))
            confidence.append(performanceSample)

            print("Global Static Accuracy " + str(attributeStaticAccuracy))
            print("Global Dynamic Accuracy " + str(attributeAccuracy))

    return attrName, attrAccuracy, confidence


def calculateSampleDeviation(line):
    # lambda (key,(sumDev,count,sumMean)):
    # (key,((sumDev/(count-1))**0.5,sumMean/count,count))
    try:
        newLine = (line[0], ((line[1][0] / (line[1][1] - 1))**0.5,
                             line[1][2] / float(line[1][1]), float(line[1][1])))
    except Exception:
        newLine = (line[0], (0, line[1][2] /
                             float(line[1][1]), float(line[1][1])))

    return newLine


def precision(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the dimension of precision for each numerical attribute, save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """

    print(" ")
    print("Precision")
    print(" ")

    finalPrecisionRdd = sc.emptyRDD()
    attrPrecision = []
    attrName = []
    confidence = []

    # get the desired degrees of granularity
    try:
        granularity = granularityDimensions["Precision"].split(",")
        globalPrec = True if "global" in granularity else False
        attributePrec = True if "attribute" in granularity else False
        valuePrec = True if "value" in granularity else False

        if granularity[0] == "":
            globalPrec = True
    except Exception:
        globalPrec = True
        attributePrec = False
        valuePrec = False

    for j in columns:
        print("-Numerical Attribute = " + str(desiredColumns[j]))

        reShift = False
        statsDoc = document.map(lambda x: (
            float(x[j].replace(',', '.')))).stats()
        devAttribute = float(statsDoc.stdev())
        meanAttribute = float(statsDoc.mean())
        minValue = float(statsDoc.min())
        maxValue = float(statsDoc.max())

        if (minValue < 0) & (maxValue > 0):
            reShift = True

        if attributePrec or valuePrec:

            for stringIndex in columnsKey:

                # key columns = keyColumns
                stringSplitIndex = stringIndex.split(",")
                newIndexes = [desiredColumns.index(
                    k) for k in stringSplitIndex]
                newDocument = document.map(
                    lambda x: restriction_filter(x, newIndexes, j))
                stringAttribute = '_'.join(stringSplitIndex)

                try:
                    # it is useless to group by the timestamps
                    if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                        continue

                except Exception:
                    pass

                if j in newIndexes:
                    continue

                print("--Key Attribute = " + stringAttribute)

                keyFloatDocument = newDocument.map(
                    lambda x: (x[0], float(x[1].replace(',', '.'))))

                if reShift:
                    print("---Shifting all the values to make them greater than 1 ")
                    keyFloatDocument = keyFloatDocument.map(
                        lambda x: (x[0], x[1] + abs(minValue)))

                meanRdd = (keyFloatDocument.map(lambda x: (x[0], (x[1], 1)))
                           .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
                           .map(lambda x: (x[0], x[1][0] / float(x[1][1])))
                           )

                varRdd = (keyFloatDocument.join(meanRdd)
                          .map(lambda (key, (value, mean)): (key, ((value - mean)**2, 1, mean)))
                          .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
                          .map(lambda (key, (sumDev, counter, sumMean)): (key, ((sumDev / counter)**0.5, sumMean / counter)))
                          )

                precisionRdd = varRdd.map(lambda (key, (dev, mean)): (key, mean, max(0.0, 1.0 - dev / abs(mean)), dev))
                print(
                    "---Calculate value of precision per Attribute's value: --> Value, Mean, Precision, Standard Deviation")
                # print(precisionRdd.take(5))

                if reShift:
                    print(
                        "---Shifting all the values again to replace the correct mean")
                    precisionRdd = precisionRdd.map(lambda (key, mean, prec, dev): (key, mean - abs(minValue), prec, dev))

                if valuePrec:
                    save_json(precisionRdd, ["Value", "Mean", "Precision", "StandardDeviation"], resultFolder +
                              "/precision_values/attribute_" + stringAttribute + "_number_" + str(desiredColumns[j]))

                finalPrecisionRdd = finalPrecisionRdd.union(sc.parallelize([(stringAttribute, precisionRdd.map(lambda x: x[1]).mean(
                ), precisionRdd.map(lambda x: x[2]).mean(), precisionRdd.map(lambda x: x[3]).mean(), performanceSample)]))

                print(" ")

        # calculate final aggregated value for precision

        if globalPrec:

            if reShift:
                attributePrecision = max(
                    0.0, 1.0 - devAttribute / (abs(meanAttribute) + abs(minValue)))
            else:
                attributePrecision = max(
                    0.0, 1.0 - devAttribute / abs(meanAttribute))

            attrPrecision.append(attributePrecision)
            attrName.append("Precision_" + str(desiredColumns[j]))
            confidence.append(performanceSample)
            attrPrecision.append(devAttribute)
            attrName.append("Precision(Deviation)_" + str(desiredColumns[j]))
            confidence.append(performanceSample)
            print("Global Precision " + str(attributePrecision))
        if attributePrec:
            print("--Final Aggregated File --> Attribute, Mean, Precision, Deviation")
            # print(finalPrecisionRdd.take(5))

            # save file into hdfs
            save_json(finalPrecisionRdd, ["Attribute", "Mean", "Precision", "Standard_Deviation",
                                          "Confidence"], resultFolder + "/precision_attributes_" + str(desiredColumns[j]))

    return attrName, attrPrecision, confidence


def mapping_completeness_missing(x, newIndexes):
    """
    This function remove empty,None or null value from each line, and return the key, the current line length the previous line length as a new Rdd line

    Args:
            x(Row): line of the Rdd
            newIndexes(list): list of indexes to union in a single element
    """
    for h in range(1, len(newIndexes)):
        x[newIndexes[0]] = str(x[newIndexes[0]]) + "," + str(x[newIndexes[h]])
        del x[newIndexes[h]]

    lineLength = len(x)
    previousline = x
    try:
        line = [el for el in previousline if el is not None]
        previousline = line
    except Exception:
        line = previousline

    previousline = line

    try:
        line = filter(lambda a: a != "", line)
        previousline = line
    except Exception:
        line = previousline

    previousline = line

    try:
        line = filter(lambda a: a != "nan", line)
        previousline = line
    except Exception:
        line = previousline

    previousline = line

    try:
        line = filter(lambda a: a != "null", line)
        previousline = line
    except Exception:
        line = previousline

    return (x[newIndexes[0]], (len(line) - 1, lineLength - 1))


def completeness_missing(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the value of the Part of Completeness regarding the missing elements per line,save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """

    print(" ")
    print("Completeness_Missing")
    print(" ")

    lineLength = len(document.take(1)[0])

    finalCompleteRdd = sc.emptyRDD()

    # get the desired degrees of granularity
    try:
        granularity = granularityDimensions["Completeness_Missing"].split(",")
        globalMiss = True if "global" in granularity else False
        attributeMiss = True if "attribute" in granularity else False
        valueMiss = True if "value" in granularity else False

        if granularity[0] == "":
            globalMiss = True
    except Exception:
        globalMiss = True
        attributeMiss = False
        valueMiss = False

    if attributeMiss or valueMiss:

        for stringIndex in columnsKey:

            # key columns = keyColumns
            stringSplitIndex = stringIndex.split(",")
            newIndexes = [desiredColumns.index(k) for k in stringSplitIndex]
            newDocument = document.map(
                lambda x: restriction_filter(x, newIndexes))
            stringAttribute = '_'.join(stringSplitIndex)

            print("--Key Attribute = " + stringAttribute)

            itIsTime = False

            if valueMiss:
                try:
                    # it is useless to group by the timestamps
                    if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                        itIsTime = True

                except Exception:
                    pass

                if not itIsTime:
                    # analysis per value
                    print("--Key Attribute's Values Analysis")

                    keyValueDocument = document.map(
                        lambda x: mapping_completeness_missing(x, newIndexes))
                    print("---Find number of filtered and total elements per record, for each Key Attribute's Value: -> ( Key Attribute , ( Filtered Line Lenght , Full Line Lenght ) )")
                    # print(keyValueDocument.take(5))

                    # Add a filter to remove the null,none or empty keys
                    print("---Filter Null keys")
                    keyValueDocument = keyValueDocument.filter(lambda x: x[0] != "null").filter(lambda x: x[0] is not None).filter(
                        lambda x: x[0] != "").filter(lambda x: x[0] != "nan").reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

                    keyValueDocument = keyValueDocument.map(lambda x: (
                        x[0], x[1][1] - x[1][0], x[1][0] / float(x[1][1])))
                    print(
                        "---Calculate Completeness Missing for each Attribute's Value: -> ( Value, Missing Values, Completeness Missing Value )")
                    # print(keyValueDocument.take(5))

                    save_json(keyValueDocument, ["Value", "MissingValues", "CompletenessMissingValue"],
                              resultFolder + "/completeness_missing_values/" + stringAttribute)

            if attributeMiss:
                # Analysis per attribute
                print("--Attribute's Analysis")

                # attribute elements
                attributeDocument = newDocument

                totElements = attributeDocument.count()
                print("---Total Elements")
                print(totElements)

                filteredElements = attributeDocument.filter(lambda x: x != "null").filter(
                    lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x != "nan").count()
                print("---Total Filtered Elements")
                print(filteredElements)

                completenessAttribute = filteredElements / float(totElements)
                print(completenessAttribute)

                # Save both values
                finalCompleteRdd = finalCompleteRdd.union(sc.parallelize(
                    [(stringAttribute, totElements - filteredElements, completenessAttribute, performanceSample)]))

    if attributeMiss:
        print("--Calculate value of Completeness Missing per Attribute: --> Attribute, Missing Values, Completeness Missing Value, Confidence")
        # print(finalCompleteRdd.take(5))

        # save file into hdfs
        save_json(finalCompleteRdd, ["Attribute", "MissingValues", "CompletenessMissingValue",
                                     "Confidence"], resultFolder + "/completeness_missing_attributes")

    if globalMiss:
        # Global Analysis
        print("-Global Missing Analysis")

        globalDocument = document.flatMap(lambda x: x)
        globalCount = globalDocument.count()
        filteredCount = globalDocument.filter(lambda x: x != "null").filter(
            lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x != "nan").count()

        qualityCompletenessMissing = filteredCount / float(globalCount)

        print("--Final Global Completeness Missing: " +
              str(qualityCompletenessMissing))
    else:
        qualityMissing = None

    return ["Completeness_Missing"], [qualityCompletenessMissing], [performanceSample]


def mapping_completeness_frequency(x, volatiliyTime, performanceSample):
    """
    Mapping Function for completeness_frequency, it returns the new line containing the found value of the Completeness_Frequency dimension, or a fake line that will be eliminated if there is only 1 record for the considered grouping value

    Args:
            x(line): line of the rdd considered
            volatilityTime(float or string): Number of hours to consider the data still recent, used to derive quickly the amount of passed time
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """
    try:
        newLine = (x[0], x[3], min(1.0, float(x[3]) /
                                   (volatiliyTime * (x[4] - x[5]) * performanceSample * x[1])))
    except Exception:
        # this exception is raised whenever there is only 1 element available
        # for the current key element ( max = min ), or even if the frequency
        # or the volatiliy is 0, so it will be marked to be removed. (if
        # volatiliy is exceeded there is an error)
        newLine = (x[0], x[3], 2)

    return newLine


def completeness_frequency(sc, sqlContext, keyValueCount, volatiliyTime, column, timestamp, resultFolder, valueFreq, performanceSample):
    """
    This function calculate the value of Completeness, save the results and return the Global values

    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            keyValueCount(rdd): The rdd of the source grouped by the values of an attribute with additional columns representing the Timeliness values.
            volatilityTime(float or string): Number of hours to consider the data still recent, used to derive quickly the amount of passed time
            columns(string): Attribute considered in the analysis
            timestamp(string): Timestamp attribute considered in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            valueFreq(boolean): Value set to True if the value degree of granulairty has been requested
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """

    # remap with key, count / volatiliyTime * ( max - min ) * update_rate(
    # column, key, initialTime_of_records_in_key, finalTime_of_records_in_key
    # )

    completenessFreqDoc = keyValueCount.map(lambda x: mapping_completeness_frequency(
        x, volatiliyTime, performanceSample)).filter(lambda x: x[2] != 2)
    print("-----Calculate Frequency as the number of rows per value divided by the seconds from the first analysis and the last one multiplied by the update_rate expected (obtained as the mean of the update rates ): -> ( Attribute's Value, RecordNumber, Completeness Frequency Value )")
    # print(completenessFreqDoc.take(5))

    sumRecord = completenessFreqDoc.map(lambda x: x[1]).sum()
    meanValue = completenessFreqDoc.map(lambda x: x[2]).mean()

    if valueFreq:
        save_json(completenessFreqDoc, ["Value", "RecordNumber", "CompletenessFrequencyValue"],
                  resultFolder + "/completeness_frequency_values/" + str(column) + "_" + str(timestamp), None)

    return sumRecord, meanValue


def timeliness(sc, sqlContext, document, documentDF, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample, dictHeaderPosition):
    """
    This function calculate the dimension of Timeliness and prepare the Rdd to evaluate the dimension of Completeness_Frequency,, then save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """
    print(" ")
    print("Timeliness and Completeness Frequency")
    print(" ")

    updatePerTuple = False

    if "tuple" in granularityDimensions['Timeliness'].split(","):
        updatePerTuple = True

    print("\nUpdating timeliness based on delta time\n")
    timelinessInfo = sqlContext.read.json(
        inputSource + "/timelinessMetadata").head()

    headers = documentDF.columns

    dataToUpdateRdd = documentDF.fillna("null").rdd.map(
        lambda x: [i.encode("UTF8", "ignore") for i in x])
    # the analysis time is set here
    datetimeNow = datetime.datetime.now()
    print("-Save the timestamp of the analysis")

    if updatePerTuple:
        # updating old timeliness if the connected attribute is in desired
        # columns
        timelinessNameList = [x for x in timelinessInfo.timelinessNames]
        timeFormatsList = [x for x in timelinessInfo.timeFormats]
        # because indexes are all over the places
        for element in range(len(timelinessNameList)):
            name = timelinessNameList[element]
            i = headers.index(name)
            # getting the name of the attribute connected to timeliness
            attributeName = name.split("_")[1]
            attributePosition = headers.index(
                attributeName)  # and the position
            if attributeName in desiredColumns:
                dataToUpdateRdd = dataToUpdateRdd.map(lambda x: x[0:i] + [datetimeNow - datetime.datetime.strptime(x[attributePosition], timeFormatsList[element])] + x[i + 1:]).map(lambda x: x[0:i] + [(
                    float(x[i].microseconds + (x[i].seconds + x[i].days * 24 * 3600) * 10**6) / 10**6)] + x[i + 1:]).map(lambda x: x[0:i] + [str((max(0.0, 1 - (x[i] / (float(volatiliy[0]) * 3600)))))] + x[i + 1:])
                print("-Updated timeliness")

    print("-Save updated timeliness")

    finalCompletenessRdd = sc.emptyRDD()
    finalTimeRdd = sc.emptyRDD()

    updateValues = False
    updateGlobal = False

    attrTimeFreq = []
    attrName = []
    confidence = []
    counter = 0

    if timelinessAnalyzer:

        # get the desired degrees of granularity
        try:
            granularity = granularityDimensions["Timeliness"].split(",")
            globalTime = True if "global" in granularity else False
            attributeTime = True if "attribute" in granularity else False
            valueTime = True if "value" in granularity else False

            if granularity[0] == "":
                globalTime = True
        except Exception:
            globalTime = True
            attributeTime = False
            valueTime = False

    else:
        globalTime = False
        attributeTime = False
        valueTime = False

    if completenessAnalyzer is True:
        # get the desired degrees of granularity
        try:
            granularity = granularityDimensions[
                "Completeness_Frequency"].split(",")
            globalFreq = True if "global" in granularity else False
            attributeFreq = True if "attribute" in granularity else False
            valueFreq = True if "value" in granularity else False

            if granularity[0] == "":
                globalFreq = True
        except Exception:
            globalFreq = True
            attributeFreq = False
            valueFreq = False

    else:
        globalFreq = False
        attributeFreq = False
        valueFreq = False

    for j in columns:

        print(" ")
        print("-Timestamp Attribute = " + str(desiredColumns[j]))

        # select the first format of timestamp found
        stringFormat = dataTypes[j]

        # the volatiliy is converted in seconds
        volatiliyTime = float(volatiliy[counter]) * 3600.0
        counter = counter + 1

        # load the update rate, if the file is not available an error is
        # returned and the analysis should stop
        if completenessAnalyzer:
            updateValues = False
            try:
                updateRateValuesDF = sqlContext.read.json(
                    inputSource + "/update_rate_values_" + str(desiredColumns[j]))
                print("-Update rate for each value available")
                updateValues = True
            except Exception:
                print("-Update rate for each value not available")

            updateGlobal = False
            try:
                updateRateGlobalDF = sqlContext.read.json(
                    inputSource + "/update_rate_global_" + str(desiredColumns[j]))
                print("-Update rate for the source available")
                updateGlobal = True
            except Exception:
                print("-Update rate for the source not available")

        # search global frequency
        if updateGlobal & completenessAnalyzer & globalFreq:
            print(" ")
            print("--Calculate Global Frequency")

            completenessHour = (document.map(lambda x: datetime.datetime.strptime(x[j], stringFormat))
                                .map(lambda x: (x.hour, (1, x.date(), x.date())))
                                .reduceByKey(lambda x, y: (x[0] + y[0], max(x[1], y[1]), min(x[2], y[2])))
                                .map(lambda x: (x[0], (float(x[1][0]), float((x[1][1] - x[1][2]).days + 1))))
                                )

            print("---Current New Elements per Hour")
            # print(completenessHour.take(5))

            updateRateGlobal = updateRateGlobalDF.rdd.map(
                lambda x: (x.Hour, (x.Frequency)))
            # print(updateRateGlobal.take(4))
            completenessHour = (completenessHour.join(updateRateGlobal)
                                                .map(lambda x: (x[0], x[1][0][0], max(0.0, min(1.0, x[1][0][0] / (float(x[1][1]) * float(x[1][0][1]) * performanceSample * 3600)))))
                                )

            print("---Completeness Frequency per Hour")
            # print(completenessHour.take(5))

            save_json(completenessHour, ["Hour", "RecordNumber", "CompletenessFrequency"],
                      resultFolder + "/completeness_frequency_global_hour/" + str(desiredColumns[j]))

            qualityCompletenessFrequency = completenessHour.map(lambda x: x[
                                                                2]).mean()

            print("---Global Frequency = " + str(qualityCompletenessFrequency))

            attrTimeFreq.append(qualityCompletenessFrequency)
            attrName.append("Completeness_Frequency_" + str(desiredColumns[j]))
            confidence.append(performanceSample)

            print(" ")

        print("--Calculate Dimensions per Attribute")

        for stringIndex in columnsKey:

            # key columns = keyColumns
            stringSplitIndex = stringIndex.split(",")
            newIndexes = [desiredColumns.index(k) for k in stringSplitIndex]
            newDocument = document.map(
                lambda x: restriction_filter(x, newIndexes, j))

            stringAttribute = '_'.join(stringSplitIndex)

            try:
                # it is useless to group by the timestamps
                if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                    continue

            except Exception:
                pass

            if j in newIndexes:
                continue

            print("--Key Attribute = " + stringAttribute)

            keyValueDocument = (newDocument.map(lambda x: (x[0], datetime.datetime.strptime(x[1], stringFormat)))
                                .map(lambda x: (x[0], datetimeNow - x[1], x[1].hour))
                                .map(lambda x: ((x[0], x[2]), (float(x[1].microseconds + (x[1].seconds + x[1].days * 24 * 3600) * 10**6) / 10**6)))
                                .map(lambda x: ((x[0]), 1 - (x[1] / volatiliyTime)))
                                )

            print("----Convert timestamp into datetime and calculate the difference in seconds with respect to the data analysis: -> (( Attribute's Value, Hour ), Timeliness )")
            # print(keyValueDocument.take(5))

            if updateValues & completenessAnalyzer & (attributeFreq or valueFreq):

                # convert to datetime, create the key (value,hour) with the
                # difference in seconds, join the update rate with the same key
                # (value,hour) to obtain the frequency

                currentUpdateRate = (updateRateValuesDF.filter(updateRateValuesDF.Key == stringAttribute).drop("Key")
                                     .rdd.map(lambda x: ((x.Value.encode("UTF8", "ignore"), x.Hour), x.Frequency))
                                     )

                # print(currentUpdateRate.take(5))

                newTimeDocument = keyValueDocument.join(currentUpdateRate)

                if not newTimeDocument.isEmpty():
                    print(
                        "----Select the elements of the update rate with the same key column and join the previous data with it")
                    # print(newTimeDocument.take(5))

                    # reduce the key together, this allow to derive min and max
                    # time plus the count of record per key

                    newTimeDocument = newTimeDocument.combineByKey(lambda x: (x[0], 1.0, x[0], x[0], x[1]),
                                                                   lambda x, value: (
                                                                       x[0] + value[0], x[1] + 1, max(x[2], value[0]), min(x[3], value[0]), max(x[4], value[1])),
                                                                   lambda x, y: (x[0] + y[0], x[1] + y[1], max(x[2], y[2]), min(x[3], y[3]), max(x[4], y[4])))

                    # average by key,hour: sum,count,max,min,frequency

                    print("----Derive sum of the timeliness value, the number of rows, maximum, minimum time and the update rate for each couple of value and hour: -> ( ( Attribute's Value, Hour ),( TimelinessSum, Count, MaxTimeliness, MinTimeliness, Update Rate) )")
                    # print(newTimeDocument.take(5))

                    print("Remap and redefine the key only as the value, and then repeat the same reduction by considering also the different hours for the same key deriving the frequency as count/expected count plus min and max timeliness: -> ( ( Attribute's Value, Mean Update Rate, Sum Timeliness, Count, Max Timeliness, Min Timeliness) )")

                    keyValueCount = (newTimeDocument.map(lambda x: (x[0][0], (x[1][4], x[1][0], x[1][1], x[1][2], x[1][3])))
                                                    .combineByKey(lambda x: (x[0], 1.0, x[1], x[2], x[3], x[4]),
                                                                  lambda x, value: (x[0] + value[0], x[1] + 1, x[2] + value[1], x[
                                                                      3] + value[2], max(x[4], value[3]), min(x[5], value[4])),
                                                                  lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], max(x[4], y[4]), min(x[5], y[5])))
                                                    .map(lambda x: (x[0], x[1][0] / x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))
                                     )

                    # frequency,sum_timeliness,count_timeliness,max_timeliness,min_timeliness
                    # print(keyValueCount.take(5))

                    print(" ")
                    print("----Completeness_Frequency")

                    startTime = time.time()

                    # derive also the completeness frequency using the previous
                    # results
                    completenessSumRecord, completenessMeanValue = completeness_frequency(
                        sc, sqlContext, keyValueCount, volatiliyTime, stringAttribute, desiredColumns[j], resultFolder, valueFreq, performanceSample)

                    endTime = time.time()

                    print("-----Completeness Frequency Elapsed Time: " +
                          str(endTime - startTime) + " seconds")

                    if attributeFreq:
                        # completeness frequency rdd
                        finalCompletenessRdd = finalCompletenessRdd.union(sc.parallelize(
                            [(stringAttribute, completenessSumRecord, completenessMeanValue, performanceSample)]))

                    if timelinessAnalyzer & (attributeTime or valueTime):
                        averageByKey = keyValueCount.map(lambda (label, update_rate, value_sum, count, max, min): (label, value_sum / float(count), max, min, datetimeNow.strftime(stringFormat)))
                else:

                    if timelinessAnalyzer & (attributeTime or valueTime):
                        averageByKey = (keyValueDocument.map(lambda x: (x[0][0], x[1]))
                                        .combineByKey(lambda x: (x, 1.0, x, x),
                                                      lambda x, value: (
                                                          x[0] + value, x[1] + 1, max(x[2], value), min(x[3], value)),
                                                      lambda x, y: (x[0] + y[0], x[1] + y[1], max(x[2], y[2]), min(x[3], y[3])))
                                        .map(lambda (label, (sumTime, countTime, maxTime, minTime)): (label, sumTime / float(countTime), maxTime, minTime, datetimeNow.strftime(stringFormat)))
                                        )
            else:

                if timelinessAnalyzer & (attributeTime or valueTime):
                    averageByKey = (keyValueDocument.map(lambda x: (x[0][0], x[1]))
                                    .combineByKey(lambda x: (x, 1.0, x, x),
                                                  lambda x, value: (
                                                      x[0] + value, x[1] + 1, max(x[2], value), min(x[3], value)),
                                                  lambda x, y: (x[0] + y[0], x[1] + y[1], max(x[2], y[2]), min(x[3], y[3])))
                                    .map(lambda (label, (sumTime, countTime, maxTime, minTime)): (label, sumTime / float(countTime), maxTime, minTime, datetimeNow.strftime(stringFormat)))
                                    )

            if timelinessAnalyzer & attributeTime:
                print(" ")
                print("----Final Timeliness per Attribute's Value: -> (Value, TimelinessMean, TimelinessMax, TimelinessMin, AnalysisTime )")
                # print(averageByKey.take(5))

                # Final aggregated value for timeliness
                timelinessMeanValue = averageByKey.map(lambda x: x[1]).mean()
                timelinessMaxValue = averageByKey.map(lambda x: x[2]).max()
                timelinessMinValue = averageByKey.map(lambda x: x[3]).min()

                if timelinessMeanValue < 0:
                    timelinessMeanValue = 0.0
                if timelinessMaxValue < 0:
                    timelinessMaxValue = 0.0
                if timelinessMinValue < 0:
                    timelinessMinValue = 0.0

                finalTimeRdd = finalTimeRdd.union(sc.parallelize(
                    [(stringAttribute, timelinessMeanValue, timelinessMaxValue, timelinessMinValue, performanceSample, datetimeNow.strftime(stringFormat))]))

            if timelinessAnalyzer & valueTime:
                averageByKey = averageByKey.map(lambda x: (
                    x[0], max(0.0, x[1]), max(0.0, x[2]), max(0.0, x[3]), x[4]))
                save_json(averageByKey, ["Value", "TimelinessMean", "TimelinessMax", "TimelinessMin", "AnalysisTime"],
                          resultFolder + "/timeliness_values/" + stringAttribute + "_" + str(desiredColumns[j]))

        if timelinessAnalyzer & attributeTime:
            print("-Calculate value of Timeliness per Attribute: --> Attribute, Mean Timeliness, Max Timeliness, Min Timeliness, Confidence, Analysis Time")
            # print(finalTimeRdd.take(5))

            # save file into hdfs
            save_json(finalTimeRdd, ["Attribute", "TimelinessMean", "TimelinessMax", "TimelinessMin",
                                     "Confidence", "AnalysisTime"], resultFolder + "/timeliness_attributes_" + str(desiredColumns[j]))

        if updateValues & attributeFreq & completenessAnalyzer & (not finalCompletenessRdd.isEmpty()):
            print(
                "-Value of Completeness per Attribute's value: --> Value, Mean Completeness")
            # print(finalCompletenessRdd.take(5))

            # save file into hdfs
            save_json(finalCompletenessRdd, ["Attribute", "RecordNumber", "CompletenessFrequencyValue",
                                             "Confidence"], resultFolder + "/completeness_frequency_attributes_" + str(desiredColumns[j]))

        # Aggregate Values of different timestamps
        if timelinessAnalyzer & globalTime:
            globalTimeDocument = keyValueDocument.map(lambda x: (x[1]))

            attrTimeFreq.append(max(0.0, globalTimeDocument.mean()))
            attrTimeFreq.append(max(0.0, globalTimeDocument.max()))
            attrTimeFreq.append(max(0.0, globalTimeDocument.min()))
            attrName.append("Timeliness_Mean_" + str(desiredColumns[j]))
            attrName.append("Timeliness_Max_" + str(desiredColumns[j]))
            attrName.append("Timeliness_Min_" + str(desiredColumns[j]))
            confidence.append(performanceSample)
            confidence.append(performanceSample)
            confidence.append(performanceSample)

            # Add analysis Timestamp
            attrTimeFreq.append(str(datetimeNow.strftime(stringFormat)))
            attrName.append("Last_Analysis_Timestamp_" +
                            str(desiredColumns[j]))
            confidence.append(performanceSample)

    return dataToUpdateRdd, attrName, attrTimeFreq, confidence


def volume(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the quality value of the Volume, save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """
    print(" ")
    print("Volume")
    print(" ")

    attrVolume = []
    attrName = []
    confidence = []

    # get the desired degrees of granularity
    try:
        granularity = granularityDimensions["Volume"].split(",")
        globalVol = True if "global" in granularity else False
        attributeVol = True if "attribute" in granularity else False
        valueVol = True if "value" in granularity else False

        if granularity[0] == "":
            globalVol = True
    except Exception:
        globalVol = True
        attributeVol = False
        valueVol = False

    if globalVol:
        print("-Global Volume")
        rowCount = document.count()

        # calculate volume as the fraction between the number of rows of the
        # file after the filters and the requirements and the total available
        # rows before the filtering
        qualityVolume = min(1.0, rowCount / float(totVolume))

        attrName.append("Volume")
        attrVolume.append(qualityVolume)
        confidence.append(performanceSample)
        attrName.append("Volume(TotalRows)")
        attrVolume.append(rowCount)
        confidence.append(performanceSample)

    if valueVol:
        for stringIndex in columnsKey:

            # key columns = keyColumns
            stringSplitIndex = stringIndex.split(",")
            newIndexes = [desiredColumns.index(k) for k in stringSplitIndex]
            newDocument = document.map(
                lambda x: restriction_filter(x, newIndexes))
            stringAttribute = '_'.join(stringSplitIndex)

            try:
                # it is useless to group by the timestamps
                if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                    continue

            except Exception:
                pass

            print("-Key Attribute = " + stringAttribute)

            keyValueDocument = newDocument.map(lambda x: (x, 1)).reduceByKey(
                lambda x, y: x + y).map(lambda x: (x[0], x[1], x[1] / float(totVolume)))

            print(
                "--Return the count of rows per Attribute's Value: -> ( Attribute's Value, Count, VolumeValue )")
            # print(keyValueDocument.take(4))

            save_json(keyValueDocument, [
                      "Value", "Count", "VolumeValue"], resultFolder + "/volume_values/" + stringAttribute)

    return attrName, attrVolume, confidence


def distinctness(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the dimension of Distinctness for each attribute, save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """
    print(" ")
    print("Distinctness & Population")
    print(" ")

    attrDistinctPop = []
    attrName = []
    confidence = []

    finalDistinctRdd = sc.emptyRDD()
    finalPopulationRdd = sc.emptyRDD()

    if populationAnalyzer:
        # get the desired degrees of granularity
        try:
            granularity = granularityDimensions[
                "Completeness_Population"].split(",")
            globalPop = True if "global" in granularity else False
            attributePop = True if "attribute" in granularity else False
            valuePop = True if "value" in granularity else False

            if granularity[0] == "":
                globalPop = True
        except Exception:
            globalPop = True
            attributePop = False
            valuePop = False
    else:
        globalPop = False
        attributePop = False
        valuePop = False

    if distinctnessAnalyzer:
        # get the desired degrees of granularity
        try:
            granularity = granularityDimensions["Distinctness"].split(",")
            globalDistinct = True if "global" in granularity else False
            attributeDistinct = True if "attribute" in granularity else False
            valueDistinct = True if "value" in granularity else False

            if granularity[0] == "":
                globalDistinct = True
        except Exception:
            globalDistinct = True
            attributeDistinct = False
            valueDistinct = False
    else:
        globalDistinct = False
        attributeDistinct = False
        valueDistinct = False

    # Analysis

    rowCount = document.count()
    print(rowCount)
    if globalDistinct:
        # print(document.take(100))
        distinctDocument = document.map(lambda x: (tuple(x), 1)).reduceByKey(
            lambda x, y: x + y).map(lambda x: float(x[1]) / 2).filter(lambda x: x > 1)
        # print(distinctDocument.take(400))
        # print(distinctDocument.count())
        duplicateDocumentLines = distinctDocument.sum()

        print("Duplicate Lines")
        print(duplicateDocumentLines)

        qualityDistinctness = max(
            0.0, 1 - (duplicateDocumentLines / float(rowCount)))

        print("Distinctness " + str(qualityDistinctness))

        attrName.append("Distinctness")
        attrDistinctPop.append(qualityDistinctness)
        confidence.append(performanceSample)

    if attributePop or attributeDistinct:
        previousSource = sqlContext.read.json(
            inputSource + "/source_distinctness")
        correctAttributePop = False

        for stringIndex in columnsKey:

            # key columns = keyColumns
            stringSplitIndex = stringIndex.split(",")
            newIndexes = [desiredColumns.index(k) for k in stringSplitIndex]
            newDocument = document.map(
                lambda x: restriction_filter(x, newIndexes))
            stringAttribute = '_'.join(stringSplitIndex)

            try:
                # it is useless to group by the timestamps
                if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                    continue

            except Exception:
                pass

            print("--Key Attribute = " + stringAttribute)

            currentCount = newDocument.distinct().count()

            if attributePop & populationAnalyzer & (len(stringSplitIndex) == 1):
                correctAttributePop = True
                previousCompletePopulation = previousSource.filter(
                    previousSource.Attribute == stringSplitIndex[0]).head()["Count"]

                qualityCompletenessPopulation = min(
                    1.0, currentCount / float(previousCompletePopulation))
                print("--The Completeness Population Value is derived as the fraction between the number of distinct elements in the document and the new global number of elements found after the addition of the new values found in this document = " + str(qualityCompletenessPopulation))

                finalPopulationRdd = finalPopulationRdd.union(sc.parallelize(
                    [(stringSplitIndex[0], currentCount, qualityCompletenessPopulation, performanceSample)]))

            if attributeDistinct & distinctnessAnalyzer:
                qualityDistinctness = currentCount / float(rowCount)

                print("--The Distinctness Value is derived as the fraction between the number of distinct elements in the document and the number of rows in the document")

                finalDistinctRdd = finalDistinctRdd.union(sc.parallelize(
                    [(stringAttribute, currentCount, qualityDistinctness, performanceSample)]))

        if attributePop & populationAnalyzer & correctAttributePop:
            print("-Calculate value of Completeness Population per Attribute: --> Attribute, Distinct Count, Completeness Population, Confidence")
            # print(finalPopulationRdd.take(3))

            save_json(finalPopulationRdd, ["Attribute", "DistinctCount", "CompletenessPopulationValue",
                                           "Confidence"], resultFolder + "/completeness_population_attributes")

        if attributeDistinct & distinctnessAnalyzer:
            print("-Calculate value of Distinctness per Attribute: --> Attribute, Distinct Count, Distinctness, Confidence")
            # print(finalDistinctRdd.take(3))

            save_json(finalDistinctRdd, ["Attribute", "DistinctCount", "Distinctness",
                                         "Confidence"], resultFolder + "/distinctness_attributes")

    return attrName, attrDistinctPop, confidence


def consistency_zero_division(line):
    """
    This function returns a new line for each Row of the rdd with the evaluation of the Consistency dimension

    Args:
            line(Row): row of the rdd
    """
    try:
        return (line[0], float(line[1][1]) / line[1][0])
    except Exception:
        return (line[0], 0.0)


def multiple_row_filter(line, antecedent, consequent):
    """
    This function remap the rdd in order to match the requested rule

    Args:
            line(Row): row of the rdd
            antecedent(list): list of the indexes of the attributes that are part of the antecedent elements of the rule
            consequent(list): list of the indexes of the attributes that are part of the consequent elements of the rule
    """
    return (((','.join([line[i] for i in antecedent])), (','.join([line[j] for j in consequent]))), 1)


def consistency(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the dimension of consistency for each rule, save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """
    print(" ")
    print("Consistency")
    print(" ")

    # get the desired degrees of granularity
    try:
        granularity = granularityDimensions["Consistency"].split(",")
        globalCon = True if "global" in granularity else False
        attributeCon = True if "attribute" in granularity else False
        valueCon = True if "value" in granularity else False

        if granularity[0] == "":
            globalCon = True
    except Exception:
        globalCon = True
        attributeCon = False
        valueCon = False

    finalConsistentRdd = sc.emptyRDD()
    isRules = False
    try:
        rules = sqlContext.read.json(inputSource + "/association_rules")
        dataframeColumns = rules.columns
        isRules = True
    except Exception:
        print("No pre-existent association rules")

    try:

        # add custom rules. 'x1,x2:y1','x1:y1'

        for customRule in associationRules:
            newCustomRule = customRule.split(":")
            print(newCustomRule)
            print(isRules)
            if not isRules:
                rules = sqlContext.createDataFrame([(newCustomRule[0], newCustomRule[1])], [
                                                   "Antecedent", "Consequent"])
                isRules = True
            else:
                newRow = sqlContext.createDataFrame(
                    [(newCustomRule[0], newCustomRule[1])], rules.columns)
                rules = rules.unionAll(newRow)

        print("-List of Rules:")

        # invert dictionary
        #dictPositionHeader = dict([(v, k) for k, v in dictHeaderPosition.iteritems()])
        # rules.show()

        if attributeCon or valueCon:

            for row in rules.rdd.collect():

                print("-Checking rule: " + str(row[0]) + " -> " + str(row[1]))

                antecedents = [x.encode("UTF8", "ignore")
                               for x in row[0].split(",")]
                consequents = [x.encode("UTF8", "ignore")
                               for x in row[1].split(",")]

                # print(desiredColumns)
                antecedent = [desiredColumns.index(i) for i in antecedents]
                consequent = [desiredColumns.index(j) for j in consequents]

                consistentRdd = document.map(
                    lambda x: multiple_row_filter(x, antecedent, consequent))
                print("--Select only the interested columns")
                # print(consistentRdd.take(2))

                consistentRdd = (consistentRdd.reduceByKey(lambda x, y: x + y)
                                 .map(lambda x: (x[0][0], x[1]))
                                 .combineByKey(lambda x: (x, x),
                                               lambda x, value: (
                                                   x[0] + value, max(x[1], value)),
                                               lambda x, y: (x[0] + y[0], max(x[1], y[1])))
                                 .map(consistency_zero_division)
                                 .map(lambda x: (x[0], str(row[0]) + " -> " + str(row[1]), x[1]))
                                 )

                print("--Count the number of occurrence of both antecedent and consequent in the document, group by the antecedent and from the different partial counts, return the sum and the maximum, finally derives the consistency of the rule the sum of all the previous partial count with the same antecedent and the maximum number of different")
                # print(consistentRdd.take(5))

                if valueCon:
                    save_json(consistentRdd, ["AntecedentValue", "Rule", "ConsistencyValue"],
                              resultFolder + "/consistency_values/" + str(row[0]) + "_" + str(row[1]))

                if attributeCon:
                    consistentValue = consistentRdd.map(lambda x: x[2]).mean()
                    print("--Mean Consistent Value for current rule = " +
                          str(consistentValue))

                    partialConsistentRdd = sc.parallelize([(row[0].encode("UTF8", "ignore"), row[
                                                          1].encode("UTF8", "ignore"), consistentValue, performanceSample)])

                    finalConsistentRdd = finalConsistentRdd.union(
                        partialConsistentRdd)

        if attributeCon:
            print("-List of all rules with the Mean Consistency Value: ( Antecedent, Consequent, Consistency Value, Confidence ) ")

            # calculate final aggregated value for consistency
            meanConsistencyValue = finalConsistentRdd.map(
                lambda x: x[2]).mean()

            # save file into hdfs
            save_json(finalConsistentRdd, ["RuleAntecedent", "RuleConsequent", "ConsistencyValue",
                                           "Confidence"], resultFolder + "/consistency_attributes_mean_per_rule")

    except Exception:
        print("-no available association rules")
        import traceback
        traceback.print_exc()

    return [], [], []


def check_association_rules(row, rules, headerPositionList):
    """
    This function check if the row satisfies any of the association consistency rules providede by the user.

    Args:
        row(list): the row to check
        rules(list): list of correct bindings for the parameters
        heaerPositionList: header position as gathered from the preliminary informations
    """
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


def association_consistency(document, dictHeaderPosition, desiredColumns, newRuleList):
    """
    This function performs a per tuple check to see if the desired columns satisty the consistency rules provided by the user

    Args:
        document(RDD): the document to analyze
        dictHeaderPosition(dict): preliminary informations on the position of the headers
        desiredColumns(list): desired columns as provided by the user in the config file
        newRuleList(list): association consistency rules
    """
    print(" ")
    print("Updating association consistency")
    print(" ")

    consistencyRulesCount = 0.0
    invertedDictHeaderPosition = {v: k for k,
                                  v in dictHeaderPosition.iteritems()}
    consistencyRulesList = []
    headerPositionList = []

    for f in newRuleList:
        try:
            rulesDF = sqlContext.read.json(f)
            header = rulesDF.columns
            if set(header) <= set(desiredColumns):
                rulesRDD = rulesDF.rdd
                rulesRDD = rulesRDD.map(
                    lambda x: [i.encode("UTF8", "ignore") for i in x])
                consistencyRulesList.append(rulesRDD.collect())
                headerPositionList.append(
                    [int(invertedDictHeaderPosition[i]) for i in header])
                consistencyRulesCount += 1
        except Exception as e:
            print("Cannot open " + f + " file")

    # print("######Consistency rules#######")
    # print(consistencyRulesList)

    if (consistencyRulesCount == 0):
        return document

    oldAssociationConsistencyPosition = invertedDictHeaderPosition[
        'ASSOCIATION_CONSISTENCY']

    document = document.map(lambda x: x[0:oldAssociationConsistencyPosition] + [str(check_association_rules(
        x, consistencyRulesList, headerPositionList) / consistencyRulesCount)] + x[oldAssociationConsistencyPosition + 1:])

    return document


def main(sc, sqlContext, configuration_path, performanceSample):
    """
    $$$$$ Main Program $$$$$

    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            configuration_path(string): Absolute path of the location of the configuration file for the analysis
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """

    """
    IMPORT Step
    """

    # load the data
    configuration_file = sc.textFile(configuration_path).zipWithIndex()

    # Extract Input Files
    inputSource = configuration_file.filter(lambda x: x[1] == 0).map(
        lambda x: x[0]).collect()[0].encode("UTF8", "ignore")
    inputSource = inputSource.split(';')
    inputFolderList = []
    if(len(inputSource) > 1 and inputSource[1] != '*'):
        inputFolderList = inputSource[1:]
    inputSource = inputSource[0]
    print(inputSource)
    print(inputFolderList)

    # Extract Input Files
    resultFolder = configuration_file.filter(lambda x: x[1] == 1).map(
        lambda x: x[0]).collect()[0].encode("UTF8", "ignore")

    tsForOutputFolder = datetime.datetime.fromtimestamp(
        time.time()).strftime('_%Y%m%d-%H%M%S')
    resultFolder = resultFolder + tsForOutputFolder
    resultFolder = resultFolder + "_confidence_" + str(performanceSample)
    print(resultFolder)

    # Extract Source Quality Requirement
    sourceDesired = int(configuration_file.filter(
        lambda x: x[1] == 12).map(lambda x: x[0]).collect()[0])
    print(sourceDesired)

    if sourceDesired:
        sourceQuality = sqlContext.read.json(inputSource + "/source_quality")
        save_json(sourceQuality, None, resultFolder + "/source_quality")
        return 1

    # Extract Desired Columns
    desiredColumns = configuration_file.filter(
        lambda x: x[1] == 2).map(lambda x: x[0]).collect()[0].split(";")
    desiredColumns = [x.encode("UTF8", "ignore") for x in desiredColumns]
    print(desiredColumns)

    # Extract Key Columns
    keyColumns = configuration_file.filter(lambda x: x[1] == 3).map(
        lambda x: x[0]).collect()[0].split(";")
    keyColumns = [x.encode("UTF8", "ignore") for x in keyColumns]
    print(keyColumns)

    # Extract Desired Intervals
    desiredIntervalsDim = configuration_file.filter(
        lambda x: x[1] == 4).map(lambda x: x[0]).collect()[0].split(";")
    desiredIntervalsDim = [x.encode("UTF8", "ignore")
                           for x in desiredIntervalsDim]
    print(desiredIntervalsDim)

    # Extract Desired Elements
    desiredElementsDim = configuration_file.filter(
        lambda x: x[1] == 5).map(lambda x: x[0]).collect()[0].split(";")
    desiredElementsDim = [x.encode("UTF8", "ignore")
                          for x in desiredElementsDim]
    print(desiredElementsDim)

    # Extract Desired Dimensions
    desiredDimensions = configuration_file.filter(
        lambda x: x[1] == 6).map(lambda x: x[0]).collect()[0].split(";")
    desiredDimensions = [x.encode("UTF8", "ignore") for x in desiredDimensions]
    forceConsistencyRefresh = False
    if "Consistency-R" in desiredDimensions:
        print("-Forcing update of association consistency")
        forceConsistencyRefresh = True
        i = desiredDimensions.index("Consistency-R")
        desiredDimensions[i] = "Consistency"
    print(desiredDimensions)

    # Extract Validity for Timeliness
    volatilities = configuration_file.filter(lambda x: x[1] == 7).map(
        lambda x: x[0]).collect()[0].split(";")
    volatilities = [x.encode("UTF8", "ignore") for x in volatilities]
    print(volatilities)

    # Extract mean Value for Accuracy
    meanAccuracyValues = configuration_file.filter(
        lambda x: x[1] == 8).map(lambda x: x[0]).collect()[0].split(";")
    meanAccuracyValues = [x.encode("UTF8", "ignore")
                          for x in meanAccuracyValues]
    print(meanAccuracyValues)

    # Extract maximum deviation for Accuracy
    devAccuracyValues = configuration_file.filter(
        lambda x: x[1] == 9).map(lambda x: x[0]).collect()[0].split(";")
    devAccuracyValues = [x.encode("UTF8", "ignore") for x in devAccuracyValues]
    print(devAccuracyValues)

    # Extract desider association rules
    associationRules = configuration_file.filter(
        lambda x: x[1] == 10).map(lambda x: x[0]).collect()[0].split(";")
    associationRules = [x.encode("UTF8", "ignore") for x in associationRules]
    print(associationRules)

    # Extract granularities
    granularityDim = configuration_file.filter(
        lambda x: x[1] == 11).map(lambda x: x[0]).collect()[0].split(";")
    granularityDim = [x.encode("UTF8", "ignore") for x in granularityDim]
    print(granularityDim)

    # Extract association rule files
    newRuleList = configuration_file.filter(lambda x: x[1] == 13).map(
        lambda x: x[0]).collect()[0].split(";")
    newRuleList = [x.encode("UTF8", "ignore") for x in newRuleList]
    print(newRuleList)

    """
    Union and Structuration
    """

    totStart = time.time()

    start = time.time()

    # initialize the final quality dictionary
    quality = {}
    finalConfidence = []
    finalQuality = sqlContext.createDataFrame([{"_test_": "test"}])

    """
    try to get the preliminary information
    """

    jparsing = False

    try:
        preliminaryInformation = sqlContext.read.json(
            inputSource + "/preliminary_information").head()
    except Exception as e:
        print("Missing Preliminary Information")

    print("Basic Information Retrieval...")

    try:
        volatiliy = float(preliminaryInformation.volatiliy)
        print(volatiliy)
    except Exception:
        volatiliy = 17520.0

    for i in range(len(volatilities)):
        if volatilities[i] == "":
            volatilities[i] = volatiliy

    header = preliminaryInformation.header
    header = [x.encode("UTF8", "ignore") for x in header]
    print("-Header Getted..")
    print(header)

    xmlRegex = preliminaryInformation.regex.encode("UTF8", "ignore")
    print(xmlRegex)
    if xmlRegex == "":
        jparsing = True

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
    dictHeaderPosition = dict([(int(k), v.encode("UTF8", "ignore"))
                               for k, v in dictHeaderPosition.iteritems()])

    ready = True

    """
    DATA structuration
    """
    # search the corrected analysed data in the source path
    requestedNames = []
    try:
        analysedNames = sqlContext.read.json(
            inputSource + "/source_analysed_updates")
        analysedName = analysedNames.filter(
            analysedNames.correctAnalysis == 1).rdd.map(lambda x: (x.fileName)).collect()
        if(len(inputFolderList) >= 1):
            for fName in analysedName:
                if fName in inputFolderList:
                    requestedNames.append(fName)
        else:
            requestedNames = list(analysedName)

        inputPaths = [(inputSource + "/updates/" + fName +
                       ".txt").encode("UTF8", "ignore") for fName in requestedNames]
        inputPaths = ','.join(inputPaths)
        print("--Profiled files " + str(requestedNames))

    except Exception:
        print("There are no files in the source")
        return 0

    if jparsing:
        fields = [StructField(field_name, StringType(), True)
                  for field_name in preliminaryInformation.header]
        print(fields)
        schema = StructType(fields)

        requestedPaths = [inputSource +
                          "/extended_dataset/" + f for f in requestedNames]

        try:
            documentDF = sqlContext.read.schema(schema).json(requestedPaths)
        except Exception:
            print("Input file not found, run profiling again")
            return 0

        print(documentDF.count())
        print(documentDF.columns)
    else:
        try:
            document = sc.textFile(inputSource + "/updates/*.txt")
        except Exception:
            print("File not found, check the input path and try again")
            return 0

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

    end = time.time()

    timeRdd = sc.parallelize([("Structuration", (end - start))])
    print("- Structuration Elapsed Time: " + str(end - start) + " seconds")

    start = time.time()

    """
    Confidence Selector
    """
    # choose the sample based on the performance
    print("Fraction considered: " + str(performanceSample))

    """
    DATA Selection
    """

    # associate desiredDimensions with possible columns
    dictPositionHeader = dict([(v, k)
                               for k, v in dictHeaderPosition.iteritems()])
    print(dictPositionHeader)

    desiredNum = [dictPositionHeader[i] for i in desiredColumns]
    print(desiredNum)

    dimensionColumn = dict((k, dimensionColumn.pop(k, None))
                           for k in desiredDimensions)
    print(dimensionColumn)

    for k in dimensionColumn.iterkeys():
        print(k)
        newList = [desiredNum.index(i) for i in dimensionColumn[
            k] if i in desiredNum]
        dimensionColumn[k] = newList

    print(dimensionColumn)

    # dataTypes
    dataTypes = [dataTypes[i] for i in desiredNum]

    # set values for volatiliy: volatilities

    # set the mean and dev values for accuracy: meanAccuracy, devAccuracy

    """
    Column Selection
    """

    if not jparsing:
        filteredDocumentDF = document.toDF(header).select(desiredColumns)
    else:
        filteredDocumentDF = documentDF.select(desiredColumns)

    # documentDF.show(5)
    totVolume = filteredDocumentDF.count()
    print("Total Volume = " + str(totVolume))

    #documentDF = documentDF.sample(False,0.0002)

    #totVolume = documentDF.count()
    #print("Total Volume = " + str(totVolume))
    try:

        """
        Intervals Selection
        """
        newDocumentDF = sqlContext.createDataFrame(
            sc.emptyRDD(), filteredDocumentDF.schema)

        dictDesiredPosition = dict([(v, k)
                                    for k, v in enumerate(desiredColumns)])
        for colo in desiredColumns:
            # newDocumentDF.show(5)
            print(colo)

            selectedIntervals = desiredIntervalsDim[
                dictDesiredPosition[colo]].split(",")
            if selectedIntervals[0] != "":
                print("Deleting Null")
                filteredDocumentDF = filteredDocumentDF.dropna(
                    how="any", subset=[colo])

            for interval in selectedIntervals:
                extremes = interval.split(":")
                print(extremes)

                if (extremes[0] == '') & (len(extremes) == 1):
                    continue
                else:
                    if colo in dictHeaderNames["Timeliness"]:
                        convertFormat = "%d/%m/%Y %H.%M.%S"
                        stringFormat = dataTypes[dictDesiredPosition[colo]]

                        timeFunc = udf(lambda x: datetime.datetime.strptime(
                            x, stringFormat), TimestampType())

                        filteredDocumentDF = filteredDocumentDF.withColumn(
                            "Time" + str(colo), timeFunc(col(colo)))

                        if (extremes[0] != '') & (extremes[1] != ''):
                            print("both")

                            leftTimestamp = datetime.datetime.strptime(
                                extremes[0], convertFormat)
                            rightTimestamp = datetime.datetime.strptime(
                                extremes[1], convertFormat)
                            # print(documentDF.show(5))

                            splitDocumentDF = filteredDocumentDF.filter(
                                (col("Time" + str(colo)) >= leftTimestamp) & (col("Time" + str(colo)) <= rightTimestamp))
                            # splitDocumentDF.show(5)
                        else:
                            if extremes[0] != '':
                                print("left")
                                #documentDF = documentDF.filter("" + str(colo) + ">=" + str(extremes[0]))
                                leftTimestamp = datetime.datetime.strptime(
                                    extremes[0], convertFormat)

                                splitDocumentDF = filteredDocumentDF.filter(
                                    col("Time" + str(colo)) >= leftTimestamp)

                            else:
                                print("right")
                                rightTimestamp = datetime.datetime.strptime(
                                    extremes[1], convertFormat)
                                splitDocumentDF = filteredDocumentDF.filter(
                                    col("Time" + str(colo)) <= rightTimestamp)

                        splitDocumentDF = splitDocumentDF.drop(
                            "Time" + str(colo))

                    else:
                        if (extremes[0] != '') & (extremes[1] != ''):
                            print("both")
                            splitDocumentDF = filteredDocumentDF.filter((col(colo).cast("string") >= str(
                                extremes[0])) & (col(colo).cast("string") <= str(extremes[1])))
                        else:
                            if extremes[0] != '':
                                print("left")
                                #documentDF = documentDF.filter("" + str(colo) + ">=" + str(extremes[0]))
                                splitDocumentDF = filteredDocumentDF.filter(
                                    col(colo).cast("string") >= str(extremes[0]))
                            else:
                                print("right")
                                splitDocumentDF = filteredDocumentDF.filter(
                                    col(colo).cast("string") <= str(extremes[1]))

                    # print(splitDocumentDF.show())
                    print("end interval")

                newDocumentDF = newDocumentDF.unionAll(splitDocumentDF)
                # newDocumentDF.show(5)
            print("end column")

            if not newDocumentDF.rdd.isEmpty():
                filteredDocumentDF = newDocumentDF

        # documentDF.show()

        """
        Values Selection
        """
        for colo in desiredColumns:
            colValues = desiredElementsDim[
                dictDesiredPosition[colo]].split(",")

            print(colo)
            print(colValues)
            if colValues[0] == '':
                continue
            else:
                filteredDocumentDF = filteredDocumentDF.dropna(
                    how="any", subset=[colo])
                #filterDict = map(lambda x: (colo, x), colValues)

                # print(filterDict)
                #filterValues = sqlContext.createDataFrame(filterDict,["partial",colo]).drop("partial")

                # filterValues.show()
                #documentDF = documentDF.join(filterValues,colo,'inner')
                # documentDF.show()

                filteredDocumentDF = filteredDocumentDF.filter(
                    col(colo).isin(colValues))
                # documentDF.show()

        # documentDF.show()

    except Exception:
        print("the file to analyse is empty after the selection module, or there is a problem with it")
        traceback.print_exc()
        return 0

    """
    Desired Degrees of Granularity and requirements
    """
    granularityDimensions = {}
    for i in range(len(desiredDimensions)):
        granularityDimensions[desiredDimensions[i]] = granularityDim[i]

    """
    Dimension Variable Settings ( if the dimensions that are calculated together are not both requested )
    """
    completenessAnalyzer = False
    timelinessAnalyzer = False
    distinctnessAnalyzer = False
    populationAnalyzer = False

    # set timeliness and completeness_frequency
    if "Completeness_Frequency" in dimensionColumn.keys():
        completenessAnalyzer = True

        if "Timeliness" in dimensionColumn:
            timelinessAnalyzer = True
        else:
            timelinessAnalyzer = False
            dimensionColumn["Timeliness"] = dimensionColumn[
                "Completeness_Frequency"]

        del dimensionColumn["Completeness_Frequency"]
    else:
        completenessAnalyzer = False

        if "Timeliness" in dimensionColumn.keys():
            timelinessAnalyzer = True
        else:
            timelinessAnalyzer = False

    print(completenessAnalyzer)
    print(timelinessAnalyzer)

    # set timeliness and completeness_frequency
    if "Completeness_Population" in dimensionColumn.keys():
        populationAnalyzer = True

        if "Distinctness" in dimensionColumn:
            distinctnessAnalyzer = True
        else:
            distinctnessAnalyzer = False
            dimensionColumn["Distinctness"] = dimensionColumn[
                "Completeness_Population"]

        del dimensionColumn["Completeness_Population"]
    else:
        populationAnalyzer = False

        if "Distinctness" in dimensionColumn.keys():
            distinctnessAnalyzer = True
        else:
            distinctnessAnalyzer = False

    print(populationAnalyzer)
    print(distinctnessAnalyzer)

    """
    Document Sampling
    """
    if performanceSample < 1:
        filteredDocumentDF = filteredDocumentDF.sample(
            False, performanceSample)

    end = time.time()

    print("- Selection Elapsed Time: " + str(end - start) + " seconds")
    timeRdd = timeRdd.union(sc.parallelize([("Selection", (end - start))]))

    """
    Document Missing Dimension
    """

    # convert to Rdd
    notNullDocumentDF = filteredDocumentDF.dropna()
    document = filteredDocumentDF.fillna("null").rdd.map(
        lambda x: [i.encode("UTF8", "ignore") for i in x])

    print("Completeness_Missing Analysis")
    # completeness_missing
    if "Completeness_Missing" in dimensionColumn.keys():
        try:
            start = time.time()

            qualityName, partialQuality, confidence = completeness_missing(
                sc,
                sqlContext,
                document,
                dimensionColumn["Completeness_Missing"],
                dataTypes,
                volatilities,
                desiredColumns,
                resultFolder,
                dimensionColumn,
                keyColumns,
                meanAccuracyValues,
                devAccuracyValues,
                timelinessAnalyzer,
                completenessAnalyzer,
                distinctnessAnalyzer,
                populationAnalyzer,
                inputSource,
                associationRules,
                totVolume,
                granularityDimensions,
                performanceSample)

            end = time.time()

            print("- Completeness_Missing Elapsed Time: " +
                  str(end - start) + " seconds")
            timeRdd = timeRdd.union(sc.parallelize(
                [("Completeness_Missing", (end - start))]))

            # save final quality
            for j in range(len(qualityName)):
                quality[qualityName[j]] = partialQuality[j]
                finalQuality = finalQuality.withColumn(
                    qualityName[j], lit(partialQuality[j]))
                finalConfidence.append(confidence[j])

        except Exception:
            print("Error in dimension Completeness_Missing, solve the problem, delete the partial results and run the analysis again")
            traceback.print_exc()

    document = notNullDocumentDF.rdd.map(
        lambda x: [i.encode("UTF8", "ignore") for i in x])
    # print(document.take(4))

    """
    DATA ANALYSIS
    """
    start = time.time()

    dimensionName = {'Accuracy': accuracy, 'Precision': precision, 'Completeness_Missing': completeness_missing,
                     'Distinctness': distinctness, 'Consistency': consistency, 'Timeliness': timeliness, 'Volume': volume}

    print("Starting Other Dimensions Analysis")
    print(dimensionColumn)

    for i in dimensionColumn.iterkeys():

        if i in ["Completeness_Frequency", "Completeness_Population", "Completeness_Missing"]:
            continue

        try:

            func = dimensionName[i]
            start = time.time()

            if(func == timeliness):
                completeDocumentRdd, qualityName, partialQuality, confidence = func(
                    sc,
                    sqlContext,
                    document,
                    documentDF,
                    dimensionColumn[i],
                    dataTypes,
                    volatilities,
                    desiredColumns,
                    resultFolder,
                    dimensionColumn,
                    keyColumns,
                    meanAccuracyValues,
                    devAccuracyValues,
                    timelinessAnalyzer,
                    completenessAnalyzer,
                    distinctnessAnalyzer,
                    populationAnalyzer,
                    inputSource,
                    associationRules,
                    totVolume,
                    granularityDimensions,
                    performanceSample,
                    dictHeaderPosition)
            else:
                qualityName, partialQuality, confidence = func(
                    sc,
                    sqlContext,
                    document,
                    dimensionColumn[i],
                    dataTypes,
                    volatilities,
                    desiredColumns,
                    resultFolder,
                    dimensionColumn,
                    keyColumns,
                    meanAccuracyValues,
                    devAccuracyValues,
                    timelinessAnalyzer,
                    completenessAnalyzer,
                    distinctnessAnalyzer,
                    populationAnalyzer,
                    inputSource,
                    associationRules,
                    totVolume,
                    granularityDimensions,
                    performanceSample)

            end = time.time()

            # save times
            if i == "Timeliness":
                if timelinessAnalyzer & completenessAnalyzer:
                    print("- " + str(i) + " + Completeness_Frequency " +
                          " Elapsed Time: " + str(end - start) + " seconds")
                    timeRdd = timeRdd.union(sc.parallelize(
                        [(str(i) + "Completeness_Frequency", (end - start))]))
                else:
                    if completenessAnalyzer:
                        print("Completeness_Frequency " +
                              " Elapsed Time: " + str(end - start) + " seconds")
                        timeRdd = timeRdd.union(sc.parallelize(
                            [("Completeness_Frequency", (end - start))]))
                    else:
                        print(str(i) + " Elapsed Time: " +
                              str(end - start) + " seconds")
                        timeRdd = timeRdd.union(
                            sc.parallelize([(str(i), (end - start))]))

            # save times
            if i == "Distinctness":
                if distinctnessAnalyzer & populationAnalyzer:
                    print("- " + str(i) + " + Completeness_Population " +
                          " Elapsed Time: " + str(end - start) + " seconds")
                    timeRdd = timeRdd.union(sc.parallelize(
                        [(str(i) + "Completeness_Population", (end - start))]))
                else:
                    if populationAnalyzer:
                        print("Completeness_Population " +
                              " Elapsed Time: " + str(end - start) + " seconds")
                        timeRdd = timeRdd.union(sc.parallelize(
                            [("Completeness_Population", (end - start))]))
                    else:
                        print(str(i) + " Elapsed Time: " +
                              str(end - start) + " seconds")
                        timeRdd = timeRdd.union(
                            sc.parallelize([(str(i), (end - start))]))

            else:
                print("-" + str(i) + " Elapsed Time: " +
                      str(end - start) + " seconds")
                timeRdd = timeRdd.union(
                    sc.parallelize([(str(i), (end - start))]))

            # save final quality
            for j in range(len(qualityName)):
                quality[qualityName[j]] = partialQuality[j]
                finalQuality = finalQuality.withColumn(
                    qualityName[j], lit(partialQuality[j]))
                finalConfidence.append(confidence[j])

        except Exception:
            print("Error in dimension " + str(i) +
                  " , solve the problem, delete the partial results and run the analysis again")
            traceback.print_exc()

    if "Consistency" in dimensionColumn.iterkeys() and "tuple" in granularityDimensions["Consistency"].split(","):
        # ASSOCIATION CONSISTENCY UPDATE
        print("\n\nAssociation consistency\n\n")
        updateAssociationConsistency = True
        oldRulesList = preliminaryInformation.consistencyRuleFiles
        print("## Old rules list ")
        print(oldRulesList)
        print("## New rules list")
        print(newRuleList)

        if set(oldRulesList) == set(newRuleList) and not forceConsistencyRefresh:
            updateAssociationConsistency = False
            print("## Old association consistency still valid, not updating")

        if updateAssociationConsistency:
            completeDocumentRdd = association_consistency(
                completeDocumentRdd, dictHeaderPosition, desiredColumns, newRuleList)

    # adding the confidence
    try:
        conf = sum(finalConfidence) / float(len(finalConfidence))
    except Exception:
        conf = "Error"

    finalQuality = finalQuality.withColumn("Confidence", lit(conf))

    finalQuality = finalQuality.drop("_test_")
    print(" ")
    print("final Quality")
    finalQuality.show()
    save_json(finalQuality, None, resultFolder + "/final_quality")

    flattenedList = [i for k in granularityDimensions.iterkeys()
                     for i in granularityDimensions[k].split(",")]

    print(flattenedList)
    if "tuple" in flattenedList:
        print("-Saving updated tuple to final subfolder")
        save_json(completeDocumentRdd, list(
            dictHeaderPosition.values()), resultFolder + "/extended_dataset")

    totEnd = time.time()
    print("Total Time Elapsed: " + str(totEnd - totStart))

    def toCSVLine(data):
        return ','.join(str(d) for d in data)

    lines = timeRdd.map(toCSVLine)
    save_txt(lines, resultFolder + "/Times")

    return 1

"""
Entry Point
"""
if __name__ == "__main__":

    # reload(sys)
    # sys.setdefaultencoding('utf-8')

    args = sys.argv[1:]
    # get configuration path and performance sample
    configuration_path = args[0]
    performanceSample = float(args[1])
    # create spark variables
    conf = SparkConf().setAppName("DQAssessment_v1.3")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext.getOrCreate(sc)

    # call the main function
    correctAnalysis = main(
        sc, sqlContext, configuration_path, performanceSample)
    print(correctAnalysis)
    sc.stop()
