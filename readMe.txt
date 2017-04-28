################## Run INFO ###################

#The algorithm named DQProfiling.py is a pySpark script that should be called whenever an incremental analysis is needed to create the Profiling quality Information.
#	This algorithm should be called before the other one by specifying up to 5 parameters, the first 2 always mandatory, the successive 2 mandatory the first time a new source is analysed:
#		input_file(string): Absolute path of the location of the new portion of data to analyse
#		resultFolder(string): Absolute path of the destination of all the saved files
#		volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
#		timeFormat(list): List of strings containing the possible timestamp format of the attributes
#		xmlPath(string): Absolute path of the location of the xml with the structure of the data

#The algorithm named DQAssessment.py is a pySpark script that should be called whenever an on-demand analysis is needed, it needs the Profiling information to correctly evaluate some dimensions.
#	This algorithm should be called by specifying 2 parameters:
#		configuration_path(string): Absolute path of the location of the configuration file for the analysis
#		performanceSample(float): Number from 0+ to 1 representing the portion of data that has to be considered in the analysis


#Both the algorithm can be run by a spark-submit command followed by the path of the file and the previous corresponding parameters.



#################### Results-Parameters INFO #####################
#All the information regarding the output and the configuration will be explained here:


#The file of the sources are in the folders: "data/doc1/","data/doc2/" while the configuration file for the on demand analysis is located in the folder "data/assessment/"

#Data Profiling List of Files, the ones with an * should be given in input, and are obtained with simple external algorithms that should be substituted.

data/doc1		# source of BusUsers files 
	xmldoc1.xml		#file containing the structuration information for the files,it contains separators, headers, elements and timestamps in appearance order. It is MANDATORY (for the current version, a complete version is in development)	
	
	association_rules*	#folder of the json file containing the consistency rules 1 per line
		
	update_rate_values_DATAUTILIZACAO*	#folder	of the json file containing the expected number of tuples per value and per hour for each column for the considered timestamp, it will be joined with the analyzed file so all the values should appear in it
	update_rate_global_DATAUTILIZACAO*	#folder	of the json file containing the expected number of tuples per hour for the entire source for the considered timestamp

	preliminary_information			#folder	of the json file containing the preliminary information of the entire source, this allow to speed up the structuration after the first ever analysis
		header				#name of attributes of the source's files
		volatility			#volatility in hours for the timeliness profiling dimension, after the first run it is saved here and this will be used for each successive update file 
		dimensionAllowedTypes		#it is a dictionary that associate to each dimension of quality, the type of attribute allowed
		datatypes			#types of the element in the attributes of the source's files, if there is a timestamp, the file will contain the string format of the datetime object 
		dimensionAttributePosition	#it is a dictionary that represents all the analyzable attribute's indexes for each quality dimension
		dimensionAttributeName		#it is a dictionary that represents all the analyzable attribute's names for each quality dimension	
		attributePosition		#it is a dictionary that associate to each attribute index, it's name
		regex				#regular expression that will be used to extract the elements from each file, if it is empty, then the files are jsons anche can be read without parsing

	#The previous files are derived from the source data, they can be modified only to improve the results of the analysis or they should be derived precisely after a more deep analysis of the source

	source_analysed_updates		#folder	of the json file containing the name of the analysed portion of file with an associated integer set to 1 if the analysis has been successful, 0 otherwise
	source_distinctness		#folder	of the json file containing the number of distinct values plus the distinctness value for each attribute of the source (this information are available in the source_quality, but are necessary to update the quality)
	source_distinct_values		#folder	of the json file containing the distinct values for each attribute of the source (this information are necessary to update the quality)
	source_quality			#folder	of the json file containing the quality values of all the dimension that can be calculated for the source

	#The previous files describe the source and will be updated each time a new file is added to the source, these should never be deleted
	
	updates			#folder containing all the update files of the source, here there are the data of the source
	Times			#folder	of the json file containing the execution times, in seconds, for each module of the algorithm that have to be executed

#Data Assessment, an example of the configuration file is explained

data/assessment
	configuration_file.txt				
		
		data/doc1								#Path to the source to analyse
		data/assessment/test							#Path to the output folder
		CODLINHA;CODVEICULO;NUMEROCARTAO;DATAUTILIZACAO;NOMELINHA		#Names of Attributes to Consider
		CODLINHA,CODVEICULO;CODVEICULO;NUMEROCARTAO				#Names of Key Attributes (Grouping Attributes)
		0:s,u:v;:v;;29/09/16 07:00:00,000000:30/09/16 23:00:00,000000;		#Interval of Values for each Attribute in the form of start:end separated by a comma. Even only one of the extremes could be considered, and an empty value should be inserted if no filter has to be applied
		216;BA600,KA696,HA284,02108,06055,DC293;;;				#List of values for each attribute to consider, empty string means all values
		Volume;Precision;Accuracy;Completeness_Missing;Distinctness;Timeliness;Completeness_Frequency;Completeness_Population;Consistency		#Desired Dimensions
		6000;									#Desired Volatility in hours (or empty if not needed)
		0002075900;								#Desired Mean Accuracy (or empty if not needed)
		200;									#Desired Interval Accuracy (or empty if not needed)
		CODLINHA,CODVEICULO:DATAUTILIZACAO;CODLINHA:CODVEICULO			#Additional Rules to be checked in consistency dimension in the form X:Y where X and Y can be formed by multiple attribute names separated by a comma	
		global,value;global,attribute,value;global,attribute,value;global,attribute,value;global,attribute;global,attribute,value;global,attribute,value;attribute;attribute,value		#Desired degrees of granularity for each chosen Dimension, this is the list of all the degrees selectable for each Dimension, the others will be ignored

	#In the result folder there could be different files, if only the global granularity has been specified, the quality results are in the final_quality Json file, otherwise the folder dimensionName_attribute and dimensionName_value can appear
	
	#I.E. (it could change)
		accuracy_attributes_{name}			#folder of the json containing the Accuracy Attribute degree of granularity values. The name refers to the considered attribute.		
		accuracy_values					#folder containing the folders of the Value degree of granularity for each grouping attribute and for each allowed dimension attribute.
			attribute_{name}_REF_{name}		#folder of the json containing the Accuracy Value degree of granularity values. The first name refers to the grouping attribute while the second refer to the allowed dimension attribute considered.
		
		completeness_frequency_attributes_{name}	#folder of the json containing the Completeness_Frequency Attribute degree of granularity values. The name refers to the considered attribute.		
		completeness_frequency_global_hour		#folder of the json containing the Completeness_Frequency value for each Hour of the day with at least 1 registered record.
		completeness_frequency_values			#folder containing the folders of the Value degree of granularity for each grouping attribute and for each allowed dimension attribute.
			{name}_{name}				#folder of the json containing the Completeness_Frequency Value degree of granularity values. The first name refers to the grouping attribute while the second refer to the allowed dimension attribute considered.

		completeness_missing_attributes			#folder of the json containing the Completeness_Missing Attribute degree of granularity values.		
		completeness_missing_values			#folder containing the folders of the Value degree of granularity for each grouping attribute.
			{name}					#folder of the json containing the Completeness_Missing Value degree of granularity values. The name refers to the grouping attribute.
		
		completeness_population_attributes		#folder of the json containing the Completeness_Population Attribute degree of granularity values.		
		
		consistency_attributes_mean_per_rule		#folder of the json containing the Consistency Attribute(Rule) degree of granularity values.
		consistency_values				#folder containing the folders of the Value degree of granularity for each rule considered.
			{name,name}_{name,name}			#folder of the json containing the Consistency Value degree of granularity values. The first name refers to the antecedent attributes separated by a comma while the second refers to the consequent attributes separated by a comma.

		distinctness_attributes				#folder of the json containing the Distinctness Attribute degree of granularity values.		
		
		precision_attributes_{name}			#folder of the json containing the Precision Attribute degree of granularity values. The name refers to the considered attribute.		
		precision_values				#folder containing the folders of the Value degree of granularity for each grouping attribute and for each allowed dimension attribute.
			attribute_{name}_number_{name}		#folder of the json containing the Precision Value degree of granularity values. The first name refers to the grouping attribute while the second refer to the allowed dimension attribute considered.

		timeliness_attributes_{name}			#folder of the json containing the Timeliness Attribute degree of granularity values. The name refers to the considered attribute.		
		timeliness_values				#folder containing the folders of the Value degree of granularity for each grouping attribute and for each allowed dimension attribute.
			{name}_{name}				#folder of the json containing the Timeliness Value degree of granularity values. The first name refers to the grouping attribute while the second refer to the allowed dimension attribute considered.

		volume_values					#folder containing the folders of the Value degree of granularity for each grouping attribute and for each allowed dimension attribute.
			{name}					#folder of the json containing the Volume Value degree of granularity values. The name refers to the grouping attribute.

		final_quality					#folder	of the json file containing the Global quality values of all the dimension that have been requested on the Data Object

		Times						#folder	of the json file containing the execution times, in seconds, for each module of the algorithm that have to be executed on the Data Object considered
