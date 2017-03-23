############################################################################################
"""
# Title: Craft Density 
# Purpose: Programmatically identified the density of craft transversing CRT Navigations
# Created by: L.Velasquez
# Software: Python 2.7 - Anaconda and ArcGIS 10.2
# Date: February 2016
# Process:
    	# User input  csv file
	Working with the CSV file using Pandas:
	# Create DataFrame using CSV files
        	Cleanse the data.
            o	Records where Easting or Northings are Zero
            o	Sightings for craft 991100
            o	Craft with only one observation  Done by looking at frequency of the craft within the dataframe
        	Sort sightings by dates
        	Create dictionary where Key: Cardinal number : Value: numpyArray for the DataFrame  each array represents only one craft
	# Geoprocessing
        	Using the dictionary do the following: For each craft
            o	Change numpyArray to gdb Table
            o	Create XY event
            o	Create the end date and range for each sighting. (range = date of first sighting  date of next sighting
            o	Create a list of the notification  It is going to be used to loop through using a pair of sightings at the time to create the routes
            o	Select records for a pair of notification at the time and create route. If the craft transverse a navigation that is not part of CRT between sightings; the script wont calculate a route for that pair of notifications. The script keeps calculating the routes for the other observations as long as there are along CRT navigations
            o	For each individual route (pair of notifications) adds the following: Route Number, Notification and Date
            o	Append each individual route to the Final Routes feature class
            o	Clear individual routes ready for the next set of notifications for the same craft
	Create a copy of the Final Routes layer called Notification Routes. The last will allow the modification of the field name and alias 
	Calculate density. Spatially join the midpoint polygon to the Final Routes  Add Join the output to the 100 Segments of Canal and Dissolve this last join to create the Density Feature class. The value showing how many times a segment has been crossed is: XXX.Join_Count

# Note:
    # To run this script the user most have the PANDA module for python.
    # This script was created using Anaconda, which contains the PANDA module.
"""
##################################### MODULES ###############################################

import arcpy, time, os, shutil, errno
import pandas as pd
import numpy as np

start_time = time.time()

##################################### FUNCTIONS ###############################################

def LicExt(extension,licence):
    
    if arcpy.CheckExtension(extension) == "Available":
        arcpy.CheckOutExtension(extension)
        print (" Network Analyst extension checked out")
    else:
        print (arcpy.GetMessages())

    if arcpy.CheckProduct(licence) == "Available":
        print  (" Selected an ArcView license")
        arcpy.SetProduct(licence)
    else:
       print (" Starting geoprocessing routine...")
       print (arcpy.GetMessages())

def XYevent(table,FC):
    in_Table = table
    x_coords = "Easting"
    y_coords = "Northing"
    out_Layer = "Sightings"
    spRef = r"G:\Misc\Coordinate systems\Projection_files\British National Grid.prj"

    #Make the xy event layer
    arcpy.MakeXYEventLayer_management(in_Table, x_coords, y_coords, out_Layer, spRef)
    arcpy.CopyFeatures_management(out_Layer,FC,"#","0","0","0")
    print (' Done XY Feature Class')
    return FC

def dateRange(fcSighthings,sort_field,sortFinal):
    # Start sorting process by date
    lyrsortSight = arcpy.MakeFeatureLayer_management(fcSighthings,"lyrsortSight")

    #Adding endDate and rangeDate
    arcpy.AddField_management(lyrsortSight, "EndDate", "DATE")
    arcpy.CalculateEndTime_management(lyrsortSight, "Date", "EndDate")
    calcExp = '!Date! + " - " + !EndDate! '
    arcpy.AddField_management(lyrsortSight, "dateRange", "TEXT", "#", "#", "50")
    finalSort = arcpy.CalculateField_management(lyrsortSight, "dateRange", calcExp, "PYTHON_9.3")

    #Append new records to the final sort fc
    arcpy.Append_management(lyrsortSight,sortFinal,"NO_TEST","","")
    
    return finalSort

def Selection(FeatClass,expStart,expEnd):
    lyrFc = arcpy.MakeFeatureLayer_management(FeatClass,"singleboat")   
    selectionStart = arcpy.SelectLayerByAttribute_management(lyrFc,"NEW_SELECTION",expStart)

    selectionEnd = arcpy.SelectLayerByAttribute_management(lyrFc,"ADD_TO_SELECTION",expEnd)
    
    return selectionStart,selectionEnd 

def NetAnalysis (network_dataset, out_analysis_layer, impedance,fm,locationStart,locationEnd,singleRoute):
    try:
        arcpy.MakeRouteLayer_na (network_dataset, out_analysis_layer, impedance, "#", "#", "#", "#", "ALLOW_UTURNS", \
        "#", "#", "#", "TRUE_LINES_WITH_MEASURES", "#")
        

        arcpy.AddLocations_na(out_network_analysis_layer, "Stops", locationStart, fm, "500 Meters", "Equipment", "#", \
        "MATCH_TO_CLOSEST", "APPEND", "SNAP", "5 Meters", "#", "#")
        arcpy.AddLocations_na(out_network_analysis_layer, "Stops", locationEnd, fm, "500 Meters", "Equipment", "#", \
        "MATCH_TO_CLOSEST", "APPEND", "SNAP", "5 Meters", "#", "#")
        
                    
        # SOLVE   
        arcpy.Solve_na(out_network_analysis_layer)
        lines = out_network_analysis_layer + '\\Routes'  
        arcpy.Append_management(lines,singleRoute,"TEST","","")

        print (' Route Solved')

    except Exception as e:
        # If an error occurred, print line number and error message
        import traceback, sys
        tb = sys.exc_info()[2]
        return(("An error occured on line %i" % tb.tb_lineno))
        return((str(e)))

###########################################################################################################################################
########################################################CODE###############################################################################
###########################################################################################################################################    


######################################## LOCAL COPY OF THE DATA ###############################################


print ('... copying data to c drive')
workspace = r"C:\DensityAnalysis" 
src = r"G:\GIS_Admin\zNamed_folders\Luis\Tasks\2016\BoatDensity\DATA"

try:
    if os.path.exists(workspace):
        shutil.rmtree(workspace)
        shutil.copytree(src, workspace)
    
    else:
        shutil.copytree(src, workspace)
        
except OSError as exc: # python >2.5
    if exc.errno == errno.ENOTDIR:
        shutil.copy(src, workspace)
    else: raise

print (' Data copied to c drive ')

####################################### VARIABLES ###################################################


                                    ### GLOBAL ###
csvFile = raw_input('Enter the path and name of the sightings data: ')
sightings = workspace + os.sep + r"BoatDensity.gdb\Sightings_Final"
fcSortFinal = workspace + os.sep + r"\BoatDensityResults.gdb\sortRange_Final" 
routesNotification = workspace + os.sep + r"\BoatDensityResults.gdb\notificationRoutes"
fcCanals = workspace + os.sep + r"BoatDensity.gdb\CanalNetwork\Obs100m"
finalRoutes = workspace + os.sep + r"\BoatDensityResults.gdb\FinalRoutes"
midPoint = workspace + os.sep + r"BoatDensity.gdb\MidPoint50cm"
outputJoin = workspace + os.sep + r"BoatDensity.gdb\OutputJoin"
density = workspace + os.sep + r"\BoatDensityResults.gdb\Density"
Table = workspace + os.sep + r"BoatDensity.gdb\DensTable"


                                ###  NETWORK ANALYSIS  ###

in_network_dataset = workspace + os.sep + r"NavCanalNetwork.gdb\Navigable_Canal_Network\Navigable_Canal_Network_ND"
out_network_analysis_layer = "Routes"
impedance_attribute = "Length"
default_break_values = "#"
output_path_shape = "#"
field_mappings = "CurbApproach # 0;Name Name #"
temp_lines = "tempLines"
layer_name = "RoutesSingleBoat"
individualRoute = workspace + os.sep + r"BoatDensity.gdb\IndividualRoute"

####################################### CODE ###################################################
# Check licence and extension
extension = LicExt("Network", "ArcView")
print ('-----------------------------------------------')

# Overwriting files
arcpy.env.overwriteOutput = True

# Delete old files and leave fc ready for the process
arcpy.TruncateTable_management(fcSortFinal)
arcpy.TruncateTable_management(finalRoutes)
print (' I have deleted the records ')
print ('-----------------------------------------------')
print ('-----------------------------------------------')


######################### DATA PRE-PROCESSING #################################
###############################################################################
# Opening the csv using Pandas
data = pd.read_csv(csvFile)

#Change date string to date type
data['Date'] = pd.to_datetime(data['Date'])

# Get rid of sighitngs with errors in the Coordinates and boat 991100
data = data[data.Northing != 0] 
data = data[data.Easting!= 0]
data = data[data.Equipment != 991100]

# Get rid of craft with only one sighting
data['freq'] = data.groupby('Equipment')['Equipment'].transform('count')
OneSighting = data[data.freq == 1] # Crafts with only one sighting
OneSighting.drop('freq', axis=1, inplace=True) # get rid of the freq column
OneSighting = OneSighting.reset_index(drop=True) # Reset index


# Data set with crafts with more than one sighting for the year
data = data[data.freq != 1]
data.drop('freq', axis=1, inplace=True) # get rid of the freq column
data = data.reset_index(drop=True) # Reset index

# Create list of unique values
craftList = pd.unique(data.Equipment.ravel())
remainingCraft = len(craftList)
print (' There are %s crafts in this dataset' %remainingCraft)

# Sort by date
data = data.sort_values(by='Date', ascending= True)
data = data.reset_index(drop=True)

# create subsets of data for each craft
craftDic = {}
i= 0 # Place within the dictionary for each craft dataset
for craft in craftList:
        
    craftSubset = data.loc[(data.Equipment == craft)]
    
    ## Change the dataframe to a numpyArray   
    x = np.array(np.rec.fromrecords(craftSubset.values))
    names = craftSubset.dtypes.index.tolist()
    x.dtype.names = tuple(names)

    craftDic[i] = x # appending numpyArray to the dictionary 
    i += 1 # move position in the dictionary for next numpyArray

######################### GEO-PROCESSING #################################
###############################################################################
idNumber = 1
for key, value in craftDic.iteritems():
    
    arcpy.env.overwriteOutput = True
    print ('-----------------------------------------------')
    print (' I am doing ' + str(craftDic[key][1][1]))
    
    
    # Create the gdb table for the XY event fc
    if arcpy.Exists(Table):
        arcpy.Delete_management(Table)
        print (' I deleted table')
        arcpy.da.NumPyArrayToTable(value, Table)
    else:
        arcpy.da.NumPyArrayToTable(value, Table)

    check = int(arcpy.GetCount_management(Table).getOutput(0))
    print (' The number of sightings for %s are: %s ' %((craftDic[key][1][1]),check))
    
#    
#    # Create the sighting layer
    sightings = XYevent(Table,sightings)

    # Create layer from sighting fc
    lyrCraft = arcpy.MakeFeatureLayer_management(sightings,"singleCraft")   
    
    sortedLayer = dateRange(sightings,"Date",fcSortFinal)
    print (' I have created the end date and date range ')

    notifList = []
    cursor = arcpy.SearchCursor(sortedLayer)
    for row in cursor:
        fcVal = row.getValue("Notification")
        notifList.append(str(fcVal))
    del row
        
    # Looping through notification for a single craft to create the routes
    i = 0
    while i < (len(notifList) - 1):
        start = notifList[i]
        end = notifList[i+1]
        print (start, end)

        ### Selection ###
        expStart = 'Notification =' + start 
#            expStart = 'Notification =' + "'" + start + "'"
#            expEnd = 'Notification =' + "'" + end + "'"
        expEnd = 'Notification =' + end
        print (expStart) 
        print (expEnd)

        select = Selection(sortedLayer,expStart,expEnd)
        selectionStart = select[0]
        selectionEnd = select[1]

        NA = NetAnalysis(in_network_dataset, out_network_analysis_layer, impedance_attribute,field_mappings,selectionStart,selectionEnd,individualRoute)

        #Adding route name and notification to route
        rows = arcpy.UpdateCursor(individualRoute)
        for row in rows:
            row.Name = "Routes" + str(idNumber)
            row.FirstStopID = start
            rows.updateRow(row)
        del row
        del rows

        print (' notification, routeID  added to the route ')

        #Append record to final routes
        arcpy.Append_management(individualRoute,finalRoutes,"TEST","","")
        print (' Append done')

        # Delete record from the Individual Route
        arcpy.TruncateTable_management(individualRoute)
        arcpy.Delete_management("in_memory")
        print (' Moving to next set of Notifications')

        i += 1

    idNumber += 1
    remainingCraft = remainingCraft - 1
    arcpy.Delete_management("in_memory")
    print ('-----------------------------------------------')
    print ('-----------------------------------------------')
    print ('-----------------------------------------------')
    print (' There are %s remaining crafts in the process ' %remainingCraft)
    print ('-----------------------------------------------')
    print ('-----------------------------------------------')
print ('-----------------------------------------------')
print ("...creating the notification per route dataset")

# Create Routes Notification feature class and cleanse the attribute table
arcpy.CopyFeatures_management(finalRoutes, routesNotification)
dropFields = ["LastStopID","StopCount"]
arcpy.DeleteField_management(routesNotification, dropFields)
print (' Notification per route dataset has been created ')

####### This changes the field name and alias from FirstStop to Notification - It requires ArcGIS 10.2 or above
#######fieldList = arcpy.ListFields(routesNotification)
#######for field in fieldList: #loop through each field
#######        if field.name == "FirstStopID":
#######            arcpy.AlterField_management(routesNotification, field, 'Notification', 'Route_Notification') # Needs ArcGIS 10.2
#####
######################################################DENSITY PROCESS ###################################################################

print ("... Starting to calculate density")
# Creating Density per canal section

lyrCanals = arcpy.MakeFeatureLayer_management(fcCanals, 'canals_lyr')
lyrMidPoint = arcpy.MakeFeatureLayer_management(midPoint, 'midPoint_lyr')
lyrRoutes = arcpy.MakeFeatureLayer_management(finalRoutes, 'routes_lyr')
# Set field count to Zero

##arcpy.CalculateField_management(lyrCanals, "Density", 0, "PYTHON_9.3") # NEED TO DELETE THIS BIT!!!
##print 'Density field set to zero'

# Join midPoint to routes spatially
joinLayer = arcpy.SpatialJoin_analysis(lyrMidPoint, lyrRoutes, outputJoin, "JOIN_ONE_TO_ONE", "KEEP_ALL")

# Join to 100m Canal Section
arcpy.AddJoin_management(lyrCanals, "LineID", outputJoin, "LineID", "KEEP_ALL")
print ('done join 100m')

# Dissolve 
arcpy.Dissolve_management(lyrCanals, density, "OutputJoin.Join_Count", "", "SINGLE_PART", "DISSOLVE_LINES")
print (' I have now created the density feature class')
print ("--- The execution time was %s minutes ---" % ((time.time() - start_time)/60))
print (' The script has completed')
