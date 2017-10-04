#import arcpy

# Select only properties that are within the state boundaries (gets rid of any properties in a state db that were mis-geocoded

# Clip leveed areas out of properties files
# def remove_properties_in_leveed_areas(state_numbers):
    # leveed_areas = [shapefile]
    # for state in state_numbers:
        # all_properties = read in shapefile of properties
        # outfile = coastal_properties_state_{0}_nonleveed .format(state)
        # arcpy.Erase_management(all_properties, leveed_areas, outfile)


# ID CI properties for each state, year, and projection
# def identify_ci_properties(state_numbers, years, projection):
    # for state in state_numbers:
        # properties = read in data file (shapefile or just table ? of properties-nonleveed)

        # for year in years:
            # inundated_area = shapefile (pass in state, year, projection}
            # outfile = ci_properties_state_{0}_{1}_{2} .format(state, year, projection)
            # ci_properties = arcpy.SelectByLocation_management(properties, inundated_area, "WITHIN") # check parameters here
            # ci_properties.save(outfile)

# Output state statistics for year, projection, number of CI properties, total value of CI properties
    # Output as shapefile
    # Output as CSV

# Aggregate and Output year, projection, number of CI properties, total value of CI properties per county subdivision
    # Output as shapefile
    # Output as CSV


# Analyze other parameters (year built, foundation type, loan type, loan due date)
    # Of the CI properties, how many will be CI before mortgage is paid off?
    # Of the CI properties, are there patterns to age of building? I.e. are there more old houses getting CI first?
    # Square footage
    # Property taxes