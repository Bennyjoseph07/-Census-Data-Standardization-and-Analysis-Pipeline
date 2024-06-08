import pandas as pd
import re,yaml
from docx import Document
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi



with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

census_data = config['census_data']
telangana_doc = config['telangana_doc']

df = pd.read_excel(census_data)




def renaming_columnname(df):
    """
    Rename columns in the input dataframe to more concise and standardized names.

    Parameters:
    df (pd.DataFrame): The input dataframe with original column names.

    Returns:
    pd.DataFrame: The dataframe with renamed columns.
    """
   
    df.rename(columns={'State name': 'State/UT','District name':'District',
                    'Male_Literate':'Literate_Male','Female_Literate':'Literate_Female',
                    'Rural_Households':'Households_Rural','Urban_Households':'Households_Urban',
                    'Age_Group_0_29':'Young_and_Adult','Age_Group_30_49':'Middle_Aged',
                    'Age_Group_50':'Senior_Citizen','Age not stated':'Age_Not_Stated',
                    "Households_with_Bicycle": "HouseholdsBicycle",
                        "Households_with_Car_Jeep_Van": "HouseholdsCarJeepVan",
                        "Households_with_Radio_Transistor": "HouseholdsRadioTransistor",
                        "Households_with_Scooter_Motorcycle_Moped": "HouseholdsScooterMotorcycleMoped",
                        "Households_with_Telephone_Mobile_Phone_Landline_only": "HouseholdsPhoneLandlineOnly",
                        "Households_with_Telephone_Mobile_Phone_Mobile_only": "HouseholdsPhoneMobileOnly",
                        "Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car": "HouseholdsTechVehicles",
                        "Households_with_Television": "HouseholdsTelevision",
                        "Households_with_Telephone_Mobile_Phone": "HouseholdsPhone",
                        "Households_with_Telephone_Mobile_Phone_Both": "HouseholdsPhoneBoth",
                        "Condition_of_occupied_census_houses_Dilapidated_Households": "DilapidatedHouseholds",
                        "Households_with_separate_kitchen_Cooking_inside_house": "HouseholdsSeparateKitchen",
                        "Having_bathing_facility_Total_Households": "HouseholdsBathingFacility",
                        "Having_latrine_facility_within_the_premises_Total_Households": "HouseholdsLatrineFacility",
                        "Ownership_Owned_Households": "OwnedHouseholds",
                        "Ownership_Rented_Households": "RentedHouseholds",
                        "Type_of_bathing_facility_Enclosure_without_roof_Households": "HouseholdsBathEnclosureNoRoof",
                        "Type_of_fuel_used_for_cooking_Any_other_Households": "HouseholdsOtherFuel",
                        "Type_of_latrine_facility_Pit_latrine_Households": "HouseholdsPitLatrine",
                        "Type_of_latrine_facility_Other_latrine_Households": "HouseholdsOtherLatrine",
                        "Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households": "HouseholdsNightSoilDrain",
                        "Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households": "HouseholdsFlushLatrineSystem",
                        "Not_having_bathing_facility_within_the_premises_Total_Households": "NoBathingFacilityHouseholds",
                        "Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households": "NoLatrineOpenSource",
                        "Main_source_of_drinking_water_Un_covered_well_Households": "HouseholdsUncoveredWell",
                        "Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households": "HouseholdsHandpumpBorewell",
                        "Main_source_of_drinking_water_Spring_Households": "HouseholdsSpringWater",
                        "Main_source_of_drinking_water_River_Canal_Households": "HouseholdsRiverCanal",
                        "Main_source_of_drinking_water_Other_sources_Households": "HouseholdsOtherWaterSources",
                        "Main_source_of_drinking_water_Other_sources_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households": "HouseholdsAllWaterSources",
                        "Location_of_drinking_water_source_Near_the_premises_Households": "WaterSourceNearPremises",
                        "Location_of_drinking_water_source_Within_the_premises_Households": "WaterSourceWithinPremises",
                        "Main_source_of_drinking_water_Tank_Pond_Lake_Households": "HouseholdsTankPondLake",
                        "Main_source_of_drinking_water_Tapwater_Households": "HouseholdsTapwater",
                        "Main_source_of_drinking_water_Tubewell_Borehole_Households": "HouseholdsTubewellBorehole",
                        "Household_size_1_person_Households": "Household1Person",
                        "Household_size_2_persons_Households": "Household2Persons",
                        "Household_size_1_to_2_persons": "Household1to2Persons",
                        "Household_size_3_persons_Households": "Household3Persons",
                        "Household_size_3_to_5_persons_Households": "Household3to5Persons",
                        "Household_size_4_persons_Households": "Household4Persons",
                        "Household_size_5_persons_Households": "Household5Persons",
                        "Household_size_6_8_persons_Households": "Household6to8Persons",
                        "Household_size_9_persons_and_above_Households": "Household9PlusPersons",
                        "Location_of_drinking_water_source_Away_Households": "WaterSourceAway",
                        "Married_couples_1_Households": "Household1Couple",
                        "Married_couples_2_Households": "Household2Couples",
                        "Married_couples_3_Households": "Household3Couples",
                        "Married_couples_3_or_more_Households": "Household3PlusCouples",
                        "Married_couples_4_Households": "Household4Couples",
                        "Married_couples_5__Households": "Household5Couples",
                        "Married_couples_None_Households": "HouseholdNoCouples",
                        "Power_Parity_Less_than_Rs_45000": "PowerParityBelow45000",
                        "Power_Parity_Rs_45000_90000": "PowerParity45000to90000",
                        "Power_Parity_Rs_90000_150000": "PowerParity90000to150000",
                        "Power_Parity_Rs_45000_150000": "PowerParity45000to150000",
                        "Power_Parity_Rs_150000_240000": "PowerParity150000to240000",
                        "Power_Parity_Rs_240000_330000": "PowerParity240000to330000",
                        "Power_Parity_Rs_150000_330000": "PowerParity150000to330000",
                        "Power_Parity_Rs_330000_425000": "PowerParity330000to425000",
                        "Power_Parity_Rs_425000_545000": "PowerParity425000to545000",
                        "Power_Parity_Rs_330000_545000": "PowerParity330000to545000",
                        "Power_Parity_Above_Rs_545000": "PowerParityAbove545000",
                        "Total_Power_Parity": "TotalPowerParity"},inplace=True)
    return df
    
    



def renaming_state_name(df):
    """
    Capitalize the first letter of each word in the 'State/UT' column 
    and make the word 'And' lowercase in the state names.

    Parameters:
    df (pd.DataFrame): The input dataframe with a 'State/UT' column.

    Returns:
    pd.DataFrame: The dataframe with renamed state names in the 'State/UT' column.
    """
    

    df["State/UT"]= df["State/UT"].str.title()
    def lowercase_word(text, word_to_lowercase):
        """
        Make a specific word lowercase in a given text.

        Parameters:
        text (str): The input text.
        word_to_lowercase (str): The word to be lowercased in the text.

        Returns:
        str: The text with the specific word lowercased.
        """
        pattern = re.compile(r'\b' +re.escape(word_to_lowercase) + r'\b', re.IGNORECASE)
        return pattern.sub(word_to_lowercase.lower(), text)
    
    df['State/UT'] = df['State/UT'].apply(lambda x: lowercase_word(x, 'And'))
    success_logger.info("State/UT column renamed successfully")
    return df
    





def new_state_formation(df,path):
    """
    Update the 'State/UT' column in the dataframe to 'Telangana' for districts listed in the given document.

    Parameters:
    df (pd.DataFrame): The input dataframe with 'District' and 'State/UT' columns.
    path (str): The file path to the document containing the list of districts.

    Returns:
    pd.DataFrame: The dataframe with updated 'State/UT' values for specified districts.
    """

    doc = Document(path)
    doc_text =""
    
    for paragraph in doc.paragraphs:
            doc_text += paragraph.text+','
    
    doc_text = doc_text.rstrip(',')
    dist_list= doc_text.split(',')
    
    df.loc[df['District'].isin(dist_list), 'State/UT'] = 'Telangana'

    

def missing_percentage(df):
    """
    Calculate the percentage of missing values for each column in the dataframe.

    Parameters:
    df (pd.DataFrame): The input dataframe.

    Returns:
    pd.DataFrame: A dataframe containing the missing value percentages for each column.
    """
    
    missing_percentage_df =pd.DataFrame()
    for column in df.columns:
        new_column_name = f'{column}'
        missing_percentage_df[new_column_name] = [round(df[column].isnull().sum() * 100 / len(df))]
        
    missing_percentage_df.reset_index(drop=True, inplace=True)

    return missing_percentage_df
   








def filling_data_population(df):
    """
    Fill missing population data by calculating the difference between Male and Female columns.

    Parameters:
    df (pd.DataFrame): The input dataframe.

    Returns:
    pd.DataFrame: The dataframe with filled population data.
    """
  
    df['Male'] = df['Male'].apply(int)
    df['Female'] = df['Female'].apply(int)
    df['Population'] = df['Population'].apply(int)

    for index,rows in df.iterrows():
        if rows['Male'] and rows['Population'] != 0:
            df.at[index,'Female'] = rows['Population'] - rows['Male']
        if rows['Female'] and rows['Population'] != 0:
            df.at[index,'Male'] = rows['Population'] - rows['Female']

    df['Population'] = df[['Male', 'Female']].sum(axis=1)
    return df
    



def filling_data_literate(df):
    """
    Cast columns to integers for calculations (assuming data represents whole numbers)


    Args:
        df (pandas.DataFrame): The DataFrame containing male, female, and total literate data.

    Returns:
        pandas.DataFrame: The DataFrame with missing values filled and total literate population recalculated.
    """
   

       
    df['Literate_Male'] = df['Literate_Male'].apply(int)
    df['Literate_Female'] = df['Literate_Female'].apply(int)
    df['Literate'] = df['Literate'].apply(int)

    for index,rows in df.iterrows():
        if rows['Literate_Male'] and rows['Literate'] != 0:
            df.at[index,'Literate_Female'] = rows['Literate'] - rows['Literate_Male']
        if rows['Literate_Female'] and rows['Population'] != 0:
            df.at[index,'Literate_Male'] = rows['Literate'] - rows['Literate_Female']

    df['Literate'] = df[['Literate_Male', 'Literate_Female']].sum(axis=1)
    return df
    


def filling_data_households(df):   
    """
    Fills in missing rural or urban household data points and calculates the total households
    based on the existing values.

    Args:
        df (pandas.DataFrame): The DataFrame containing rural, urban, and total households data.

    Returns:
        pandas.DataFrame: The DataFrame with missing values filled and total households recalculated.
    """ 

    df['Households_Rural'] = df['Households_Rural'].apply(int)
    df['Households_Urban'] = df['Households_Urban'].apply(int)
    df['Households'] = df['Households'].apply(int)

    for index,rows in df.iterrows():
        if rows['Households_Rural'] and rows['Households'] != 0:
            df.at[index,'Households_Urban'] = rows['Households'] - rows['Households_Rural']
        if rows['Households_Urban'] and rows['Population'] != 0:
            df.at[index,'Households_Rural'] = rows['Households'] - rows['Households_Urban']

    df['Households'] = df[['Households_Rural', 'Households_Urban']].sum(axis=1)
    return df
    


def filling_literate_education(df): 
    """
    Calculates the total literate education based on the individual education levels.

    Args:
        df (pandas.DataFrame): The DataFrame containing data for various education levels and total literate education.

    Returns:
        pandas.DataFrame: The DataFrame with the total literate education calculated.
    """

    df['Below_Primary_Education'] = df['Below_Primary_Education'].apply(int)
    df['Primary_Education'] = df['Primary_Education'].apply(int)
    df['Middle_Education'] = df['Middle_Education'].apply(int)
    df['Secondary_Education'] = df['Secondary_Education'].apply(int)
    df['Higher_Education'] = df['Higher_Education'].apply(int)
    df['Graduate_Education'] = df['Graduate_Education'].apply(int)
    df['Other_Education'] = df['Other_Education'].apply(int)
    df['Literate_Education'] = df['Literate_Education'].apply(int)


    for index,rows in df.iterrows():           
            df.at[index,'Literate_Education'] = rows['Below_Primary_Education'] + rows['Primary_Education'] + rows['Middle_Education'] + rows['Secondary_Education'] + rows['Higher_Education'] + rows['Graduate_Education'] + rows['Other_Education'] 

    return df


def filling_data_totaleducation(df):
    """
    Fills in missing literate or illiterate education data points and calculates the total
    education based on the existing values.

    Args:
        df (pandas.DataFrame): The DataFrame containing literate, illiterate, and total education data.

    Returns:
        pandas.DataFrame: The DataFrame with missing values filled and total education recalculated.
    """
    
    df['Literate_Education'] = df['Literate_Education'].apply(int)
    df['Illiterate_Education'] = df['Illiterate_Education'].apply(int)
    df['Total_Education'] = df['Total_Education'].apply(int)

    for index,rows in df.iterrows():
        if rows['Literate_Education'] and rows['Total_Education'] != 0:
            df.at[index,'Illiterate_Education'] = rows['Total_Education'] - rows['Literate_Education']
        if rows['Illiterate_Education'] and rows['Total_Education'] != 0:
            df.at[index,'Literate_Education'] = rows['Total_Education'] - rows['Illiterate_Education']

    df['Total_Education'] = df[['Literate_Education', 'Illiterate_Education']].sum(axis=1)
    return df


def filling_data_sc(df):
    """
    Fills in missing male or female SC data points and calculates the total SC population
    based on the existing values.

    Args:
        df (pandas.DataFrame): The DataFrame containing male, female, and total SC data.

    Returns:
        pandas.DataFrame: The DataFrame with missing values filled and total SC population recalculated.
    """
    
    df['Male_SC'] = df['Male_SC'].apply(int)
    df['Female_SC'] = df['Female_SC'].apply(int)
    df['SC'] = df['SC'].apply(int)

    for index,rows in df.iterrows():
        if rows['Male_SC'] and rows['SC'] != 0:
            df.at[index,'Female_SC'] = rows['SC'] - rows['Male_SC']
        if rows['Female_SC'] and rows['SC'] != 0:
            df.at[index,'Male_SC'] = rows['SC'] - rows['Female_SC']

    df['SC'] = df[['Male_SC', 'Female_SC']].sum(axis=1)
    return df

def filling_data_st(df):
    """
    Fills in missing male or female ST data points and calculates the total ST population
    based on the existing values.

    Args:
        df (pandas.DataFrame): The DataFrame containing male, female, and total ST data.

    Returns:
        pandas.DataFrame: The DataFrame with missing values filled and total ST population recalculated.
    """

    df['Male_ST'] = df['Male_ST'].apply(int)
    df['Female_ST'] = df['Female_ST'].apply(int)
    df['ST'] = df['ST'].apply(int)

    for index,rows in df.iterrows():
        if rows['Male_ST'] and rows['ST'] != 0:
            df.at[index,'Female_ST'] = rows['ST'] - rows['Male_ST']
        if rows['Female_ST'] and rows['ST'] != 0:
            df.at[index,'Male_ST'] = rows['ST'] - rows['Female_ST']

    df['ST'] = df[['Male_ST', 'Female_ST']].sum(axis=1)
    return df

def filling_data_workers(df):    
    """
    Fills in missing male or female worker data points and calculates the total worker population
    based on the existing values.

    Args:
        df (pandas.DataFrame): The DataFrame containing male, female, and total worker data.

    Returns:
        pandas.DataFrame: The DataFrame with missing values filled and total worker population recalculated.
    """
    
    df['Male_Workers'] = df['Male_Workers'].apply(int)
    df['Female_Workers'] = df['Female_Workers'].apply(int)
    df['Workers'] = df['Workers'].apply(int)

    for index,rows in df.iterrows():
        if rows['Male_Workers'] and rows['Workers'] != 0:
            df.at[index,'Female_Workers'] = rows['Workers'] - rows['Male_Workers']
        if rows['Female_Workers'] and rows['Workers'] != 0:
            df.at[index,'Male_Workers'] = rows['Workers'] - rows['Female_Workers']

    df['Workers'] = df[['Male_Workers', 'Female_Workers']].sum(axis=1)
    return df


if __name__ == "__main__": 

    ##### Task 1 Renaming column names
    renaming_columnname(df)
    ##### Task 2 Rename State/UT Names
    renaming_state_name(df)
    ##### Task 3  New State/UT formation

    new_state_formation(df,telangana_doc)
    ######## Task 4 Find and process Missing Data and filling data
    missing_df = missing_percentage(df)
    df = df.fillna(0)
    filled_population = filling_data_population(df)
    literate_data = filling_data_literate(df)

    household_data = filling_data_households(df)
    literate_education_data =filling_literate_education(df)
    totaleducation_data = filling_data_totaleducation(df)
    sc_data= filling_data_sc(df)
    st_data = filling_data_st(df)
    workers_data = filling_data_workers(df)

    missing_df_after = missing_percentage(df)

    appended_df = missing_df.append(missing_df_after, ignore_index=True) 
    for  i in df.columns:
        if df[i].dtype == 'float64':
            df[i] = df[i].apply(int)


    uri = config['database']['mongodb']['url']

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    db_name = config['database']['mongodb']['database']
    db = client[db_name]  
    collection_name = config['database']['mongodb']['collection']
    collection = db[collection_name]  


    records = df.to_dict(orient='records')


    collection.insert_many(records)
    print('success')







