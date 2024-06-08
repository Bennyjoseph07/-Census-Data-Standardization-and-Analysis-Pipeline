import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import pandas as pd

user = 'root'
password = 'Welcome12345'
hostname = 'localhost'
port = '3306'
database = 'learning'
table = 'census_data'

engine_mysql = create_engine('mysql+mysqlconnector://' + user + ':' + password + '@' + hostname + ':' + port + '/' + database,poolclass=NullPool)

col1, col2 = st.columns([6,3])
# Title of the app
with col1:
    st.title("Census Data Standardization and Analysis Pipeline")

with col2:
    st.image(r'E:\guvi\capstone_project\census_datapipeline\census_datapipeline\Guvi.jpg')

# Input fields for two numbers
question = st.text_input('Enter the question?',value='Enter the text')
submit_button = st.button(label='Submit')
if submit_button:

    q_dic = {'What is the total population of each district?':'SELECT District,sum(Population) as total_population FROM learning.census_data group by District',
             'How many literate males and females are there in each district?':'select District,sum(Literate_Male) as literate_male , sum(Literate_Female) as literate_female FROM learning.census_data group by District',
             'How many households have access to LPG or PNG as a cooking fuel in each district?':'select District,sum(LPG_or_PNG_Households) as lpg_or_png_cooking_fuel from learning.census_data group by District',
             'What is the religious composition (Hindus, Muslims, Christians, etc.) of each district?':'select District, sum(Hindus) as hindus,sum(Muslims) as muslim, sum(Sikhs) as sikhs, sum(Buddhists) as buddha, sum(Jains) as jains, sum(Others_Religions) as other_religion from learning.census_data group by District',
             'How many households have internet access in each district?':'select District, sum(Households_with_Internet) as households_with_internet from learning.census_data group by District',
             'What is the educational attainment distribution (below primary, primary, middle, secondary, etc.) in each district?':'select District, sum(Below_Primary_Education) as below_pri_edu,sum(Primary_Education) as pri_edu,sum(Middle_Education) as middle_edu,sum(Secondary_Education) as sec_edu,sum(Higher_Education) as higher_edu,sum(Graduate_Education) as graduate_edu,sum(Other_Education) as other_edu from learning.census_data group by District',
             'How many households have access to various modes of transportation (bicycle, car, radio, television, etc.) in each district?':'select District, sum(HouseholdsBicycle) as householdsbicycle,sum(HouseholdsCarJeepVan) as householdscarjeepvan,sum(HouseholdsScooterMotorcycleMoped) as householdsscootermotorcyclemoped,sum(HouseholdsTechVehicles) as householdstechvehicles from learning.census_data group by District',
             'What is the condition of occupied census houses (dilapidated, with separate kitchen, with bathing facility, with latrine facility, etc.) in each district?':'select District, sum(DilapidatedHouseholds) as dilapidatedhouseholds,sum(HouseholdsSeparateKitchen) as householdsseparatekitchen,sum(HouseholdsBathingFacility) as householdsbathingfacility,sum(HouseholdsLatrineFacility) as householdslatrinefacility,sum(OwnedHouseholds) as ownedhouseholds,sum(RentedHouseholds) as rentedhouseholds from learning.census_data group by District',
             'How is the household size distributed (1 person, 2 persons, 3-5 persons, etc.) in each district?':'select District, sum(Household1Person) as household1person,sum(Household2Persons) as household2persons,sum(Household1to2Persons) as household1to2persons,sum(Household3Persons) as household3persons,sum(Household3to5Persons) as household3to5persons,sum(Household4Persons) as household4persons,sum(Household5Persons) as household5persons,sum(Household6to8Persons) as household6to8persons,sum(Household9PlusPersons) as household9pluspersons,sum(Household1Couple) as household1couple,sum(Household2Couples) as household2couples,sum(Household3Couples) as household3couples,sum(Household3PlusCouples) as household3pluscouples,sum(Household4Couples) as household4couples,sum(Household5Couples) as household5couples,sum(HouseholdNoCouples) as householdnocouples from learning.census_data group by District',
             'What is the total number of households in each state?':'select `State/UT`, sum(Households) as households from  learning.census_data group by `State/UT`',
             'How many households have a latrine facility within the premises in each state?':'select `State/UT`, sum(HouseholdsLatrineFacility) as latrine_facility_within_the_premises  from  learning.census_data group by `State/UT`',
             'What is the average household size in each state?':'select `State/UT`, avg(Households) as avg_households from  learning.census_data group by `State/UT`',
             'How many households are owned versus rented in each state?':'select `State/UT`, sum(OwnedHouseholds) as owned_house , sum(RentedHouseholds) as rent_house from  learning.census_data group by `State/UT`',
             'What is the distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) in each state?':'select `State/UT`, sum(HouseholdsPitLatrine) as householdspitlatrine,sum(HouseholdsOtherLatrine) as householdsotherlatrine,sum(HouseholdsNightSoilDrain) as householdsnightsoildrain,sum(HouseholdsFlushLatrineSystem) as householdsflushlatrinesystem from  learning.census_data group by `State/UT`',
             'What is the percentage of married couples with different household sizes in each state?':'select `State/UT`, (sum(Household1Couple + Household2Couples + Household3Couples + Household4Couples + Household5Couples + HouseholdNoCouples)/count(*) * 100) as married_couple_percentage  from learning.census_data group by `State/UT`'}

    query = q_dic[question]

    data = pd.read_sql(query,engine_mysql)

    st.header('Resultant output', divider='rainbow')
    st.dataframe(data)
