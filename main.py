import streamlit as st
import pandas as pd
import snowflake.connector as sf

st.title("Insight Employee Data App")
st.write('This app helps you take a look at employee data with selecting column at your ease.')

sidebar = st.sidebar

# Function to connect to Snowflake
def connect_to_snowflake(acct, u, pd, rl, wh, db):
    ctx = sf.connect(user=u, account=acct, password=pd, role=rl, warehouse=wh, database=db)
    cs = ctx.cursor()
    st.session_state['snow_conn'] = cs
    st.session_state['is_ready'] = True
    return cs

# Function to fetch data from Snowflake
def get_data(query):
    results = st.session_state['snow_conn'].execute(query)
    results = st.session_state['snow_conn'].fetchall()
    return results

# Initialize an empty DataFrame to store the selected employee details
selected_data_df = pd.DataFrame()

with sidebar:
    account = st.text_input('Account')
    username = st.text_input('Username')
    password = st.text_input('Password', type='password')
    rl = st.text_input('Role')
    wh = st.text_input('Warehouse')
    db = st.text_input('Database')
    
    connect = st.button('Connect to Snowflake', on_click=connect_to_snowflake, args=[account, username, password, rl, wh, db])

if 'is_ready' not in st.session_state:
    st.session_state['is_ready'] = False

if st.session_state['is_ready'] == True:
    # Fetch column names from Snowflake table
    column_data = get_data('select * from raw_data.raw_schema.employee LIMIT 1;')
    
    # Extract the column names from the result
    column_names = [desc[0] for desc in st.session_state['snow_conn'].description]

    # Display the multiselect widget for column names
    selected_columns = st.multiselect("Pick columns to show:", column_names)
    
    # Add a checkbox to enable/disable filtering columns with only null data
    filter_null_columns = st.checkbox("Show Columns with Null Data")
    
    # Fetch all rows of selected columns from Snowflake table
    if selected_columns:
        query = f"select {', '.join(selected_columns)} from raw_data.raw_schema.employee;"
        selected_data = get_data(query)
        selected_data_df = pd.DataFrame(selected_data, columns=selected_columns)
        
        # Filter out columns with only null data if the checkbox is enabled
        if filter_null_columns:
            non_null_columns = selected_data_df.columns[selected_data_df.notnull().any()]
            selected_data_df = selected_data_df[non_null_columns]

# Display the accumulated data table
st.write("Accumulated Data:")
if not selected_data_df.empty:
    st.write(selected_data_df)
else:
    st.write("No data selected.")
