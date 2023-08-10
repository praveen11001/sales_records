import streamlit as st
import pandas as pd
import snowflake.connector as sf

st.title("Snowflake data to Streamlit app")
st.header('This is used to check Snowflake to Streamlit data flow')

sidebar = st.sidebar

# Function to connect to Snowflake
def connect_to_snowflake(acct, u, pd, rl, wh, db):
    ctx = sf.connect(user=u, account=acct, password=pd, role=rl, warehouse=wh, database=db)
    cs = ctx.cursor()
    st.session_state['snow_conn'] = cs
    st.session_state['is_ready'] = True
    return cs

# Function to apply highlighting
def highlight_cells(value):
    if value < 5:
        return 'background-color: red'
    elif 5 <= value < 10:
        return 'background-color: yellow'
    else:
        return 'background-color: green'

# Function to fetch data from Snowflake
def get_data():
    query = 'select * from raw_data.refined_products.products;'
    results = st.session_state['snow_conn'].execute(query)
    results = st.session_state['snow_conn'].fetchall()
    return results

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
    st.write('Connected')
    data = get_data()

    # Convert fetched data to a Pandas DataFrame
    df = pd.DataFrame(data, columns=['ID', 'NAME', 'CATEGORY', 'QUANTITY', 'Amount', 'Discount_price', 'Purchased_at'])

    # Apply highlighting to the 'Amount' and 'Discount_price' columns based on the specified conditions
    df_styled = df.style.applymap(highlight_cells, subset=['Amount', 'Discount_price'])

    # Display the styled DataFrame using st.write
    st.write(df_styled)
