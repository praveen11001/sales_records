import streamlit as st
import pandas as pd
import snowflake.connector as sf

st.title("Connecting Snowflake Data to Streamlit Web App")
st.header('The Product Data')
st.write('The discount price was given in the "discount_price" column, and they are marked with color background to provide easy data visualization insight.')

sidebar = st.sidebar

# Function to connect to Snowflake
def connect_to_snowflake(acct, u, pd, rl, wh, db):
    ctx = sf.connect(user=u, account=acct, password=pd, role=rl, warehouse=wh, database=db)
    cs = ctx.cursor()
    st.session_state['snow_conn'] = cs
    st.session_state['is_ready'] = True
    return cs

# Function to apply highlighting
def highlight_discount_price(value):
    if value == 0:
        return 'background-color: #8FBC8F'  # Green
    else:
        return 'background-color: #FF6347'  # Red

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
    data = get_data()

    # Convert fetched data to a Pandas DataFrame
    df = pd.DataFrame(data, columns=['ID', 'NAME', 'CATEGORY', 'QUANTITY', 'Amount', 'Discount_price', 'Purchased_at'])

    # Apply highlighting to the 'Discount_price' column based on the value
    df_styled = df.style.applymap(highlight_discount_price, subset=['Discount_price'])

    # Display the styled DataFrame using st.write
    st.write(df_styled)
