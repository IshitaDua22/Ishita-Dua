import psycopg2
import streamlit as st

# Function to establish a database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=st.secrets["db_credentials"]["Ish"],
            user=st.secrets["db_credentials"]["postgres"],
            password=st.secrets["db_credentials"]["Iamstrong22082000!"],
            host=st.secrets["db_credentials"]["localhost"],
            port=st.secrets["db_credentials"]["5432"]
        )
        return conn
    except psycopg2.OperationalError as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# Function to fetch all transactions based on filters and sorting
def fetch_transactions(transaction_type=None, sort_by=None, sort_order='ASC'):
    conn = get_db_connection()
    if conn is None:
        return []
    
    cursor = conn.cursor()
    
    query = "SELECT transaction_id, transaction_date, description, amount, type FROM transactions"
    
    params = []
    if transaction_type and transaction_type != 'All':
        query += " WHERE type = %s"
        params.append(transaction_type)
        
    if sort_by:
        if sort_by == 'amount':
            query += f" ORDER BY amount {sort_order}"
        elif sort_by == 'transaction_date':
            query += f" ORDER BY transaction_date {sort_order}"
    
    try:
        cursor.execute(query, params)
        transactions = cursor.fetchall()
        cursor.close()
        conn.close()
        return transactions
    except psycopg2.Error as e:
        st.error(f"Error fetching transactions: {e}")
        return []

# Function to fetch aggregated data
def fetch_aggregates():
    conn = get_db_connection()
    if conn is None:
        return {}
        
    cursor = conn.cursor()
    
    try:
        # Total number of transactions
        cursor.execute("SELECT COUNT(*) FROM transactions")
        total_transactions = cursor.fetchone()[0]
        
        # Total Revenue
        cursor.execute("SELECT SUM(amount) FROM transactions WHERE type = 'Revenue'")
        total_revenue = cursor.fetchone()[0]
        
        # Total Expense
        cursor.execute("SELECT SUM(amount) FROM transactions WHERE type = 'Expense'")
        total_expense = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "total_transactions": total_transactions,
            "total_revenue": total_revenue if total_revenue is not None else 0,
            "total_expense": total_expense if total_expense is not None else 0
        }
    except psycopg2.Error as e:
        st.error(f"Error fetching aggregates: {e}")
        return {}# Ishita-Dua
